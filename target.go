package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/TinySkillet/DecentralizedP2PStorage/node"
)

// This file is how a command reaches a node: which one carries out the work,
// and how it is built when none is already running.

// nodeTarget is whichever node carries out a command: the one already running
// against this database, or a temporary one started for the command alone.
//
// Commands never branch on which they got. The two are asked the same
// questions through the methods below, because the distinction is an
// implementation detail of how the work is delivered, not of what was asked.
type nodeTarget struct {
	// running is set when a node owns this database and can be asked to act.
	running *node.Client

	// local is set instead, and is a node started for this command alone.
	local *node.FileServer
}

func (t nodeTarget) Node() (node.NodeView, error) {
	if t.running != nil {
		return t.running.Node()
	}
	return t.local.NodeView(context.Background())
}

func (t nodeTarget) Peers() ([]node.PeerView, error) {
	if t.running != nil {
		return t.running.Peers()
	}
	return t.local.PeerViews(context.Background())
}

func (t nodeTarget) Files() ([]node.ReplicaSnapshot, error) {
	if t.running != nil {
		return t.running.Files()
	}
	return t.local.ReplicationSnapshot(context.Background())
}

func (t nodeTarget) Shares() ([]node.ShareView, error) {
	if t.running != nil {
		return t.running.Shares()
	}
	return t.local.ShareViews(context.Background())
}

func (t nodeTarget) Trusted() ([]node.TrustedPeerView, string, error) {
	if t.running != nil {
		return t.running.Trusted()
	}
	trusted, err := t.local.TrustedPeers(context.Background())
	return trusted, t.local.TrustMode(), err
}

func (t nodeTarget) Trust(nodeID, label string) error {
	if t.running != nil {
		return t.running.Trust(nodeID, label)
	}
	return t.local.Trust(nodeID, label)
}

func (t nodeTarget) Untrust(nodeID string) (bool, error) {
	if t.running != nil {
		return t.running.Untrust(nodeID)
	}
	return t.local.Untrust(nodeID)
}

func (t nodeTarget) Mode(mode string) (string, error) {
	if t.running != nil {
		return t.running.Mode(mode)
	}
	if mode != "" {
		if err := t.local.SetTrustMode(mode); err != nil {
			return "", err
		}
	}
	return t.local.TrustMode(), nil
}

func (t nodeTarget) Status(replicas int) ([]node.FileHealth, error) {
	if t.running != nil {
		return t.running.Status(replicas)
	}
	return t.local.ReplicationStatus()
}

func (t nodeTarget) Repair(replicas int) (int, error) {
	if t.running != nil {
		return t.running.Repair(replicas)
	}
	return t.local.RepairOnce()
}

// Store sends a file. The two paths differ because the control socket needs
// the length up front to frame the payload, while a local node reads to EOF.
func (t nodeTarget) Store(name string, f *os.File) error {
	if t.running != nil {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		return t.running.Store(name, info.Size(), f)
	}
	return t.local.Store(name, f)
}

// Get writes the file to w.
func (t nodeTarget) Get(name string, w io.Writer) error {
	if t.running != nil {
		return t.running.Get(name, w)
	}

	_, r, err := t.local.Get(name)
	if err != nil {
		return err
	}
	if rc, ok := r.(io.Closer); ok {
		defer rc.Close()
	}
	_, err = io.Copy(w, r)
	return err
}

func (t nodeTarget) Delete(name string) error {
	if t.running != nil {
		return t.running.Delete(name)
	}
	return t.local.Delete(name)
}

// onNode runs work that may need the network.
//
// A running node owns the database and its storage, so it does the work; a
// command that started a second node against the same files is what produced
// races, miscounted replica counts and files owned by a key that vanished.
// Starting a temporary node is safe only when there is no running one, which is
// exactly when this takes that path.
func onNode(dbPath, transport, listen string, bootstrap []string, replicas int, work func(nodeTarget) error) error {
	if client, ok := node.DialControl(dbPath); ok {
		return work(nodeTarget{running: client})
	}

	d, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer d.Close()

	s, stop, err := startClientNode(transport, listen, d, bootstrap, replicas)
	if err != nil {
		return err
	}
	defer stop()

	return work(nodeTarget{local: s})
}

// onNodeReading runs work that only reads.
//
// Deliberately unlike onNode in two ways, both of which were making the tool
// report confident nonsense:
//
// It never creates a database. A read against a path where none exists used to
// make an empty one and answer from it, so a command run in the wrong
// directory reported "No peers found" rather than "there is nothing here" —
// the same answer a healthy but idle node gives.
//
// It never joins the network. Reading needs no peers: with no node running
// there is no live peer set, so everything is offline and that is the true
// answer. Connecting first meant a read waited out the bootstrap timeout,
// ten seconds and more, whenever the node it would have asked was down.
func onNodeReading(dbPath string, work func(nodeTarget) error) error {
	if client, ok := node.DialControl(dbPath); ok {
		return work(nodeTarget{running: client})
	}

	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no database at %s\n\n"+
				"Nothing has been stored here yet. Start a node with 'p2p serve',\n"+
				"or point at an existing database with --db.", dbPath)
		}
		return err
	}

	d, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer d.Close()

	// Constructed but never started: it answers from the database and its own
	// storage, and its live peer set is empty because it has no connections.
	s, err := node.NewClient(node.TransportTCP, "", d)
	if err != nil {
		return err
	}
	key, err := node.LoadOrInitKey(d)
	if err != nil {
		return err
	}
	s.EncryptionKey = key

	return work(nodeTarget{local: s})
}

// openDB opens and migrates the node database, creating its directory if
// needed: the default path lives under ~/.p2p, which does not exist until a
// node has run at least once.
func openDB(path string) (*dbpkg.DB, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("creating %s: %w", dir, err)
		}
	}

	d, err := dbpkg.Open(path)
	if err != nil {
		return nil, err
	}
	if err := d.Migrate(context.Background()); err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

// startClientNode brings up a short-lived node for a one-shot command and
// returns it together with a shutdown function.
//
// Listen is synchronous, so a bind failure (a port already in use, most
// often) is reported here rather than crashing a background goroutine.
func startClientNode(transport, listen string, d *dbpkg.DB, bootstrap []string, replicas int) (*node.FileServer, func(), error) {
	keyBytes, err := node.LoadOrInitKey(d)
	if err != nil {
		return nil, nil, err
	}

	// A command run against a serving node's database can reach the network
	// through that node without being told where it is.
	if owner, ok, err := d.GetSetting(context.Background(), dbpkg.ServingAddressSetting); err == nil && ok && owner != "" {
		bootstrap = withBootstrap(bootstrap, owner)
	}

	s, err := node.NewClient(transport, listen, d, bootstrap...)
	if err != nil {
		return nil, nil, err
	}
	s.EncryptionKey = keyBytes
	if replicas > 0 {
		// Applied before Serve: the goroutines it starts read this.
		s.ReplicationFactor = replicas
	}

	if err := s.Listen(); err != nil {
		return nil, nil, fmt.Errorf("starting node on %s: %w", listen, err)
	}
	go s.Serve()

	if len(bootstrap) > 0 {
		if err := s.WaitForPeers(peerWaitTimeout); err != nil {
			fmt.Printf("Warning: %v. Proceeding anyway.\n", err)
		}
		// Gossip keeps introducing peers after the first connection lands.
		// Wait for the set to settle rather than for a fixed period.
		n := s.WaitForPeerDiscovery(discoveryQuietPeriod, discoverySettleTimeout)
		fmt.Printf("Connected to %d peer(s).\n", n)
	}

	return s, s.Stop, nil
}
