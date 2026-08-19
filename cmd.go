package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/spf13/cobra"
)

const (
	// peerWaitTimeout bounds how long a one-shot command waits for its first
	// peer before giving up and acting on whatever it is connected to.
	peerWaitTimeout = 10 * time.Second

	// discoveryQuietPeriod is how long the peer set must hold steady before
	// discovery is treated as settled, and discoverySettleTimeout caps the
	// wait on a network that keeps producing new peers.
	discoveryQuietPeriod   = 300 * time.Millisecond
	discoverySettleTimeout = 5 * time.Second
)

// withBootstrap adds addr to the bootstrap list if it is not already there.
func withBootstrap(bootstrap []string, addr string) []string {
	for _, b := range bootstrap {
		if b == addr {
			return bootstrap
		}
	}
	return append(bootstrap, addr)
}

// nodeTarget describes where a command should send its work: the node already
// running against this database, or a temporary one it starts itself.
type nodeTarget struct {
	// running is set when a node owns this database and can be asked to act.
	running *controlClient

	// local is set instead, and is a node started for this command alone.
	local *FileServer
}

// onNode runs work against whichever node should carry it out.
//
// A running node owns the database and its storage, so it does the work; a
// command that started a second node against the same files is what produced
// races, miscounted replica counts and files owned by a key that vanished.
// Starting a temporary node is safe only when there is no running one, which is
// exactly when this takes that path.
func onNode(dbPath, listen string, bootstrap []string, replicas int, work func(nodeTarget) error) error {
	if node, ok := dialControl(dbPath); ok {
		return work(nodeTarget{running: node})
	}

	d, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer d.Close()

	s, stop, err := startClientNode(listen, d, bootstrap, replicas)
	if err != nil {
		return err
	}
	defer stop()

	return work(nodeTarget{local: s})
}

// openDB opens and migrates the node database.
func openDB(path string) (*dbpkg.DB, error) {
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
func startClientNode(listen string, d *dbpkg.DB, bootstrap []string, replicas int) (*FileServer, func(), error) {
	keyBytes, err := loadOrInitKey(d)
	if err != nil {
		return nil, nil, err
	}

	// A command run against a serving node's database can reach the network
	// through that node without being told where it is.
	if owner, ok, err := d.GetSetting(context.Background(), dbpkg.ServingAddressSetting); err == nil && ok && owner != "" {
		bootstrap = withBootstrap(bootstrap, owner)
	}

	s, err := makeClientNode(listen, d, bootstrap...)
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
		if err := s.waitForPeers(peerWaitTimeout); err != nil {
			fmt.Printf("Warning: %v. Proceeding anyway.\n", err)
		}
		// Gossip keeps introducing peers after the first connection lands.
		// Wait for the set to settle rather than for a fixed period.
		n := s.waitForPeerDiscovery(discoveryQuietPeriod, discoverySettleTimeout)
		fmt.Printf("Connected to %d peer(s).\n", n)
	}

	return s, s.Stop, nil
}

func setupCommands() *cobra.Command {
	var (
		dbPath     string
		listen     string
		bootstrap  []string
		configPath string
		replicas   int
	)

	root := &cobra.Command{
		Use:   "p2p",
		Short: "Decentralized P2P storage node",

		// A command that fails at run time has already parsed its flags
		// correctly, so printing the usage text buries the actual error.
		SilenceUsage: true,
	}
	root.PersistentFlags().StringVar(&dbPath, "db", "p2p.db", "sqlite database path")

	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Start a P2P storage node",
		RunE: func(cmd *cobra.Command, args []string) error {
			if configPath != "" {
				cfg, err := LoadConfig(configPath)
				if err != nil {
					return fmt.Errorf("error loading config: %v", err)
				}

				if !cmd.Flags().Changed("listen") && cfg.Listen != "" {
					listen = cfg.Listen
				}
				if !cmd.Flags().Changed("db") && cfg.DB != "" {
					dbPath = cfg.DB
				}
				if !cmd.Flags().Changed("bootstrap") && len(cfg.Bootstrap) > 0 {
					bootstrap = cfg.Bootstrap
				}
				if !cmd.Flags().Changed("replicas") && cfg.Replicas > 0 {
					replicas = cfg.Replicas
				}
			}

			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			keyBytes, err := loadOrInitKey(d)
			if err != nil {
				return err
			}
			s, err := makeServerWithDB(listen, d, bootstrap...)
			if err != nil {
				return err
			}
			s.EncryptionKey = keyBytes
			s.ReplicationFactor = replicas
			s.OwnsDatabase = true

			if err := s.Listen(); err != nil {
				return err
			}

			// Commands ask this node to act rather than starting one of their
			// own against the same storage.
			if err := s.ListenControl(ControlSocketPath(dbPath)); err != nil {
				return err
			}

			// Shut down cleanly on Ctrl-C so the database is closed and
			// in-flight writes are not cut off mid-file.
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(sigs)

			go func() {
				sig := <-sigs
				fmt.Printf("\nReceived %s, shutting down...\n", sig)
				s.Stop()
			}()

			s.Serve()
			return nil
		},
	}
	serveCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address")
	serveCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes")
	serveCmd.Flags().StringVar(&configPath, "config", "", "config file path (e.g., ~/.p2p/config)")
	serveCmd.Flags().IntVar(&replicas, "replicas", DefaultReplicationFactor, "how many copies of each file the network should hold")
	root.AddCommand(serveCmd)

	storeCmd := &cobra.Command{
		Use:   "store <key> <file>",
		Short: "Store a file locally and broadcast to peers",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, path := args[0], args[1]
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			return onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				if t.running != nil {
					info, err := f.Stat()
					if err != nil {
						return err
					}
					return t.running.store(key, info.Size(), f)
				}
				return t.local.Store(key, f)
			})
		},
	}
	storeCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	storeCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	root.AddCommand(storeCmd)

	getCmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Fetch a file (local or from peers)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			out, _ := cmd.Flags().GetString("out")

			writeOut := func(fn func(io.Writer) error) error {
				if out == "" {
					return fn(os.Stdout)
				}
				of, err := os.Create(out)
				if err != nil {
					return err
				}
				defer of.Close()
				return fn(of)
			}

			return onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				if t.running != nil {
					return writeOut(func(w io.Writer) error { return t.running.get(key, w) })
				}

				_, r, err := t.local.Get(key)
				if err != nil {
					return err
				}
				if rc, ok := r.(io.Closer); ok {
					defer rc.Close()
				}
				return writeOut(func(w io.Writer) error {
					_, err := io.Copy(w, r)
					return err
				})
			})
		},
	}
	getCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	getCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	getCmd.Flags().String("out", "", "output file path")
	root.AddCommand(getCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a file locally and from all peers",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]

			return onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				if t.running != nil {
					return t.running.delete(key)
				}
				return t.local.Delete(key)
			})
		},
	}
	deleteCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	deleteCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	root.AddCommand(deleteCmd)

	filesCmd := &cobra.Command{Use: "files", Short: "File operations"}
	filesListCmd := &cobra.Command{
		Use:   "list",
		Short: "List known files",
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			ff, err := d.ListFiles(context.Background())
			if err != nil {
				return err
			}
			if len(ff) == 0 {
				fmt.Println("No files found.")
				return nil
			}
			fmt.Printf("%-20s\t%-10s\t%s\n", "FILE", "SIZE", "CREATED")
			fmt.Println(strings.Repeat("-", 60))
			for _, f := range ff {
				fmt.Printf("%-20s\t%-10d\t%s\n",
					f.Name,
					f.Size,
					f.CreatedAt.Format("2006-01-02 15:04:05"))
			}
			return nil
		},
	}
	filesCmd.AddCommand(filesListCmd)
	root.AddCommand(filesCmd)

	sharesCmd := &cobra.Command{
		Use:   "shares",
		Short: "List file shares (files stored in other peers)",
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			shares, err := d.ListShares(context.Background())
			if err != nil {
				return err
			}
			if len(shares) == 0 {
				fmt.Println("No shares found.")
				return nil
			}
			fmt.Printf("%-20s\t%-20s\t%-15s\t%-10s\t%s\n", "FILE", "PEER", "DIRECTION", "SIZE", "CREATED")
			fmt.Println(strings.Repeat("-", 100))
			for _, s := range shares {
				fmt.Printf("%-20s\t%-20s\t%-15s\t%-10d\t%s\n",
					s.FileName,
					s.PeerID,
					s.Direction,
					s.FileSize,
					s.CreatedAt.Format("2006-01-02 15:04:05"))
			}
			return nil
		},
	}
	root.AddCommand(sharesCmd)

	peersCmd := &cobra.Command{
		Use:   "peers",
		Short: "List connected and known peers",
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			peers, err := d.GetActivePeers(context.Background(), 24*time.Hour, 100)
			if err != nil {
				return err
			}

			if len(peers) == 0 {
				fmt.Println("No peers found.")
				return nil
			}

			fmt.Printf("%-30s\t%-15s\t%s\n", "ADDRESS", "STATUS", "LAST SEEN")
			fmt.Println(strings.Repeat("-", 70))
			for _, p := range peers {
				lastSeen := "never"
				if p.LastSeen != nil {
					lastSeen = p.LastSeen.Format("2006-01-02 15:04:05")
				}
				fmt.Printf("%-30s\t%-15s\t%s\n", p.Address, p.Status, lastSeen)
			}
			return nil
		},
	}
	root.AddCommand(peersCmd)

	cleanupCmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Remove stale peer records from database",
		RunE: func(cmd *cobra.Command, args []string) error {
			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			removed, err := d.CleanupStalePeers(context.Background(), 1*time.Hour)
			if err != nil {
				return err
			}

			fmt.Printf("Removed %d stale peer(s)\n", removed)
			return nil
		},
	}
	root.AddCommand(cleanupCmd)

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Report how well replicated the local files are",
		RunE: func(cmd *cobra.Command, args []string) error {
			var health []FileHealth
			err := onNode(dbPath, listen, bootstrap, replicas, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					health, err = t.running.status(replicas)
				} else {
					health, err = t.local.ReplicationStatus()
				}
				return err
			})
			if err != nil {
				return err
			}
			if len(health) == 0 {
				fmt.Println("No files stored locally.")
				return nil
			}

			fmt.Printf("%-24s\t%-12s\t%-10s\t%s\n", "FILE", "COPIES", "SIZE", "STATE")
			fmt.Println(strings.Repeat("-", 70))

			atRisk := 0
			for _, h := range health {
				state := "ok"
				if h.AtRisk() {
					state = "AT RISK"
					atRisk++
				}
				fmt.Printf("%-24s\t%d of %-8d\t%-10d\t%s\n", h.Name, h.Copies, h.Target, h.Size, state)
			}

			if atRisk > 0 {
				fmt.Printf("\n%d file(s) below the replication target of %d.\n", atRisk, replicas)
				fmt.Println("Run 'p2p repair' to place the missing copies, or start more nodes.")
			}
			return nil
		},
	}
	statusCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	statusCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	statusCmd.Flags().IntVar(&replicas, "replicas", DefaultReplicationFactor, "replication target to measure against")
	root.AddCommand(statusCmd)

	repairCmd := &cobra.Command{
		Use:   "repair",
		Short: "Place missing copies of under-replicated files",
		RunE: func(cmd *cobra.Command, args []string) error {
			var placed int
			err := onNode(dbPath, listen, bootstrap, replicas, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					placed, err = t.running.repair(replicas)
				} else {
					placed, err = t.local.RepairOnce()
				}
				return err
			})
			if err != nil {
				return err
			}
			if placed == 0 {
				fmt.Println("Nothing to repair.")
				return nil
			}
			// A peer refuses contents it has deleted, so an offer is not
			// always a copy kept. Run status afterwards to see the result.
			fmt.Printf("Offered %d missing replica(s). Run 'p2p status' to confirm.\n", placed)
			return nil
		},
	}
	repairCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	repairCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	repairCmd.Flags().IntVar(&replicas, "replicas", DefaultReplicationFactor, "replication target to restore")
	root.AddCommand(repairCmd)

	demoCmd := &cobra.Command{
		Use:   "demo",
		Short: "Run the local 3-node demo",
		RunE: func(cmd *cobra.Command, args []string) error {
			specs := []struct {
				listen    string
				bootstrap []string
			}{
				{":3000", nil},
				{":4000", []string{":3000"}},
				{":5000", []string{":3000", ":4000"}},
			}

			// Every node needs its own database: it is what maps a name to the
			// contents stored under it, so a node without one cannot answer
			// for anything by name.
			root, err := os.MkdirTemp("", "p2p-demo-*")
			if err != nil {
				return err
			}
			defer os.RemoveAll(root)
			fmt.Printf("demo data in %s\n", root)

			servers := make([]*FileServer, 0, len(specs))
			for i, spec := range specs {
				dir := filepath.Join(root, fmt.Sprintf("node%d", i+1))
				if err := os.MkdirAll(dir, 0o700); err != nil {
					return err
				}

				d, err := openDB(filepath.Join(dir, "p2p.db"))
				if err != nil {
					return err
				}
				defer d.Close()

				s, err := makeServerWithDB(spec.listen, d, spec.bootstrap...)
				if err != nil {
					return err
				}
				key, err := loadOrInitKey(d)
				if err != nil {
					return err
				}
				s.EncryptionKey = key
				if err := s.Listen(); err != nil {
					return err
				}
				go s.Serve()
				defer s.Stop()

				servers = append(servers, s)
			}

			origin, other := servers[0], servers[2]

			if err := other.waitForPeers(peerWaitTimeout); err != nil {
				return err
			}
			other.waitForPeerDiscovery(discoveryQuietPeriod, discoverySettleTimeout)

			// Store on one node.
			const key = "coolpicture.jpg"
			payload := []byte("my big data file here!")
			if err := origin.Store(key, bytes.NewReader(payload)); err != nil {
				return fmt.Errorf("storing: %w", err)
			}
			fmt.Printf("stored %q on %s\n", key, origin.Transport.Address())

			// Read it back from a different node, which is the point of the
			// exercise: the file is available from a peer that did not store it.
			_, r, err := other.Get(key)
			if err != nil {
				return fmt.Errorf("fetching from %s: %w", other.Transport.Address(), err)
			}
			got, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			fmt.Printf("fetched from %s: %s\n", other.Transport.Address(), got)

			if !bytes.Equal(got, payload) {
				return fmt.Errorf("fetched contents differ from what was stored")
			}

			// Then delete it, and show it is gone rather than fetching it again
			// straight afterwards, which could never have succeeded.
			if err := origin.Delete(key); err != nil {
				return fmt.Errorf("deleting: %w", err)
			}
			if _, _, err := origin.Get(key); err == nil {
				return fmt.Errorf("%q is still readable after deletion", key)
			}
			fmt.Printf("deleted %q\n", key)

			return nil
		},
	}
	root.AddCommand(demoCmd)

	return root
}
