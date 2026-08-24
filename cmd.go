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
	"github.com/TinySkillet/DecentralizedP2PStorage/node"
	"github.com/TinySkillet/DecentralizedP2PStorage/storage"
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
	running *node.Client

	// local is set instead, and is a node started for this command alone.
	local *node.FileServer
}

// onNode runs work against whichever node should carry it out.
//
// A running node owns the database and its storage, so it does the work; a
// command that started a second node against the same files is what produced
// races, miscounted replica counts and files owned by a key that vanished.
// Starting a temporary node is safe only when there is no running one, which is
// exactly when this takes that path.
func onNode(dbPath, listen string, bootstrap []string, replicas int, work func(nodeTarget) error) error {
	if node, ok := node.DialControl(dbPath); ok {
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

// describeEvent renders the fields an event actually carries, since which
// ones are meaningful depends on its kind.
func describeEvent(e node.Event) string {
	var parts []string
	if e.Name != "" {
		parts = append(parts, e.Name)
	}
	if e.Digest != "" {
		parts = append(parts, storage.Short(e.Digest))
	}
	if e.Node != "" {
		parts = append(parts, storage.Short(e.Node))
	}
	if e.Peer != "" {
		parts = append(parts, e.Peer)
	}
	if e.Size > 0 {
		parts = append(parts, fmt.Sprintf("%d bytes", e.Size))
	}
	if e.Count > 0 {
		parts = append(parts, fmt.Sprintf("x%d", e.Count))
	}
	if e.Err != "" {
		parts = append(parts, e.Err)
	}
	return strings.Join(parts, "  ")
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
func startClientNode(listen string, d *dbpkg.DB, bootstrap []string, replicas int) (*node.FileServer, func(), error) {
	keyBytes, err := node.LoadOrInitKey(d)
	if err != nil {
		return nil, nil, err
	}

	// A command run against a serving node's database can reach the network
	// through that node without being told where it is.
	if owner, ok, err := d.GetSetting(context.Background(), dbpkg.ServingAddressSetting); err == nil && ok && owner != "" {
		bootstrap = withBootstrap(bootstrap, owner)
	}

	s, err := node.NewClient(listen, d, bootstrap...)
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

func setupCommands() *cobra.Command {
	var (
		dbPath     string
		listen     string
		bootstrap  []string
		configPath string
		replicas   int

		httpAddr    string
		httpExposed bool
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
				if !cmd.Flags().Changed("http") && cfg.HTTP != "" {
					httpAddr = cfg.HTTP
				}
				if !cmd.Flags().Changed("http-exposed") && cfg.HTTPExposed {
					httpExposed = true
				}
			}

			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			keyBytes, err := node.LoadOrInitKey(d)
			if err != nil {
				return err
			}
			s, err := node.NewServer(listen, d, bootstrap...)
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
			if err := s.ListenControl(node.ControlSocketPath(dbPath)); err != nil {
				return err
			}

			// Off unless asked for: the UI can administer trust, so it is a
			// deliberate choice rather than something upgrading gives you.
			if httpAddr != "" {
				if err := s.ListenHTTP(httpAddr, httpExposed, filepath.Dir(dbPath)); err != nil {
					return err
				}
				defer s.StopHTTP()
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
	serveCmd.Flags().IntVar(&replicas, "replicas", node.DefaultReplicationFactor, "how many copies of each file the network should hold")
	serveCmd.Flags().StringVar(&httpAddr, "http", "", "serve the local web UI on this address (e.g. 127.0.0.1:7654); off by default")
	serveCmd.Flags().BoolVar(&httpExposed, "http-exposed", false, "allow binding the web UI off loopback, protected by a token file")
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
					return t.running.Store(key, info.Size(), f)
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
					return writeOut(func(w io.Writer) error { return t.running.Get(key, w) })
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
					return t.running.Delete(key)
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
			// Routed through the running node rather than opening the
			// database directly: the node owns it, and its cached
			// replication measurements come along for free.
			var files []node.ReplicaSnapshot
			err := onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					files, err = t.running.Files()
				} else {
					files, err = t.local.ReplicationSnapshot(context.Background())
				}
				return err
			})
			if err != nil {
				return err
			}
			if len(files) == 0 {
				fmt.Println("No files found.")
				return nil
			}
			fmt.Printf("%-24s\t%-10s\t%-12s\t%s\n", "FILE", "SIZE", "COPIES", "CHECKED")
			fmt.Println(strings.Repeat("-", 72))
			for _, f := range files {
				copies := "not checked"
				checked := "never"
				if f.Measured() {
					copies = fmt.Sprintf("%d of %d", f.Copies, f.Target)
					checked = f.Age().Round(time.Second).String() + " ago"
				}
				fmt.Printf("%-24s\t%-10d\t%-12s\t%s\n", f.Name, f.Size, copies, checked)
			}
			return nil
		},
	}
	filesListCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	filesListCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	filesCmd.AddCommand(filesListCmd)
	root.AddCommand(filesCmd)

	sharesCmd := &cobra.Command{
		Use:   "shares",
		Short: "List file shares (files stored in other peers)",
		RunE: func(cmd *cobra.Command, args []string) error {
			var shares []node.ShareView
			err := onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					shares, err = t.running.Shares()
				} else {
					shares, err = t.local.ShareViews(context.Background())
				}
				return err
			})
			if err != nil {
				return err
			}
			if len(shares) == 0 {
				fmt.Println("No shares found.")
				return nil
			}
			// Peers are recorded by identity, which is a full public key. It
			// is abbreviated here so the table stays readable; the identity
			// itself is what the record holds.
			fmt.Printf("%-20s\t%-14s\t%-10s\t%-10s\t%s\n", "FILE", "PEER", "DIRECTION", "SIZE", "CREATED")
			fmt.Println(strings.Repeat("-", 85))
			for _, sh := range shares {
				fmt.Printf("%-20s\t%-14s\t%-10s\t%-10d\t%s\n",
					sh.Name, sh.Short(), sh.Direction, sh.Size,
					sh.CreatedAt.Format("2006-01-02 15:04:05"))
			}
			return nil
		},
	}
	sharesCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	sharesCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	root.AddCommand(sharesCmd)

	peersCmd := &cobra.Command{
		Use:   "peers",
		Short: "List connected and known peers",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Asked of the running node, not the database. The peers table
			// records "connected" on connect and never corrects it if the
			// node dies, and GetActivePeers hides the 4th and later identity
			// on a host. Both are wrong for a list a person reads.
			var peers []node.PeerView
			err := onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					peers, err = t.running.Peers()
				} else {
					peers, err = t.local.PeerViews(context.Background())
				}
				return err
			})
			if err != nil {
				return err
			}
			if len(peers) == 0 {
				fmt.Println("No peers found.")
				return nil
			}

			fmt.Printf("%-14s\t%-24s\t%-9s\t%s\n", "PEER", "ADDRESS", "STATE", "LAST SEEN")
			fmt.Println(strings.Repeat("-", 74))
			for _, p := range peers {
				state := "offline"
				if p.Online {
					state = "online"
				}
				lastSeen := "never"
				if p.LastSeen != nil {
					lastSeen = p.LastSeen.Format("2006-01-02 15:04:05")
				}
				fmt.Printf("%-14s\t%-24s\t%-9s\t%s\n", p.Short(), p.Address, state, lastSeen)
			}
			return nil
		},
	}
	peersCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	peersCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	root.AddCommand(peersCmd)

	watchCmd := &cobra.Command{
		Use:   "watch",
		Short: "Follow what the running node is doing",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Only a running node has anything to report, so this does not
			// fall back to starting one: a temporary node would emit its own
			// startup and then exit, which tells the user nothing.
			client, ok := node.DialControl(dbPath)
			if !ok {
				return fmt.Errorf("no node is running against %s; start one with 'p2p serve'", dbPath)
			}

			events, stop, err := client.Watch()
			if err != nil {
				return err
			}
			defer stop()

			// Ctrl-C ends the watch rather than killing the command mid-line.
			interrupted := make(chan os.Signal, 1)
			signal.Notify(interrupted, os.Interrupt)
			defer signal.Stop(interrupted)

			fmt.Println("Watching. Press Ctrl-C to stop.")
			for {
				select {
				case e, ok := <-events:
					if !ok {
						fmt.Println("The node stopped.")
						return nil
					}
					fmt.Printf("%s  %-14s %s\n",
						e.At.Format("15:04:05"), e.Kind, describeEvent(e))
				case <-interrupted:
					fmt.Println()
					return nil
				}
			}
		},
	}
	root.AddCommand(watchCmd)

	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Report this node's identity and state",
		RunE: func(cmd *cobra.Command, args []string) error {
			var view node.NodeView
			err := onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					view, err = t.running.Node()
				} else {
					view, err = t.local.NodeView(context.Background())
				}
				return err
			})
			if err != nil {
				return err
			}

			// The full identity, not the abbreviation every table shows: this
			// is the value another operator has to type to approve this node,
			// and there is nowhere else to read it from.
			fmt.Printf("Identity:    %s\n", view.NodeID)
			if view.OwnerID != view.NodeID {
				fmt.Printf("Owner:       %s\n", view.OwnerID)
			}
			fmt.Printf("Listening:   %s\n", view.Address)
			fmt.Printf("Peers:       %d connected\n", view.Peers)
			fmt.Printf("Files:       %d (%d bytes)\n", view.Files, view.Bytes)
			fmt.Printf("Replicas:    %d wanted per file\n", view.ReplicationFactor)
			return nil
		},
	}
	nodeCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
	nodeCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	root.AddCommand(nodeCmd)

	trustCmd := &cobra.Command{Use: "trust", Short: "Approve peers, and see who is approved"}

	trustAddCmd := &cobra.Command{
		Use:   "add <node-id> [label]",
		Short: "Approve a peer to push files and request deletions",
		Long: "Approve a peer to push files and request deletions here.\n\n" +
			"The identity may be abbreviated to any prefix that matches exactly one\n" +
			"peer this node knows about. A peer that has never connected has to be\n" +
			"named by its full 64-character identity, which 'p2p node' prints.",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			label := ""
			if len(args) > 1 {
				label = args[1]
			}
			return onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				if t.running != nil {
					return t.running.Trust(args[0], label)
				}
				return t.local.Trust(args[0], label)
			})
		},
	}

	trustRemoveCmd := &cobra.Command{
		Use:   "remove <node-id>",
		Short: "Withdraw approval from a peer",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var had bool
			err := onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					had, err = t.running.Untrust(args[0])
				} else {
					had, err = t.local.Untrust(args[0])
				}
				return err
			})
			if err != nil {
				return err
			}
			if !had {
				fmt.Printf("%s was not approved anyway.\n", storage.Short(args[0]))
			}
			return nil
		},
	}

	trustListCmd := &cobra.Command{
		Use:   "list",
		Short: "List approved peers",
		RunE: func(cmd *cobra.Command, args []string) error {
			var trusted []node.TrustedPeerView
			var mode string
			err := onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					trusted, mode, err = t.running.Trusted()
					return err
				}
				trusted, err = t.local.TrustedPeers(context.Background())
				mode = t.local.TrustMode()
				return err
			})
			if err != nil {
				return err
			}

			fmt.Printf("Trust mode: %s\n", mode)
			if mode != dbpkg.TrustModeEnforcing {
				fmt.Println("Approval is recorded but not enforced. Use 'p2p trust mode enforcing' to enforce it.")
			}
			fmt.Println()

			if len(trusted) == 0 {
				fmt.Println("No approved peers.")
				return nil
			}
			fmt.Printf("%-14s\t%-9s\t%-20s\t%s\n", "PEER", "STATE", "LABEL", "APPROVED")
			fmt.Println(strings.Repeat("-", 72))
			for _, p := range trusted {
				state := "offline"
				if p.Online {
					state = "online"
				}
				fmt.Printf("%-14s\t%-9s\t%-20s\t%s\n",
					storage.Short(p.NodeID), state, p.Label,
					p.TrustedAt.Format("2006-01-02 15:04:05"))
			}
			return nil
		},
	}

	trustModeCmd := &cobra.Command{
		Use:   "mode [open|enforcing]",
		Short: "Show or set whether trust is enforced",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wanted := ""
			if len(args) > 0 {
				wanted = args[0]
			}

			var mode string
			err := onNode(dbPath, listen, bootstrap, 0, func(t nodeTarget) error {
				if t.running != nil {
					var err error
					mode, err = t.running.Mode(wanted)
					return err
				}
				if wanted != "" {
					if err := t.local.SetTrustMode(wanted); err != nil {
						return err
					}
				}
				mode = t.local.TrustMode()
				return nil
			})
			if err != nil {
				return err
			}
			fmt.Printf("Trust mode: %s\n", mode)
			return nil
		},
	}

	for _, c := range []*cobra.Command{trustAddCmd, trustRemoveCmd, trustListCmd, trustModeCmd} {
		c.Flags().StringVar(&listen, "listen", ":3000", "listen address (only used when no node is running)")
		c.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
		trustCmd.AddCommand(c)
	}
	root.AddCommand(trustCmd)

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
			var health []node.FileHealth
			err := onNode(dbPath, listen, bootstrap, replicas, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					health, err = t.running.Status(replicas)
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
	statusCmd.Flags().IntVar(&replicas, "replicas", node.DefaultReplicationFactor, "replication target to measure against")
	root.AddCommand(statusCmd)

	repairCmd := &cobra.Command{
		Use:   "repair",
		Short: "Place missing copies of under-replicated files",
		RunE: func(cmd *cobra.Command, args []string) error {
			var placed int
			err := onNode(dbPath, listen, bootstrap, replicas, func(t nodeTarget) error {
				var err error
				if t.running != nil {
					placed, err = t.running.Repair(replicas)
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
	repairCmd.Flags().IntVar(&replicas, "replicas", node.DefaultReplicationFactor, "replication target to restore")
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

			servers := make([]*node.FileServer, 0, len(specs))
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

				s, err := node.NewServer(spec.listen, d, spec.bootstrap...)
				if err != nil {
					return err
				}
				key, err := node.LoadOrInitKey(d)
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

			if err := other.WaitForPeers(peerWaitTimeout); err != nil {
				return err
			}
			other.WaitForPeerDiscovery(discoveryQuietPeriod, discoverySettleTimeout)

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
