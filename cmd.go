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

// defaultDBPath is where a node keeps its database unless told otherwise.
// Shown with the tilde because that is what a person recognises; expandHome
// resolves it before use.
const defaultDBPath = "~/.p2p/p2p.db"

// expandHome resolves a leading ~ against the user's home directory.
func expandHome(path string) (string, error) {
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving %q: %w", path, err)
	}
	if path == "~" {
		return home, nil
	}
	return filepath.Join(home, path[2:]), nil
}

// withNetworkFlags adds the flags a command needs only when it has to start a
// node of its own, which is to say when no node is already running.
func withNetworkFlags(c *cobra.Command, listen *string, bootstrap *[]string, transport *string) *cobra.Command {
	c.Flags().StringVar(listen, "listen", ":3000", "listen address (only used when no node is running)")
	c.Flags().StringSliceVar(bootstrap, "bootstrap", nil, "bootstrap nodes (only used when no node is running)")
	c.Flags().StringVar(transport, "transport", node.TransportLibp2p, "network transport: libp2p or tcp (must match the network's)")
	return c
}

// withBootstrap adds addr to the bootstrap list if it is not already there.
func withBootstrap(bootstrap []string, addr string) []string {
	for _, b := range bootstrap {
		if b == addr {
			return bootstrap
		}
	}
	return append(bootstrap, addr)
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

func setupCommands() *cobra.Command {
	var (
		dbPath     string
		listen     string
		bootstrap  []string
		configPath string
		replicas   int
		transport  string

		httpAddr    string
		httpExposed bool
		discover    bool
	)

	root := &cobra.Command{
		Use:   "p2p",
		Short: "Decentralized P2P storage node",

		// A command that fails at run time has already parsed its flags
		// correctly, so printing the usage text buries the actual error.
		SilenceUsage: true,
	}
	// One default path, not one per working directory. The old default was
	// "p2p.db", resolved against wherever the command happened to be run, so
	// the same command in two directories talked to two different databases
	// and the second one was empty. This is the path config.example has always
	// documented.
	root.PersistentFlags().StringVar(&dbPath, "db", defaultDBPath, "sqlite database path")

	// Resolved once, before any command runs, so every path below sees the
	// same expanded value.
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		expanded, err := expandHome(dbPath)
		if err != nil {
			return err
		}
		dbPath = expanded
		return nil
	}

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
				if !cmd.Flags().Changed("transport") && cfg.Transport != "" {
					transport = cfg.Transport
				}
				if !cmd.Flags().Changed("discover") && cfg.Discover {
					discover = true
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
			s, err := node.NewServer(transport, listen, d, bootstrap...)
			if err != nil {
				return err
			}
			s.EncryptionKey = keyBytes
			s.ReplicationFactor = replicas
			s.OwnsDatabase = true

			if err := s.Listen(); err != nil {
				return err
			}

			// After Listen: discovery announces on the network, so it only
			// makes sense once the node is reachable.
			if discover {
				if err := s.EnableDiscovery(); err != nil {
					return err
				}
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
	serveCmd.Flags().StringVar(&transport, "transport", node.TransportLibp2p, "network transport: libp2p or tcp (peers must use the same one)")
	serveCmd.Flags().BoolVar(&discover, "discover", false, "announce on the local network and list peers found there (needs --transport libp2p)")
	serveCmd.Flags().BoolVar(&httpExposed, "http-exposed", false, "allow binding the web UI off loopback, protected by a token file")
	root.AddCommand(serveCmd)

	storeCmd := &cobra.Command{
		Use:   "store <name> <file>",
		Short: "Store a file locally and broadcast to peers",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, path := args[0], args[1]
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			return onNode(dbPath, transport, listen, bootstrap, 0, func(t nodeTarget) error {
				return t.Store(name, f)
			})
		},
	}
	withNetworkFlags(storeCmd, &listen, &bootstrap, &transport)
	root.AddCommand(storeCmd)

	getCmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Fetch a file (local or from peers)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
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

			return onNode(dbPath, transport, listen, bootstrap, 0, func(t nodeTarget) error {
				return writeOut(func(w io.Writer) error { return t.Get(name, w) })
			})
		},
	}
	withNetworkFlags(getCmd, &listen, &bootstrap, &transport)
	getCmd.Flags().String("out", "", "output file path")
	root.AddCommand(getCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a file locally and from all peers",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			return onNode(dbPath, transport, listen, bootstrap, 0, func(t nodeTarget) error {
				return t.Delete(name)
			})
		},
	}
	withNetworkFlags(deleteCmd, &listen, &bootstrap, &transport)
	root.AddCommand(deleteCmd)

	// "files" lists on its own; "files list" still works, because that is what
	// the README and the thesis document.
	listFiles := func(cmd *cobra.Command, args []string) error {
		// Routed through the running node rather than opening the
		// database directly: the node owns it, and its cached
		// replication measurements come along for free.
		var files []node.ReplicaSnapshot
		err := onNodeReading(dbPath, func(t nodeTarget) error {
			var err error
			files, err = t.Files()
			return err
		})
		if err != nil {
			return err
		}
		if len(files) == 0 {
			fmt.Println("No files stored here.")
			return nil
		}

		fmt.Printf("%-24s\t%-10s\t%-12s\t%s\n", "FILE", "SIZE", "COPIES", "CHECKED")
		fmt.Println(strings.Repeat("-", 72))

		stale := false
		for _, f := range files {
			copies := "not checked"
			checked := "never"
			if f.Measured() {
				copies = fmt.Sprintf("%d of %d", f.Copies, f.Target)
				checked = f.Age().Round(time.Second).String() + " ago"
			} else {
				stale = true
			}
			fmt.Printf("%-24s\t%-10d\t%-12s\t%s\n", f.Name, f.Size, copies, checked)
		}

		// Said plainly, because this table and the one 'p2p status' prints
		// show the same columns with different numbers, and nothing else
		// explains why: these counts are remembered, those are measured.
		if stale {
			fmt.Println("\nCounts are from the last check. 'p2p status' measures them now.")
		} else {
			fmt.Println("\nCounts are from the last check, not measured just now. Use 'p2p status' for that.")
		}
		return nil
	}

	filesCmd := &cobra.Command{
		Use:   "files",
		Short: "List stored files and their last known replication",
		RunE:  listFiles,
	}
	filesCmd.AddCommand(&cobra.Command{
		Use:    "list",
		Short:  "List stored files",
		RunE:   listFiles,
		Hidden: true,
	})
	root.AddCommand(filesCmd)

	sharesCmd := &cobra.Command{
		Use:   "shares",
		Short: "List file shares (files stored in other peers)",
		RunE: func(cmd *cobra.Command, args []string) error {
			var shares []node.ShareView
			err := onNodeReading(dbPath, func(t nodeTarget) error {
				var err error
				shares, err = t.Shares()
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
			err := onNodeReading(dbPath, func(t nodeTarget) error {
				var err error
				peers, err = t.Peers()
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
			err := onNodeReading(dbPath, func(t nodeTarget) error {
				var err error
				view, err = t.Node()
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
			return onNodeReading(dbPath, func(t nodeTarget) error {
				if err := t.Trust(args[0], label); err != nil {
					return err
				}

				// Read back rather than echoing what was typed: the argument
				// may have been an abbreviation, and confirming the full
				// identity is how the operator knows it resolved to the peer
				// they meant.
				approved, mode, err := t.Trusted()
				if err != nil {
					return err
				}
				for _, p := range approved {
					if !strings.HasPrefix(p.NodeID, args[0]) && p.NodeID != args[0] {
						continue
					}
					state := "not connected — connecting now if it is reachable"
					if p.Online {
						state = "connected"
					}
					fmt.Printf("Approved %s (%s).\n", storage.Short(p.NodeID), state)
					if mode != dbpkg.TrustModeEnforcing {
						fmt.Println("Approval is recorded but not enforced. See 'p2p trust mode'.")
					} else if p.Online {
						fmt.Println("Any copies this makes possible are being placed now.")
					}
					return nil
				}
				fmt.Printf("Approved %s.\n", args[0])
				return nil
			})
		},
	}

	trustRemoveCmd := &cobra.Command{
		Use:   "remove <node-id>",
		Short: "Withdraw approval from a peer",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var had bool
			err := onNodeReading(dbPath, func(t nodeTarget) error {
				var err error
				had, err = t.Untrust(args[0])
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
			err := onNodeReading(dbPath, func(t nodeTarget) error {
				var err error
				trusted, mode, err = t.Trusted()
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
			err := onNodeReading(dbPath, func(t nodeTarget) error {
				var err error
				mode, err = t.Mode(wanted)
				return err
			})
			if err != nil {
				return err
			}
			fmt.Printf("Trust mode: %s\n", mode)
			return nil
		},
	}

	for _, c := range []*cobra.Command{trustAddCmd, trustRemoveCmd, trustListCmd, trustModeCmd} {
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
			var approvedOnline, connected int
			approvedIDs := map[string]bool{}
			err := onNode(dbPath, transport, listen, bootstrap, replicas, func(t nodeTarget) error {
				var err error
				if health, err = t.Status(replicas); err != nil {
					return err
				}

				// Gathered so the advice below can tell "not copied yet" from
				// "nowhere to copy to", which look identical in the table.
				peers, err := t.Peers()
				if err != nil {
					return err
				}
				trusted, _, err := t.Trusted()
				if err != nil {
					return err
				}
				approved := make(map[string]bool, len(trusted))
				for _, p := range trusted {
					approved[p.NodeID] = true
				}
				for _, p := range peers {
					if !p.Online {
						continue
					}
					connected++
					if approved[p.NodeID] {
						approvedOnline++
						approvedIDs[p.NodeID] = true
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
			if len(health) == 0 {
				fmt.Println("No files stored locally.")
				return nil
			}

			fmt.Println("Asked every connected peer just now.")
			fmt.Println()
			fmt.Printf("%-24s\t%-12s\t%-10s\t%s\n", "FILE", "COPIES", "SIZE", "STATE")
			fmt.Println(strings.Repeat("-", 70))

			atRisk := 0
			placeable := 0
			for _, h := range health {
				state := "ok"
				if h.AtRisk() {
					state = "AT RISK"
					atRisk++

					// Somewhere for another copy to go means an approved peer
					// that is connected and does not already hold this file.
					holders := make(map[string]bool, len(h.Holders))
					for _, id := range h.Holders {
						holders[id] = true
					}
					for id := range approvedIDs {
						if !holders[id] {
							placeable++
							break
						}
					}
				}
				fmt.Printf("%-24s\t%d of %-8d\t%-10d\t%s\n", h.Name, h.Copies, h.Target, h.Size, state)
			}

			if atRisk > 0 {
				fmt.Printf("\n%d file(s) below the replication target of %d.\n", atRisk, replicas)

				// Three quite different situations reach this line, and the
				// table cannot tell them apart. Suggesting a repair that
				// cannot place anything is how a working system looks broken.
				switch {
				case connected == 0:
					fmt.Println("No peers are connected, so there is nowhere to put another copy.")
					fmt.Println("Start another node, or check 'p2p peers'.")
				case approvedOnline == 0:
					fmt.Printf("%d peer(s) are connected but none is approved, so copies cannot be placed.\n", connected)
					fmt.Println("Approve one with 'p2p trust add <peer>', or see 'p2p peers'.")
				case placeable == 0:
					fmt.Printf("Every approved peer already holds a copy, so %d is as many as this\n", approvedOnline+1)
					fmt.Println("network can hold. Approve or start more nodes, or lower --replicas.")
				default:
					fmt.Println("Run 'p2p repair' to place the missing copies, or start more nodes.")
				}
			}
			return nil
		},
	}
	withNetworkFlags(statusCmd, &listen, &bootstrap, &transport)
	statusCmd.Flags().IntVar(&replicas, "replicas", node.DefaultReplicationFactor, "replication target to measure against")
	root.AddCommand(statusCmd)

	repairCmd := &cobra.Command{
		Use:   "repair",
		Short: "Place missing copies of under-replicated files",
		RunE: func(cmd *cobra.Command, args []string) error {
			var placed int
			err := onNode(dbPath, transport, listen, bootstrap, replicas, func(t nodeTarget) error {
				var err error
				placed, err = t.Repair(replicas)
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
	withNetworkFlags(repairCmd, &listen, &bootstrap, &transport)
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

				s, err := node.NewServer(transport, spec.listen, d, spec.bootstrap...)
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
