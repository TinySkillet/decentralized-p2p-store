package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
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
func startClientNode(listen string, d *dbpkg.DB, bootstrap []string) (*FileServer, func(), error) {
	keyBytes, err := loadOrInitKey(d)
	if err != nil {
		return nil, nil, err
	}

	s, err := makeClientNode(listen, d, bootstrap...)
	if err != nil {
		return nil, nil, err
	}
	s.EncryptionKey = keyBytes

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

			if err := s.Listen(); err != nil {
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

			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			s, stop, err := startClientNode(listen, d, bootstrap)
			if err != nil {
				return err
			}
			defer stop()

			return s.Store(key, f)
		},
	}
	storeCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address")
	storeCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes")
	root.AddCommand(storeCmd)

	getCmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Fetch a file (local or from peers)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			out, _ := cmd.Flags().GetString("out")

			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			s, stop, err := startClientNode(listen, d, bootstrap)
			if err != nil {
				return err
			}
			defer stop()

			_, r, err := s.Get(key)
			if err != nil {
				return err
			}
			if rc, ok := r.(io.Closer); ok {
				defer rc.Close()
			}

			var w io.Writer = os.Stdout
			if out != "" {
				of, err := os.Create(out)
				if err != nil {
					return err
				}
				defer of.Close()
				w = of
			}
			_, err = io.Copy(w, r)
			return err
		},
	}
	getCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address")
	getCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes")
	getCmd.Flags().String("out", "", "output file path")
	root.AddCommand(getCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <key>",
		Short: "Delete a file locally and from all peers",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]

			d, err := openDB(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()

			s, stop, err := startClientNode(listen, d, bootstrap)
			if err != nil {
				return err
			}
			defer stop()

			return s.Delete(key)
		},
	}
	deleteCmd.Flags().StringVar(&listen, "listen", ":3000", "listen address")
	deleteCmd.Flags().StringSliceVar(&bootstrap, "bootstrap", nil, "bootstrap nodes")
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

	demoCmd := &cobra.Command{
		Use:   "demo",
		Short: "Run the local 3-node demo",
		RunE: func(cmd *cobra.Command, args []string) error {
			servers := make([]*FileServer, 0, 3)
			specs := []struct {
				listen    string
				bootstrap []string
			}{
				{":3000", nil},
				{":4000", []string{":3000"}},
				{":5000", []string{":3000", ":4000"}},
			}

			for _, spec := range specs {
				s, err := makeServer(spec.listen, spec.bootstrap...)
				if err != nil {
					return err
				}
				if err := s.Listen(); err != nil {
					return err
				}
				go s.Serve()
				defer s.Stop()

				servers = append(servers, s)
			}

			s3 := servers[2]
			if err := s3.waitForPeers(peerWaitTimeout); err != nil {
				return err
			}
			s3.waitForPeerDiscovery(discoveryQuietPeriod, discoverySettleTimeout)

			key := "coolpicture.jpg"
			data := bytes.NewReader([]byte("my big data file here!"))
			if err := s3.Store(key, data); err != nil {
				return err
			}
			if err := s3.Delete(key); err != nil {
				return err
			}

			_, r, err := s3.Get(key)
			if err != nil {
				return err
			}
			b, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			fmt.Println(string(b))
			return nil
		},
	}
	root.AddCommand(demoCmd)

	return root
}
