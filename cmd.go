package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	dbpkg "github.com/TinySkillet/DecentralizedP2PStorage/db"
	"github.com/spf13/cobra"
)

func setupCommands() *cobra.Command {
	var (
		dbPath     string
		listen     string
		bootstrap  []string
		configPath string
	)

	root := &cobra.Command{Use: "p2p", Short: "Decentralized P2P storage node"}
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

			d, err := dbpkg.Open(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.Migrate(context.Background()); err != nil {
				return err
			}
			keyBytes, err := loadOrInitKey(d)
			if err != nil {
				return err
			}
			s := makeServerWithDB(listen, d, bootstrap...)
			s.EncryptionKey = keyBytes
			return s.Start()
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

			d, err := dbpkg.Open(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.Migrate(context.Background()); err != nil {
				return err
			}

			keyBytes, err := loadOrInitKey(d)
			if err != nil {
				return err
			}
			s := makeServerWithDB(listen, d, bootstrap...)
			s.EncryptionKey = keyBytes
			go func() { log.Fatal(s.Start()) }()
			time.Sleep(1 * time.Second)
			if len(bootstrap) > 0 {
				if err := s.waitForPeers(10 * time.Second); err != nil {
					fmt.Printf("Warning: %v. Proceeding with store anyway.\n", err)
				}
				// Give peer discovery a bit more time to connect to ALL peers
				time.Sleep(2 * time.Second)
			}
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

			d, err := dbpkg.Open(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.Migrate(context.Background()); err != nil {
				return err
			}

			keyBytes, err := loadOrInitKey(d)
			if err != nil {
				return err
			}
			s := makeServerWithDB(listen, d, bootstrap...)
			s.EncryptionKey = keyBytes
			go func() { log.Fatal(s.Start()) }()
			time.Sleep(1 * time.Second)
			if len(bootstrap) > 0 {
				if err := s.waitForPeers(10 * time.Second); err != nil {
					fmt.Printf("Warning: %v. Proceeding with get anyway.\n", err)
				}
				time.Sleep(1 * time.Second)
			}
			_, r, err := s.Get(key)
			if err != nil {
				return err
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

			d, err := dbpkg.Open(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.Migrate(context.Background()); err != nil {
				return err
			}

			keyBytes, err := loadOrInitKey(d)
			if err != nil {
				return err
			}
			s := makeServerWithDB(listen, d, bootstrap...)
			s.EncryptionKey = keyBytes
			go func() { log.Fatal(s.Start()) }()
			time.Sleep(1 * time.Second)
			if len(bootstrap) > 0 {
				if err := s.waitForPeers(10 * time.Second); err != nil {
					fmt.Printf("Warning: %v. Proceeding with delete anyway.\n", err)
				}
				time.Sleep(1 * time.Second)
			}
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
			d, err := dbpkg.Open(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.Migrate(context.Background()); err != nil {
				return err
			}
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
			d, err := dbpkg.Open(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.Migrate(context.Background()); err != nil {
				return err
			}
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
			d, err := dbpkg.Open(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.Migrate(context.Background()); err != nil {
				return err
			}

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
			d, err := dbpkg.Open(dbPath)
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.Migrate(context.Background()); err != nil {
				return err
			}

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
			s1 := makeServer(":3000", "")
			s2 := makeServer(":4000", ":3000")
			s3 := makeServer(":5000", ":3000", ":4000")

			go func() { log.Fatal(s1.Start()) }()
			time.Sleep(1 * time.Second)
			go func() { log.Fatal(s2.Start()) }()
			time.Sleep(1 * time.Second)
			go s3.Start()
			time.Sleep(1 * time.Second)

			key := "coolpicture.jpg"
			data := bytes.NewReader([]byte("my big data file here!"))
			_ = s3.Store(key, data)
			_ = s3.Delete(key)
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
