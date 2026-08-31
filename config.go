package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	Listen    string
	DB        string
	Bootstrap []string
	Replicas  int

	// HTTP is the address for the local web UI. Empty means off, which is the
	// default: the UI can administer trust, so a node does not acquire it by
	// being upgraded.
	HTTP string

	// HTTPExposed permits binding the UI to something other than loopback.
	// Separate from HTTP so that exposing it is always a deliberate second
	// decision.
	HTTPExposed bool

	// Transport selects the network transport: "tcp" (default) or "libp2p".
	// Every node on a network must use the same one.
	Transport string

	// Discover announces this node on the local network and lists peers
	// found there. Discovered peers are visible and approvable, nothing
	// more; only approved peers are dialled. Needs the libp2p transport.
	Discover bool
}

func LoadConfig(path string) (*Config, error) {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		path = filepath.Join(home, path[2:])
	}

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{}, nil
		}
		return nil, err
	}
	defer file.Close()

	config := &Config{
		Bootstrap: []string{},
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "listen":
			config.Listen = value
		case "db":
			config.DB = value
		case "transport":
			config.Transport = value
		case "discover":
			config.Discover = value == "true" || value == "1" || value == "yes"
		case "http":
			config.HTTP = value
		case "http_exposed":
			config.HTTPExposed = value == "true" || value == "yes" || value == "1"
		case "replicas":
			if n, err := strconv.Atoi(value); err == nil && n > 0 {
				config.Replicas = n
			}
		case "bootstrap":
			nodes := strings.Split(value, ",")
			for _, node := range nodes {
				node = strings.TrimSpace(node)
				if node != "" {
					config.Bootstrap = append(config.Bootstrap, node)
				}
			}
		}
	}

	return config, scanner.Err()
}
