package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Listen    string
	DB        string
	Bootstrap []string
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
