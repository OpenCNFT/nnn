package config

import (
	"errors"
	"os"

	"github.com/BurntSushi/toml"
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// Config is a container for everything found in the TOML config file
type Config struct {
	ListenAddr    string          `toml:"listen_addr" split_words:"true"`
	GitalyServers []*GitalyServer `toml:"gitaly_server", split_words:"true"`
	Logging       config.Logging  `toml:"logging"`
}

// GitalyServer allows configuring the servers that RPCs are proxied to
type GitalyServer struct {
	Name       string `toml:"name"`
	ListenAddr string `toml:"listen_addr" split_words:"true"`
}

// FromFile loads the config for the passed file path
func FromFile(filePath string) (Config, error) {
	config := &Config{}
	cfgFile, err := os.Open(filePath)
	if err != nil {
		return *config, err
	}
	defer cfgFile.Close()

	_, err = toml.DecodeReader(cfgFile, config)
	return *config, err
}

var (
	errNoListenAddr        = errors.New("no listen address configured")
	errNoGitalyServers     = errors.New("no gitaly backends configured")
	errDuplicateGitalyAddr = errors.New("gitaly listen addresses are not unique")
	errGitalyWithoutName   = errors.New("all gitaly servers must have a name")
)

// Validate establishes if the config is valid
func (c Config) Validate() error {
	if c.ListenAddr == "" {
		return errNoListenAddr
	}

	if len(c.GitalyServers) == 0 {
		return errNoGitalyServers
	}

	listenAddrs := make(map[string]bool, len(c.GitalyServers))
	for _, gitaly := range c.GitalyServers {
		if gitaly.Name == "" {
			return errGitalyWithoutName
		}

		if _, found := listenAddrs[gitaly.ListenAddr]; found {
			return errDuplicateGitalyAddr
		}

		listenAddrs[gitaly.ListenAddr] = true
	}

	return nil
}
