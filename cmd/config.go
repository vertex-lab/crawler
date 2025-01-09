package main

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// The configuration parameters for the crawler.
type Config struct {
	LogWriter                      io.Writer
	DisplayStats                   bool
	EventChanCapacity              int
	PubkeyChanCapacity             int
	QueryPubkeysBatchSize          int
	NodeArbiterActivationThreshold float64
	PagerankMultiplier             float64
}

// NewConfig() returns a config with default parameters.
func NewConfig() *Config {
	return &Config{
		LogWriter:                      os.Stdout,
		DisplayStats:                   false,
		EventChanCapacity:              1000,
		PubkeyChanCapacity:             1000,
		QueryPubkeysBatchSize:          5,
		NodeArbiterActivationThreshold: 0.01,
		PagerankMultiplier:             1.5,
	}
}

// LoadConfig() read the variables from the .env file and returns an initialized config.
// If the .env file doesn't exist, default parameters are returned.
func LoadConfig() (*Config, error) {
	var config = NewConfig()
	var err error

	if err = godotenv.Load(); err != nil {
		return config, nil
	}

	logsOut := os.Getenv("LOGS")
	switch logsOut {
	case "terminal":
		config.LogWriter = os.Stdout
	default:
		config.LogWriter, err = os.OpenFile(logsOut, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return nil, err
		}
	}

	config.DisplayStats, err = strconv.ParseBool(os.Getenv("DISPLAY_STATS"))
	if err != nil {
		return nil, err
	}

	mode := os.Getenv("MODE")
	var prefix string
	switch mode {
	case "prod":
		prefix = "PROD_"
	case "init":
		prefix = "INIT_"
	default:
		return nil, fmt.Errorf("MODE must be either `init` or `prod`: %v", mode)
	}

	config.EventChanCapacity, err = strconv.Atoi(os.Getenv(prefix + "EVENT_CHAN_CAPACITY"))
	if err != nil {
		return nil, err
	}

	config.PubkeyChanCapacity, err = strconv.Atoi(os.Getenv(prefix + "PUBKEY_CHAN_CAPACITY"))
	if err != nil {
		return nil, err
	}

	config.QueryPubkeysBatchSize, err = strconv.Atoi(os.Getenv(prefix + "QUERY_PUBKEYS_BATCH_SIZE"))
	if err != nil {
		return nil, err
	}

	config.NodeArbiterActivationThreshold, err = strconv.ParseFloat(os.Getenv(prefix+"NODE_ARBITER_ACTIVATION_THRESHOLD"), 64)
	if err != nil {
		return nil, err
	}

	config.PagerankMultiplier, err = strconv.ParseFloat(os.Getenv(prefix+"PAGERANK_MULTIPLIER"), 64)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// CloseLogs() closes the config.LogWriter if that is a file.
func (c *Config) CloseLogs() {
	if file, ok := c.LogWriter.(*os.File); ok {
		file.Close()
	}
}
