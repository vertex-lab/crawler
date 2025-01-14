package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/nbd-wtf/go-nostr"
)

// The configuration parameters for the crawler.
type Config struct {
	LogWriter          io.Writer
	DisplayStats       bool
	InitPubkeys        []string // only used during initialization
	EventChanCapacity  int
	PubkeyChanCapacity int

	QueryBatchSize int
	QueryInterval  time.Duration

	NodeArbiterActivationThreshold float64
	PromotionMultiplier            float64
	DemotionMultiplier             float64
}

// NewConfig() returns a config with default parameters.
func NewConfig() *Config {
	return &Config{
		LogWriter:                      os.Stdout,
		DisplayStats:                   false,
		EventChanCapacity:              1000,
		PubkeyChanCapacity:             1000,
		QueryBatchSize:                 5,
		QueryInterval:                  30 * time.Second,
		NodeArbiterActivationThreshold: 0.01,
		PromotionMultiplier:            0.1,
		DemotionMultiplier:             1.1,
	}
}

// LoadConfig() read the variables from the specified .env file and returns an initialized config.
// If the .env file doesn't exist, default parameters are returned.
func LoadConfig(envFile string) (*Config, error) {
	var config = NewConfig()
	var err error

	if err = godotenv.Load(envFile); err != nil {
		return config, nil
	}

	for _, item := range os.Environ() {
		parts := strings.SplitN(item, "=", 2)
		key, val := parts[0], parts[1]

		switch key {
		case "LOGS":
			switch val {
			case "terminal":
				config.LogWriter = os.Stdout
			default:
				config.LogWriter, err = os.OpenFile(val, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
				if err != nil {
					return nil, fmt.Errorf("error opening file %v: %v", val, err)
				}
			}

		case "DISPLAY_STATS":
			config.DisplayStats, err = strconv.ParseBool(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", parts, err)
			}

		case "EVENT_CHAN_CAPACITY":
			config.EventChanCapacity, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", parts, err)
			}

		case "PUBKEY_CHAN_CAPACITY":
			config.PubkeyChanCapacity, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", parts, err)
			}

		case "QUERY_BATCH_SIZE":
			config.QueryBatchSize, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", parts, err)
			}

		case "QUERY_INTERVAL":
			queryInterval, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", parts, err)
			}
			config.QueryInterval = time.Duration(queryInterval) * time.Second

		case "NODE_ARBITER_ACTIVATION_THRESHOLD":
			config.NodeArbiterActivationThreshold, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", parts, err)
			}

		case "PROMOTION_MULTIPLIER":
			config.PromotionMultiplier, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", parts, err)
			}

		case "DEMOTION_MULTIPLIER":
			config.DemotionMultiplier, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", parts, err)
			}

		case "INIT_PUBKEYS":
			pubkeys := strings.Split(val, ",")
			for _, pk := range pubkeys {
				if nostr.IsValidPublicKey(pk) {
					config.InitPubkeys = append(config.InitPubkeys, pk)
				}
			}
		}
	}

	return config, nil
}

// CloseLogs() closes the config.LogWriter if that is a file.
func (c *Config) CloseLogs() {
	if file, ok := c.LogWriter.(*os.File); ok {
		file.Close()
	}
}

func (c *Config) Print() {
	fmt.Println("Config:")
	fmt.Printf("  LogWriter: %T\n", c.LogWriter) // Prints the type of LogWriter
	fmt.Printf("  DisplayStats: %t\n", c.DisplayStats)
	fmt.Printf("  InitPubkeys: %v\n", c.InitPubkeys)
	fmt.Printf("  EventChanCapacity: %d\n", c.EventChanCapacity)
	fmt.Printf("  PubkeyChanCapacity: %d\n", c.PubkeyChanCapacity)
	fmt.Printf("  QueryBatchSize: %d\n", c.QueryBatchSize)
	fmt.Printf("  QueryInterval: %s\n", c.QueryInterval)
	fmt.Printf("  NodeArbiterActivationThreshold: %f\n", c.NodeArbiterActivationThreshold)
	fmt.Printf("  PromotionMultiplier: %f\n", c.PromotionMultiplier)
	fmt.Printf("  DemotionMultiplier: %f\n", c.DemotionMultiplier)
}
