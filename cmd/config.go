package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/crawler/pkg/crawler"
	"github.com/vertex-lab/crawler/pkg/utils/logger"
)

type SystemConfig struct {
	Log                 *logger.Aggregate
	LogWriter           io.Writer
	DisplayStats        bool
	EventQueueCapacity  int
	PubkeyQueueCapacity int
	InitPubkeys         []string // only used during initialization
}

// The configuration parameters for the system and the main processes.
type Config struct {
	SystemConfig
	Firehose crawler.FirehoseConfig
	Query    crawler.QueryPubkeysConfig
	Arbiter  crawler.NodeArbiterConfig
	Process  crawler.ProcessEventsConfig
}

func NewSystemConfig() SystemConfig {
	return SystemConfig{
		LogWriter:           os.Stdout,
		DisplayStats:        false,
		EventQueueCapacity:  1000,
		PubkeyQueueCapacity: 1000,
	}
}

// NewConfig() returns a config with default parameters.
func NewConfig() *Config {
	return &Config{
		SystemConfig: NewSystemConfig(),
		Firehose:     crawler.NewFirehoseConfig(),
		Query:        crawler.NewQueryPubkeysConfig(),
		Arbiter:      crawler.NewNodeArbiterConfig(),
		Process:      crawler.NewProcessEventsConfig(),
	}
}

func (c SystemConfig) Print() {
	fmt.Println("System:")
	fmt.Printf("  LogWriter: %T\n", c.LogWriter)
	fmt.Printf("  DisplayStats: %t\n", c.DisplayStats)
	fmt.Printf("  EventQueueCapacity: %d\n", c.EventQueueCapacity)
	fmt.Printf("  PubkeyQueueCapacity: %d\n", c.PubkeyQueueCapacity)
	fmt.Printf("  InitPubkeys: %v\n", c.InitPubkeys)
}

func (c *Config) Print() {
	c.SystemConfig.Print()
	c.Firehose.Print()
	c.Query.Print()
	c.Arbiter.Print()
	c.Process.Print()
}

// LoadConfig() read the variables from the enviroment and parses them into a config struct.
func LoadConfig() (*Config, error) {
	var config = NewConfig()
	var err error

	for _, item := range os.Environ() {
		keyVal := strings.SplitN(item, "=", 2)
		key, val := keyVal[0], keyVal[1]

		switch key {
		case "LOGS":
			// LogWriter gets updated if a .log file is specified; otherwise it remains os.Stdout
			if strings.HasSuffix(val, ".log") {
				config.LogWriter, err = os.OpenFile(val, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
				if err != nil {
					return nil, fmt.Errorf("error opening file \"%v\": %v", val, err)
				}
			}

			config.Log = logger.New(config.LogWriter)
			config.Firehose.Log = config.Log
			config.Query.Log = config.Log
			config.Process.Log = config.Log
			config.Arbiter.Log = config.Log

		case "DISPLAY_STATS":
			config.DisplayStats, err = strconv.ParseBool(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "EVENT_QUEUE_CAPACITY":
			config.EventQueueCapacity, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "PUBKEY_QUEUE_CAPACITY":
			config.PubkeyQueueCapacity, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "RELAYS":
			relays := strings.Split(val, ",")
			if len(relays) == 0 {
				return nil, fmt.Errorf("list of relays is empty")
			}

			for _, rel := range relays {
				if !nostr.IsValidRelayURL(rel) {
					return nil, fmt.Errorf("relay \"%s\" is not a valid url", rel)
				}
			}

			config.Firehose.Relays = relays
			config.Query.Relays = relays

		case "QUERY_BATCH_SIZE":
			config.Query.BatchSize, err = strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "QUERY_INTERVAL":
			queryInterval, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}
			config.Query.Interval = time.Duration(queryInterval) * time.Second

		case "NODE_ARBITER_ACTIVATION_THRESHOLD":
			config.Arbiter.ActivationThreshold, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "PROMOTION_MULTIPLIER":
			config.Arbiter.PromotionMultiplier, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "DEMOTION_MULTIPLIER":
			config.Arbiter.DemotionMultiplier, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}

		case "PROCESS_PRINT_EVERY":
			printEvery, err := strconv.Atoi(val)
			if err != nil {
				return nil, fmt.Errorf("error parsing %v: %v", keyVal, err)
			}
			config.Process.PrintEvery = uint32(printEvery)

		case "INIT_PUBKEYS":
			pubkeys := strings.Split(val, ",")
			for _, pk := range pubkeys {
				if !nostr.IsValidPublicKey(pk) {
					return nil, fmt.Errorf("pubkey %s is not valid", pk)
				}
			}

			config.InitPubkeys = pubkeys
		}
	}

	return config, nil
}

// CloseLogs() closes the config.LogWriter if that is a file.
func (c *Config) CloseLogs() {
	if file, ok := c.LogWriter.(*os.File); ok && file != os.Stdout {
		file.Close()
	}
}
