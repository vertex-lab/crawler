package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

// The configuration parameters for the crawler.
type Config struct {
	Mode                           string
	RedisClient                    *redis.Client
	LogWriter                      io.Writer
	DisplayStats                   bool
	EventChanCapacity              int
	PubkeyChanCapacity             int
	QueryPubkeysBatchSize          int
	NodeArbiterActivationThreshold float64
	PromotionMultiplier            float64
	DemotionMultiplier             float64
}

// NewConfig() returns a config with default parameters.
func NewConfig() *Config {
	return &Config{
		Mode:                           "prod",
		RedisClient:                    redis.NewClient(&redis.Options{Addr: "localhost:6379"}),
		LogWriter:                      os.Stdout,
		DisplayStats:                   false,
		EventChanCapacity:              1000,
		PubkeyChanCapacity:             1000,
		QueryPubkeysBatchSize:          5,
		NodeArbiterActivationThreshold: 0.01,
		PromotionMultiplier:            0.1,
		DemotionMultiplier:             1.1,
	}
}

// LoadConfig() read the variables from the .env file and returns an initialized config.
// If the .env file doesn't exist, default parameters are returned.
func LoadConfig() (*Config, error) {
	var config = NewConfig()
	var err error

	size, err := config.RedisClient.DBSize(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	switch size {
	case 0:
		// if the DB is empty, run with initialization parameters.
		config.Mode = "init"
		if err = godotenv.Load("init.env"); err != nil {
			return nil, fmt.Errorf("failed to load init.env: %v", err)
		}

	default:
		config.Mode = "prod"
		if err = godotenv.Load("prod.env"); err != nil {
			return config, nil
		}
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
		return nil, fmt.Errorf("error parsing DISPLAY_STATS: %v", err)
	}

	config.EventChanCapacity, err = strconv.Atoi(os.Getenv("EVENT_CHAN_CAPACITY"))
	if err != nil {
		return nil, fmt.Errorf("error parsing EVENT_CHAN_CAPACITY: %v", err)
	}

	config.PubkeyChanCapacity, err = strconv.Atoi(os.Getenv("PUBKEY_CHAN_CAPACITY"))
	if err != nil {
		return nil, fmt.Errorf("error parsing PUBKEY_CHAN_CAPACITY: %v", err)
	}

	config.QueryPubkeysBatchSize, err = strconv.Atoi(os.Getenv("QUERY_PUBKEYS_BATCH_SIZE"))
	if err != nil {
		return nil, fmt.Errorf("error parsing QUERY_PUBKEYS_BATCH_SIZE: %v", err)
	}

	config.NodeArbiterActivationThreshold, err = strconv.ParseFloat(os.Getenv("NODE_ARBITER_ACTIVATION_THRESHOLD"), 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing NODE_ARBITER_ACTIVATION_THRESHOLD: %v", err)
	}

	config.PromotionMultiplier, err = strconv.ParseFloat(os.Getenv("PROMOTION_MULTIPLIER"), 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing PROMOTION_MULTIPLIER: %v", err)
	}

	config.DemotionMultiplier, err = strconv.ParseFloat(os.Getenv("DEMOTION_MULTIPLIER"), 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing DEMOTION_MULTIPLIER: %v", err)
	}

	return config, nil
}

// CloseLogs() closes the config.LogWriter if that is a file.
func (c *Config) CloseLogs() {
	if file, ok := c.LogWriter.(*os.File); ok {
		file.Close()
	}
}
