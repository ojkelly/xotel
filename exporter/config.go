package exporter

import (
	"log"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Debug       bool          // XOTEL_DEBUG
	MaxLookBack time.Duration `default:"6m"` // XOTEL_MAX_LOOK_BACK
	MinLookBack time.Duration `default:"1m"` // XOTEL_MIN_LOOK_BACK
}

func getConfig() Config {
	var cfg Config
	err := envconfig.Process("XOTEL", &cfg)
	if err != nil {
		log.Fatal(err.Error())
	}

	return cfg
}
