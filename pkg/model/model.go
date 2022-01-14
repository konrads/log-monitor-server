package model

import (
	"time"

	"github.com/caarlos0/env/v6"
)

type Config struct {
	BatchSize uint          `env:"BATCH_SIZE"`
	FlushFreq time.Duration `env:"FLUSH_FREQ"`
	MysqlURL  string        `env:"MYSQL_URL"`
}

func NewConfig() (Config, error) {
	config := Config{}
	err := env.Parse(&config)
	return config, err
}

type Message struct {
	ServiceName string    `json:"service_name"`
	Payload     string    `json:"payload"`
	Severity    string    `json:"severity"`
	Timestamp   time.Time `json:"timestamp"`
}

type MessageSeverity struct {
	ServiceName string `json:"service_name"`
	Severity    string `json:"severity"`
	Count       int    `json:"count"`
}

type BatchEvent struct {
	Messages []Message
}

type PersistEvent struct {
	Messages []Message
}
