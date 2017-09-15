package sql

import (
	"time"
)

type sqlConfig struct {
	Driver     string        `config:"driver"`
	DSN        string        `config:"dsn"`
	Table      string        `config:"table"`
	MaxRetries int           `config:"max_retries"`
	Timeout    time.Duration `config:"timeout"`
}
