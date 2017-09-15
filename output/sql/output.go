package sql

import (
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/op"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/mode"
	"github.com/elastic/beats/libbeat/outputs/mode/modeutil"
)

type sqlOutput struct {
	mode     mode.ConnectionMode
	beatName string
}

var (
	debugf = logp.MakeDebug("sql")
)

const (
	waitRetry    = 1 * time.Second
	maxWaitRetry = 60 * time.Second
)

func init() {
	outputs.RegisterOutputPlugin("sql", new)
}

func new(beatName string, cfg *common.Config, topologyExpire int) (outputs.Outputer, error) {
	s := &sqlOutput{beatName: beatName}
	if err := s.init(cfg); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sqlOutput) init(cfg *common.Config) error {
	config := sqlConfig{}
	if err := cfg.Unpack(&config); err != nil {
		return err
	}

	client, err := newClient(&config)
	if err != nil {
		return err
	}
	clients := make([]mode.ProtocolClient, 0, 1)
	clients = append(clients, client)

	maxRetries := config.MaxRetries
	maxAttempts := maxRetries + 1
	if maxRetries < 0 {
		maxAttempts = 0
	}

	m, err := modeutil.NewConnectionMode(clients, modeutil.Settings{
		Failover:     false,
		MaxAttempts:  maxAttempts,
		Timeout:      config.Timeout,
		WaitRetry:    waitRetry,
		MaxWaitRetry: maxWaitRetry,
	})
	if err != nil {
		return err
	}

	s.mode = m

	return nil
}

func (s *sqlOutput) Close() error {
	return s.mode.Close()
}

func (s *sqlOutput) PublishEvent(
	signaler op.Signaler,
	opts outputs.Options,
	data outputs.Data,
) error {
	return s.mode.PublishEvent(signaler, opts, data)
}

func (s *sqlOutput) BulkPublish(
	signaler op.Signaler,
	opts outputs.Options,
	data []outputs.Data,
) error {
	return s.mode.PublishEvents(signaler, opts, data)
}
