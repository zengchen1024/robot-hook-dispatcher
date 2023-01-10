package main

import (
	"errors"
	"strings"

	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/mq"
)

type configuration struct {
	Topic          string `json:"topic"           required:"true"`
	UserAgent      string `json:"user_agent"      required:"true"`
	KafkaAddress   string `json:"kafka_address"   required:"true"`
	AccessEndpoint string `json:"access_endpoint" required:"true"`
	ConcurrentSize int    `json:"concurrent_size" required:"true"`
}

func (c *configuration) Validate() error {
	if c.Topic == "" {
		return errors.New("missing topic")
	}

	if c.UserAgent == "" {
		return errors.New("missing user_agent")
	}

	if c.KafkaAddress == "" {
		return errors.New("missing kafka_address")
	}

	if c.AccessEndpoint == "" {
		return errors.New("missing access_endpoint")
	}

	if c.ConcurrentSize <= 0 {
		return errors.New("Concurrent_size must be > 0")
	}

	return nil
}

func (c *configuration) SetDefault() {}

func (c *configuration) kafkaConfig() (cfg mq.MQConfig, err error) {
	v := strings.Split(c.KafkaAddress, ",")
	if err = kafka.ValidateConnectingAddress(v); err == nil {
		cfg.Addresses = v
	}

	return
}
