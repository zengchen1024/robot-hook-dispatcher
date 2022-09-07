package main

import (
	"errors"
)

type configuration struct {
	Config accessConfig `json:"access,omitempty"`
}

func (c *configuration) Validate() error {
	return c.Config.validate()
}

func (c *configuration) SetDefault() {}

type accessConfig struct {
	Topic          string `json:"topic" required:"true"`
	UserAgent      string `json:"user_agent" required:"true"`
	AccessEndpoint string `json:"access_endpoint" required:"true"`
	ConcurrentSize int    `json:"concurrent_size" required:"true"`
}

func (a *accessConfig) validate() error {
	if a.Topic == "" {
		return errors.New("missing topic")
	}

	if a.UserAgent == "" {
		return errors.New("missing user_agent")
	}

	if a.AccessEndpoint == "" {
		return errors.New("missing access_endpoint")
	}

	if a.ConcurrentSize <= 0 {
		return errors.New("Concurrent_size must be > 0")
	}

	return nil
}
