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
	Endpoint       string `json:"endpoint" required:"true"`
	ConcurrentSize int    `json:"concurrent_size"`
}

func (a *accessConfig) validate() error {
	if a.Topic == "" {
		return errors.New("missing topic")
	}

	if a.Endpoint == "" {
		return errors.New("missing endpoint")
	}

	return nil
}

func (a *accessConfig) setDefault() {
	if a.ConcurrentSize <= 0 {
		a.ConcurrentSize = 40
	}
}
