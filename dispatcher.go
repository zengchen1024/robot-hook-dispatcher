package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/mq"
	"github.com/opensourceways/community-robot-lib/utils"
	"github.com/sirupsen/logrus"
)

const (
	headerUserAgent = "User-Agent"
)

type dispatcher struct {
	log       *logrus.Entry
	hc        utils.HttpClient
	topic     string
	endpoint  string
	userAgent string
	getConfig func() (*configuration, error)

	startTime time.Time
	sentNum   int
}

func newDispatcher(getConfig func() (*configuration, error), log *logrus.Entry) (*dispatcher, error) {
	v, err := getConfig()
	if err != nil {
		return nil, err
	}
	cfg := &v.Config

	return &dispatcher{
		log:       log,
		hc:        utils.NewHttpClient(3),
		topic:     cfg.Topic,
		endpoint:  cfg.AccessEndpoint,
		userAgent: cfg.UserAgent,
		getConfig: getConfig,
	}, nil
}

func (d *dispatcher) run(ctx context.Context) error {
	s, err := kafka.Subscribe(d.topic, d.handle)
	if err != nil {
		return err
	}

	<-ctx.Done()

	return s.Unsubscribe()
}

func (d *dispatcher) handle(event mq.Event) error {
	msg := event.Message()
	if err := d.validateMessage(msg); err != nil {
		return err
	}

	d.dispatch(msg)

	d.speedControl()

	return nil
}

func (d *dispatcher) speedControl() {
	cfg, err := d.getConfig()
	if err != nil {
		d.log.Errorf("getConfig, err:%s", err.Error())
	}

	if d.sentNum++; d.sentNum == 1 {
		d.startTime = time.Now()
	} else if d.sentNum >= cfg.Config.ConcurrentSize {
		now := time.Now()
		if v := d.startTime.Add(time.Second); v.After(now) {
			du := v.Sub(now)
			time.Sleep(du)

			d.log.Debugf(
				"will sleep %s after sending %d events",
				du.String(), d.sentNum,
			)
		}

		d.sentNum = 0
	}
}

func (d *dispatcher) validateMessage(msg *mq.Message) error {
	if msg == nil {
		return errors.New("get a nil msg from broker")
	}

	if len(msg.Header) == 0 || msg.Header[headerUserAgent] != d.userAgent {
		return errors.New("unexpect message: invalid header")
	}

	if len(msg.Body) == 0 {
		return errors.New("unexpect message: The payload is empty")
	}

	return nil
}

func (d *dispatcher) dispatch(msg *mq.Message) {
	if err := d.send(msg); err != nil {
		d.log.Errorf("send message, err:%s", err.Error())
	}
}

func (d *dispatcher) send(msg *mq.Message) error {
	req, err := http.NewRequest(
		http.MethodPost, d.endpoint, bytes.NewBuffer(msg.Body),
	)
	if err != nil {
		return err
	}

	for k, v := range msg.Header {
		req.Header.Add(k, v)
	}

	_, err = d.hc.ForwardTo(req, nil)

	return err
}
