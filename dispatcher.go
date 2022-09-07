package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"

	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/mq"
	"github.com/opensourceways/community-robot-lib/utils"
	"github.com/sirupsen/logrus"
)

type dispatcher struct {
	hc        utils.HttpClient
	topic     string
	endpoint  string
	getConfig func() (*configuration, error)

	messages        chan *mq.Message
	messageChanSize int

	adjustmentDone chan struct{}
	chanEmpty      chan struct{}
	done           chan struct{}
}

func newDispatcher(getConfig func() (*configuration, error)) (*dispatcher, error) {
	v, err := getConfig()
	if err != nil {
		return nil, err
	}
	cfg := &v.Config
	size := cfg.ConcurrentSize

	return &dispatcher{
		hc:        utils.HttpClient{MaxRetries: 3},
		topic:     cfg.Topic,
		endpoint:  cfg.Endpoint,
		getConfig: getConfig,

		messages:        make(chan *mq.Message, size),
		messageChanSize: size,

		adjustmentDone: make(chan struct{}),
		chanEmpty:      make(chan struct{}),
		done:           make(chan struct{}),
	}, nil
}

func (d *dispatcher) run(ctx context.Context, log *logrus.Entry) error {
	s, err := kafka.Subscribe(d.topic, d.handle)
	if err != nil {
		return err
	}

	go d.dispatch(log)

	<-ctx.Done()

	s.Unsubscribe()

	msg := mq.Message{
		Header: map[string]string{
			"adjust_concurrent_size": "exit",
		},
	}
	d.messages <- &msg

	<-d.done

	return nil
}

func (d *dispatcher) handle(event mq.Event) error {
	d.adjust()

	msg := event.Message()

	if err := d.validateMessage(msg); err != nil {
		return err
	}

	d.messages <- msg

	return nil
}

func (d *dispatcher) adjust() {
	cfg, err := d.getConfig()
	if err != nil {
		return
	}

	size := cfg.Config.ConcurrentSize
	if size == d.messageChanSize {
		return
	}

	msg := mq.Message{
		Header: map[string]string{
			"adjust_concurrent_size": "adjust_concurrent_size",
		},
	}
	d.messages <- &msg

	<-d.chanEmpty

	d.messages = make(chan *mq.Message, size)
	d.messageChanSize = size

	d.adjustmentDone <- struct{}{}

	return
}

func (d *dispatcher) validateMessage(msg *mq.Message) error {
	if msg == nil {
		return errors.New("get a nil msg from broker")
	}

	if len(msg.Header) == 0 || msg.Header["User-Agent"] != "Robot-Gitee-Access" {
		return errors.New("unexpect gitee message: Missing User-Agent Header")
	}

	if len(msg.Body) == 0 {
		return errors.New("unexpect gitee message: The payload is empty")
	}

	return nil
}

func (d *dispatcher) dispatch(log *logrus.Entry) {
	send := func(msg *mq.Message) error {
		req, err := http.NewRequest(
			http.MethodPost, d.endpoint, bytes.NewBuffer(msg.Body),
		)
		if err != nil {
			return err
		}

		h := http.Header{}
		for k, v := range msg.Header {
			h.Add(k, v)
		}
		req.Header = h

		return d.hc.ForwardTo(req, nil)
	}

	for {
		select {
		case msg := <-d.messages:
			if msg.Header[""] == "" {
				d.chanEmpty <- struct{}{}

				<-d.adjustmentDone

			} else if msg.Header[""] == "exit" {
				close(d.done)

				return

			} else {
				if err := send(msg); err != nil {
					log.Errorf("send message, err:%s", err.Error())
				}
			}
		}
	}
}
