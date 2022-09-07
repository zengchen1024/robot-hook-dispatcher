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

const (
	headerHookDispatchExit   = "HOOK_DISPATCH_EXIT"
	headerHookDispatchAdjust = "HOOK_DISPATCH_ADJUST"
	headerUserAgent          = "User-Agent"
)

type dispatcher struct {
	hc        utils.HttpClient
	topic     string
	endpoint  string
	userAgent string
	getConfig func() (*configuration, error)

	messageChan      chan *mq.Message
	messageChanEmpty chan struct{}
	messageChanSize  int

	adjustmentDone chan struct{}
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
		endpoint:  cfg.AccessEndpoint,
		userAgent: cfg.UserAgent,
		getConfig: getConfig,

		messageChan:     make(chan *mq.Message, size),
		messageChanSize: size,

		messageChanEmpty: make(chan struct{}),
		adjustmentDone:   make(chan struct{}),
		done:             make(chan struct{}),
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
			headerHookDispatchExit: "exit",
		},
	}
	d.messageChan <- &msg

	<-d.done

	return nil
}

func (d *dispatcher) handle(event mq.Event) error {
	d.adjustConcurrentSize()

	msg := event.Message()

	if err := d.validateMessage(msg); err != nil {
		return err
	}

	d.messageChan <- msg

	return nil
}

func (d *dispatcher) adjustConcurrentSize() {
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
			headerHookDispatchAdjust: "adjust_concurrent_size",
		},
	}
	d.messageChan <- &msg

	<-d.messageChanEmpty

	d.messageChan = make(chan *mq.Message, size)
	d.messageChanSize = size

	d.adjustmentDone <- struct{}{}

	return
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
		case msg := <-d.messageChan:
			if msg.Header[headerHookDispatchAdjust] != "" {
				d.messageChanEmpty <- struct{}{}

				// Must wait. Otherwise it will listen on the old chan.
				<-d.adjustmentDone

			} else if msg.Header[headerHookDispatchExit] != "" {
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
