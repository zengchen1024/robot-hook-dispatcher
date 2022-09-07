package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/opensourceways/community-robot-lib/config"
	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/logrusutil"
	"github.com/opensourceways/community-robot-lib/mq"
	liboptions "github.com/opensourceways/community-robot-lib/options"
	"github.com/opensourceways/community-robot-lib/utils"
	"github.com/sirupsen/logrus"
)

type options struct {
	service           liboptions.ServiceOptions
	kafkamqConfigFile string
}

func (o *options) Validate() error {
	return o.service.Validate()
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	o.service.AddFlags(fs)

	fs.StringVar(
		&o.kafkamqConfigFile, "kafkamq-config-file", "/etc/kafkamq/config.yaml",
		"Path to the file containing config of kafkamq.",
	)

	_ = fs.Parse(args)

	return o
}

const component = "robot-hook-dispatcher"

func main() {
	logrusutil.ComponentInit(component)
	log := logrus.NewEntry(logrus.StandardLogger())

	o := gatherOptions(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:]...)
	if err := o.Validate(); err != nil {
		log.Fatalf("Invalid options, err:%s", err.Error())
	}

	// init kafka
	kafkaCfg, err := loadKafkaConfig(o.kafkamqConfigFile)
	if err != nil {
		log.Fatalf("Error loading kfk config, err:%v", err)
	}

	if err := connetKafka(&kafkaCfg); err != nil {
		log.Fatalf("Error connecting kfk mq, err:%v", err)
	}

	defer kafka.Disconnect()

	// load config
	configAgent := config.NewConfigAgent(func() config.Config {
		return new(configuration)
	})
	if err := configAgent.Start(o.service.ConfigFile); err != nil {
		log.WithError(err).Fatal("Error starting config agent.")
	}

	defer configAgent.Stop()

	d, err := newDispatcher(func() (*configuration, error) {
		_, cfg := configAgent.GetConfig()
		if c, ok := cfg.(*configuration); ok {
			return c, nil
		}

		return nil, errors.New("can't convert to configuration")
	})
	if err != nil {
		log.Fatalf("Error new dispatcherj, err:%s", err.Error())
	}

	// run
	run(d, log)
}

func connetKafka(cfg *mq.MQConfig) error {
	tlsConfig, err := cfg.TLSConfig.TLSConfig()
	if err != nil {
		return err
	}

	err = kafka.Init(
		mq.Addresses(cfg.Addresses...),
		mq.SetTLSConfig(tlsConfig),
		mq.Log(logrus.WithField("module", "kfk")),
	)
	if err != nil {
		return err
	}

	return kafka.Connect()
}

func loadKafkaConfig(file string) (cfg mq.MQConfig, err error) {
	if err = utils.LoadFromYaml(file, &cfg); err != nil {
		return
	}

	if len(cfg.Addresses) == 0 {
		err = errors.New("missing addresses")

		return
	}

	err = kafka.ValidateConnectingAddress(cfg.Addresses)

	return
}

func run(d *dispatcher, log *logrus.Entry) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup
	defer wg.Wait()

	called := false
	ctx, done := context.WithCancel(context.Background())

	defer func() {
		if !called {
			called = true
			done()
		}
	}()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()

		select {
		case <-ctx.Done():
			log.Info("receive done. exit normally")
			return

		case <-sig:
			log.Info("receive exit signal")
			done()
			called = true
			return
		}
	}(ctx)

	if err := d.run(ctx, log); err != nil {
		log.Errorf("subscribe failed, err:%v", err)
	}
}
