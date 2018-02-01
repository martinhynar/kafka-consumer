package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	Kafka struct {
		Brokers    []string               `yaml:"brokers"`
		Topic      string                 `yaml:"topic"`
		Group      string                 `yaml:"group"`
		PollMs     int                    `yaml:"poll-ms"`
		Parameters map[string]interface{} `yaml:"parameters"`
	}
}

var (
	logger    = log.New(os.Stdout, "KC ", log.LstdFlags|log.LUTC)
	errlogger = log.New(os.Stderr, "KC ", log.LstdFlags|log.LUTC)
)

func main() {

	// os.Exit(0)
	configFile := "config.yaml"
	if len(os.Args) == 2 {
		configFile = os.Args[1]
	}

	config := Config{}
	in, err := ioutil.ReadFile(configFile)
	yaml.Unmarshal(in, &config)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Create Kafka Client
	kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    strings.Join(config.Kafka.Brokers, ","),
		"group.id":             config.Kafka.Group,
		"session.timeout.ms":   config.Kafka.Parameters["session.timeout.ms"],
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		errlogger.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	defer func() {
		logger.Printf("Closing Kafka consumer %s \n", kafkaConsumer)
		kafkaConsumer.Close()
	}()

	err = kafkaConsumer.SubscribeTopics([]string{config.Kafka.Topic}, nil)
	timeoutMs := 1000
	var januaryfirst2018 kafka.Offset = 1514761200000
	offsets, err := kafkaConsumer.OffsetsForTimes([]kafka.TopicPartition{{Topic: &config.Kafka.Topic, Partition: 0, Offset: januaryfirst2018}}, timeoutMs)
	if err != nil {
		errlogger.Printf("Timestamp offset search error: %v\n", err)
	} else {
		for _, offset := range offsets {
			logger.Printf("Partition seek: %s[%d]->%v\n", *offset.Topic, offset.Partition, offset.Offset)

			err = kafkaConsumer.Seek(offset, timeoutMs)
			if err != nil {
				errlogger.Printf("Partition seek error: %v\n", err)
			}
		}
	}
	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			logger.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := kafkaConsumer.Poll(config.Kafka.PollMs)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				logger.Printf("RECEIVED: @%d %s\n", milliseconds(&e.Timestamp), string(e.Value))
			case kafka.PartitionEOF:
				logger.Printf("%% Reached %v\n", e)
			case kafka.Error:
				errlogger.Printf("%% Error: %v\n", e)
				run = false
			default:
				// fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}

func milliseconds(moment *time.Time) int64 {
	return moment.UnixNano() / int64(time.Millisecond)
}
