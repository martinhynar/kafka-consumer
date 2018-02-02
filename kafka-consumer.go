package main

import (
	"fmt"
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
		Brokers          []string               `yaml:"brokers"`
		Topic            string                 `yaml:"topic"`
		Group            string                 `yaml:"group"`
		StartTimestampMs int64                  `yaml:"start-timestamp-ms"`
		PollMs           int                    `yaml:"poll-ms"`
		Parameters       map[string]interface{} `yaml:"parameters"`
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
		"bootstrap.servers":               strings.Join(config.Kafka.Brokers, ","),
		"group.id":                        config.Kafka.Group,
		"session.timeout.ms":              config.Kafka.Parameters["session.timeout.ms"],
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "latest"},
		"go.application.rebalance.enable": true,
		"go.events.channel.enable":        true})

	if err != nil {
		errlogger.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	defer func() {
		logger.Printf("Closing Kafka consumer %s \n", kafkaConsumer)
		kafkaConsumer.Close()
	}()

	err = kafkaConsumer.Subscribe(config.Kafka.Topic, nil)

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
			case kafka.AssignedPartitions:
				logger.Println("Partitions assigned")
				for _, ap := range e.Partitions {
					logger.Printf("%s[%d]@%v", ap.Topic, ap.Partition, ap.Offset)
				}
				kafkaConsumer.Assign(e.Partitions)

				var start kafka.Offset = kafka.Offset(config.Kafka.StartTimestampMs)
				var searchTP []kafka.TopicPartition = make([]kafka.TopicPartition, len(e.Partitions))
				for i, ap := range e.Partitions {
					searchTP[i] = kafka.TopicPartition{Topic: &config.Kafka.Topic, Partition: ap.Partition, Offset: start}
				}
				timeoutMs := 5000
				rewindTP, err := kafkaConsumer.OffsetsForTimes(searchTP, timeoutMs)
				if err != nil {
					errlogger.Printf("Timestamp offset search error: %v\n", err)
				} else {
					err = kafkaConsumer.Assign(rewindTP)
					logger.Println("Partition re-assignment")
					for _, ap := range rewindTP {
						logger.Printf("%s[%d]@%v", ap.Topic, ap.Partition, ap.Offset)
					}
					if err != nil {
						errlogger.Printf("Partition assignment error: %v\n", err)
					}
				}
			case kafka.RevokedPartitions:
				logger.Printf("%% %v\n", e)
				kafkaConsumer.Unassign()

			case *kafka.Message:
				logger.Printf("kafka@%d : %s", milliseconds(&e.Timestamp), string(e.Value))
			case kafka.PartitionEOF:
				logger.Printf("%% Reached %v\n", e)
			case kafka.Error:
				errlogger.Printf("%% Error: %v\n", e)
				run = false
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}

func milliseconds(moment *time.Time) int64 {
	return moment.UnixNano() / int64(time.Millisecond)
}
