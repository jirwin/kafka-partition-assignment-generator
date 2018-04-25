package main

import (
	"fmt"
	"os"

	"math/rand"
	"time"

	"encoding/json"

	"gopkg.in/urfave/cli.v1"
)

var randSource = rand.NewSource(time.Now().UnixNano())
var r1 = rand.New(randSource)

type KafkaPartition struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	Replicas  []int    `json:"replicas"`
	LogDirs   []string `json:"log_dirs"`
}

type KafkaPartitions struct {
	Version    int              `json:"version"`
	Partitions []KafkaPartition `json:"partitions"`
}

func containsBroker(list []int, broker int) bool {
	for _, l := range list {
		if l == broker {
			return true
		}
	}

	return false
}

func calculateReplicas(repfactor int, brokers []int) []int {
	ret := []int{}
	for len(ret) < repfactor {
		idx := r1.Intn(len(brokers))
		if !containsBroker(ret, brokers[idx]) {
			ret = append(ret, brokers[idx])
		}
	}

	return ret
}

func run(c *cli.Context) error {
	if !c.IsSet("broker") {
		return fmt.Errorf("a list of brokers is required")
	}
	brokers := c.IntSlice("broker")

	if !c.IsSet("partitions") {
		return fmt.Errorf("the number of partitions to reassign")
	}
	partitionCount := c.Int("partitions")

	replicationFactor := c.Int("rf")

	if !c.IsSet("topic") {
		return fmt.Errorf("the topic to reassign is required")
	}
	topic := c.String("topic")

	kPartitions := &KafkaPartitions{
		Version: 1,
	}

	partitionList := make([]KafkaPartition, partitionCount)

	for i := 0; i < partitionCount; i++ {
		partitionList[i] = KafkaPartition{
			Partition: i,
			Topic:     topic,
			Replicas:  calculateReplicas(replicationFactor, brokers),
			LogDirs:   []string{"any"},
		}
	}

	kPartitions.Partitions = partitionList

	out, err := json.Marshal(kPartitions)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", out)

	return nil
}

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.IntSliceFlag{
			Name:  "broker",
			Usage: "Broker Id",
		},
		cli.IntFlag{
			Name:  "rf",
			Usage: "Replication Factor",
			Value: 3,
		},
		cli.IntFlag{
			Name:  "partitions",
			Usage: "Number of partitions",
		},
		cli.StringFlag{
			Name:  "topic",
			Usage: "The topic to reassign",
		},
	}

	app.Action = run

	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}
