package main

import (
	"fmt"
	"os"

	"encoding/json"

	"gopkg.in/urfave/cli.v1"
)


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

func calculateReplicas(partition, repfactor int, brokers []int) []int {
	ret := make([]int, repfactor)
	for i := 0; i < repfactor; i++ {
		ret[i] = brokers[(partition + i) % len(brokers)]
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
	if replicationFactor > len(brokers) {
		return fmt.Errorf("replication factor is larger than the number of brokers. bailing")
	}

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
			Replicas:  calculateReplicas(i, replicationFactor, brokers),
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
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
