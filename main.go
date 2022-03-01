package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchutil"
)

type Results struct {
	Items []Item `json:"items"`
}

type Item struct {
	Timestamp   string         `json:"timestamp"`
	CarparkData []CarparkDatum `json:"carpark_data"`
}

type CarparkDatum struct {
	CarparkInfo    []CarparkInfo `json:"carpark_info"`
	CarparkNumber  string        `json:"carpark_number"`
	UpdateDatetime string        `json:"update_datetime"`
}

type CarparkInfo struct {
	TotalLots     string `json:"total_lots"`
	LotType       string `json:"lot_type"`
	LotsAvailable string `json:"lots_available"`
}

func main() {
	elastic_endpoint := ""
	elastic_username := ""
	elastic_password := ""
	indexName := "carpark-availability-{DATE}"
	topic := "carpark-availability"

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "go_example_group_1",
		"client.id":         "1",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	cfg := opensearch.Config{
		Addresses: []string{
			elastic_endpoint,
		},
		Username: elastic_username,
		Password: elastic_password,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
		},
	}

	client, _ := opensearch.NewClient(cfg)

	index := strings.Replace(indexName, "{DATE}", time.Now().Format("2006-01"), -1)

	bulkCfg := opensearchutil.BulkIndexerConfig{NumWorkers: 1, Client: client,
		Index: index}

	bi, _ := opensearchutil.NewBulkIndexer(bulkCfg)

	// Subscribe to topic
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	totalCount := 0
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			recordKey := string(msg.Key)
			recordValue := msg.Value
			data := CarparkDatum{}
			err = json.Unmarshal(recordValue, &data)
			if err != nil {
				fmt.Printf("Failed to decode JSON at offset %d: %v", msg.TopicPartition.Offset, err)
				continue
			}
			totalCount += 1
			fmt.Printf("Consumed record with key %s and value %s, and updated total count to %d\n", recordKey, recordValue, totalCount)

			err = bi.Add(context.Background(), opensearchutil.BulkIndexerItem{
				Action:     "index",
				DocumentID: fmt.Sprintf("%v-%v", time.Now().Format("2006-01-02 15:04"), data.CarparkNumber),
				Body:       strings.NewReader(string(recordValue)),
			})
			if err != nil {
				log.Fatalf("Unexpected error: %s", err)
			}
			fmt.Println("exported record to elastic")
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
