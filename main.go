package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")
	bindAddress := os.Getenv("BIND_ADDRESS")

	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	version, err := sarama.ParseKafkaVersion("3.7.1")
	brokerList := strings.Split(brokers, ",")

	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	defer producer.Close()

	opsProcessed := promauto.NewCounter(prometheus.CounterOpts{
		Name: "myapp_processed_ops_total",
		Help: "The total number of processed events",
	})

	helloHandler := func(w http.ResponseWriter, req *http.Request) {
		io.WriteString(w, "Hello, world!\n")

		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder("Key-A"),
			Value: sarama.StringEncoder("Hello World!"),
		})

		if err != nil {
			// fmt.Fprintf(w, "Failed to store your data: %s", err)
			log.Printf("Failed to store your data: %s", err)
		} else {
			// fmt.Fprintf(w, "Your data is stored with unique identifier important/%d/%d", partition, offset)
			log.Printf("Your data is stored with unique identifier important/%d/%d", partition, offset)
		}

		opsProcessed.Inc()
	}

	http.HandleFunc("/hello", helloHandler)
	http.Handle("/metrics", promhttp.Handler())

	log.Printf("Listing for requests at http://%s/hello", bindAddress)
	log.Printf("Serving metrics at http://%s/metrics", bindAddress)
	log.Fatal(http.ListenAndServe(bindAddress, nil))
}
