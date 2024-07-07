package main

import (
	"encoding/json"
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
		if req.Method != http.MethodPost {
			http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
			return
		}

		if req.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
			return
		}

		body, err := io.ReadAll(req.Body)
		log.Printf("Received a POST request with body: %s", string(body))

		defer req.Body.Close()

		var data map[string]interface{}

		err = json.Unmarshal(body, &data)
		if err != nil {
			http.Error(w, "Failed to parse JSON body", http.StatusBadRequest)
			log.Printf("Failed to parse JSON body: %s", err)
			return
		}

		remoteAddr := req.RemoteAddr
		log.Printf("Remote address: %s", remoteAddr)

		data["remoteAddr"] = remoteAddr

		payload, err := json.Marshal(data)
		if err != nil {
			log.Printf("Failed to serialize your data: %s", err)
		}

		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(string(payload)),
		})

		if err != nil {
			log.Printf("Failed to store your data: %s", err)
		} else {
			log.Printf("Your data is stored with unique identifier important/%d/%d", partition, offset)
		}

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("Created\n"))

		opsProcessed.Inc()
	}

	http.HandleFunc("/track", helloHandler)
	http.Handle("/metrics", promhttp.Handler())

	log.Printf("Listing for track requests at http://%s/track", bindAddress)
	log.Printf("Serving metric requests at http://%s/metrics", bindAddress)
	log.Fatal(http.ListenAndServe(bindAddress, nil))
}
