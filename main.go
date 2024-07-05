package main

import (
	"context"
	"io"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

func main() {
	kw := &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9092"),
		Topic:    "topic-A",
		Balancer: &kafka.LeastBytes{},
	}

	defer kw.Close()

	opsProcessed := promauto.NewCounter(prometheus.CounterOpts{
		Name: "myapp_processed_ops_total",
		Help: "The total number of processed events",
	})

	helloHandler := func(w http.ResponseWriter, req *http.Request) {
		io.WriteString(w, "Hello, world!\n")

		err := kw.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte("Key-A"),
				Value: []byte("Hello World!"),
			},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		opsProcessed.Inc()
	}

	http.HandleFunc("/hello", helloHandler)
	http.Handle("/metrics", promhttp.Handler())

	log.Println("Listing for requests at http://localhost:8000/hello")
	log.Println("Serving metrics at http://localhost:8000/metrics")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
