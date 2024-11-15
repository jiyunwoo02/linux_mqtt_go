package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

func startBroker(port string, brokerID string) {
	server := mqtt.New(nil)

	_ = server.AddHook(new(auth.AllowHook), nil)

	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	tcp := listeners.NewTCP(listeners.Config{
		ID:      brokerID,
		Address: port,
	})

	err := server.AddListener(tcp)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err := server.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		server.Close()
		done <- true
	}()

	<-done
}

func main() {
	port := flag.String("port", ":1883", "The port on which the broker should run")
	brokerID := flag.String("id", "broker1", "The ID of the broker")
	flag.Parse()
	startBroker(*port, *brokerID)
}
