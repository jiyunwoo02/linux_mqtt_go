package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// 각 구독자의 결과를 저장하는 구조체
type SubscriberResult struct {
	ID              int    // 구독자 클라이언트 아이디
	Receivedcnt     int    // MQTT를 통해 받은 메시지 수
	Expectedcnt     int    // 기대 메시지 수
	LastReceivedMsg string // 수신한 마지막 메시지 (표 출력)
	ExpectedLastMsg string // 기대한 마지막 메시지 (표 출력)
}

// MQTT 메시지 수신 함수
func subscribeToMQTT(client mqtt.Client, topic string, id int, wg *sync.WaitGroup, results chan<- SubscriberResult, stopChan chan struct{}, once *sync.Once) {
	defer wg.Done()

	receivedCount := 0
	expectedCount := 0

	// 발행 횟수 확인 주제 (topic/count) 구독
	countTopic := topic + "/count"
	client.Subscribe(countTopic, 2, func(_ mqtt.Client, msg mqtt.Message) {
		countPayload := string(msg.Payload())
		if count, err := strconv.Atoi(countPayload); err == nil {
			expectedCount += count
			fmt.Printf("-> Subscriber %d: Set expected count to %d\n", id, expectedCount)
		}
	})

	// 발행자 종료 확인 주제 (topic/exit) 구독
	exitTopic := topic + "/exit"
	client.Subscribe(exitTopic, 2, func(_ mqtt.Client, msg mqtt.Message) {
		if string(msg.Payload()) == "exit" {
			fmt.Printf("-> Subscriber %d received exit message. Shutting down.\n", id)
			// chan struct{}는 데이터 없이 간단히 신호만 전달해야 할 때 사용
			once.Do(func() { close(stopChan) }) // exit 수신 시 채널 닫음 (한 번만)
		}
	})

	// MQTT 주제 구독 및 메시지 수신 핸들러 설정 (QoS=2)
	client.Subscribe(topic, 2, func(_ mqtt.Client, msg mqtt.Message) {
		select {
		case <-stopChan:
			return
		default:
			payload := string(msg.Payload())
			fmt.Printf("[Subscriber %d] Received from MQTT: %s\n", id, payload)
			receivedCount++
		}
	})
	<-stopChan // 종료 신호가 올 때까지 대기
	results <- SubscriberResult{
		ID:          id,
		Receivedcnt: receivedCount,
		Expectedcnt: expectedCount,
	}
}

// 소켓 메시지를 수신하고 출력하는 함수
func receiveFromSocket(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	if conn != nil {
		scanner := bufio.NewScanner(conn)
		if scanner.Scan() {
			message := scanner.Text()
			fmt.Printf(">> Received from TCP Socket: %s\n", message)
		}
	}
}

func main() {
	address := flag.String("add", "tcp://localhost:1883", "Address of the broker")
	id := flag.String("id", "subscriber1", "The id of the subscriber")
	topic := flag.String("tpc", "test/topic", "MQTT topic")
	sn := flag.Int("sn", 1, "Number of subscribers")
	port := flag.String("p", "", "Port to connect for publisher connections")

	flag.Parse()

	var wg sync.WaitGroup
	var conn net.Conn
	var err error

	results := make(chan SubscriberResult, *sn)
	stopChan := make(chan struct{}) // 종료 신호용 채널
	var once sync.Once              // stopChan을 한 번만 닫기 위한 sync.Once

	if *port != "" {
		conn, err = net.Dial("tcp", "localhost:"+*port)
		if err != nil {
			fmt.Println("-- Publisher not using port. Switching to MQTT only.")
		} else {
			fmt.Println("-- Connected to publisher socket.")
			wg.Add(1)
			go receiveFromSocket(conn, &wg)
		}
	} else {
		fmt.Println("-- No port provided on Subscriber. Only MQTT subscription will occur.")
	}

	for i := 1; i <= *sn; i++ {
		wg.Add(1)
		clientID := fmt.Sprintf("%s_%d", *id, i)
		opts := mqtt.NewClientOptions().AddBroker(*address).SetClientID(clientID)
		client := mqtt.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Fatalf("-- Failed to connect to broker: %v", token.Error())
		}
		defer client.Disconnect(250)
		go subscribeToMQTT(client, *topic, i, &wg, results, stopChan, &once)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	totalMessages := 0
	totalReceived := 0
	successfulCount := 0
	unsuccessfulSubscribers := []int{}

	for result := range results {
		totalMessages += result.Expectedcnt
		totalReceived += result.Receivedcnt

		if result.Receivedcnt == result.Expectedcnt {
			successfulCount++
			fmt.Printf("=> Subscriber %d: Successfully received %d/%d messages.\n", result.ID, result.Receivedcnt, result.Expectedcnt)
		} else {
			unsuccessfulSubscribers = append(unsuccessfulSubscribers, result.ID)
		}
	}

	// 요약 결과 출력: 표 출력 -> 정상/비정상 -> 총 퍼센트 순으로 출력
	fmt.Printf("\n모든 구독자(%d명) 중 %d명이 메시지를 정상적으로 수신했습니다.\n\n", *sn, successfulCount)
	fmt.Printf("정상 수신 구독자 수: %d\n", successfulCount)
	fmt.Printf("비정상 수신 구독자 수: %d\n", len(unsuccessfulSubscribers))
	for _, id := range unsuccessfulSubscribers {
		fmt.Printf("- Subscriber%d\n", id)
	}

	fmt.Printf("총 구독자 수: %d\n", *sn)
	if totalMessages > 0 {
		fmt.Printf("성공적으로 수신한 메시지: %d/%d (%.1f%%)\n", totalReceived, totalMessages, (float64(totalReceived)/float64(totalMessages))*100)
	} else {
		fmt.Println("성공적으로 수신한 메시지: 0/0 (0%)")
	}

	fmt.Println(">> All subscribers shutting down.")
}
