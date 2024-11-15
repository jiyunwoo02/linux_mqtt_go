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
	// 각 주제에 대한 구독 설정되면, 특정 주제에 메시지 발행될 때 자동으로 구독 핸들러 호출됨
	// 메인 고루틴은 <-stopChan을 통해 채널 닫히기를 대기
	// exit 메시지가 수신되면 stopChan이 닫히고, 모든 핸들러는 return으로 종료

	defer wg.Done()

	receivedCount := 0
	expectedCount := 0
	lastReceivedMsg := ""
	expectedLastMsg := ""

	// 발행 횟수 확인 주제 (topic/count) 구독
	countTopic := topic + "/count"
	client.Subscribe(countTopic, 2, func(_ mqtt.Client, msg mqtt.Message) {
		countPayload := string(msg.Payload())
		if count, err := strconv.Atoi(countPayload); err == nil {
			expectedCount += count
			// fmt.Printf("-> Subscriber %d: Set expected count to %d\n", id, expectedCount)
		}
	})

	// 발행자 종료 확인 주제 (topic/exit) 구독
	exitTopic := topic + "/exit"
	client.Subscribe(exitTopic, 2, func(_ mqtt.Client, msg mqtt.Message) {
		if string(msg.Payload()) == "exit" {
			fmt.Printf("-> Subscriber %d received exit message. Shutting down.\n", id)
			// stopChan은 종료 신호를 전달하기 위한 채널로 사용
			// once.Do는 여러 구독자 중 단 한 명만 close(stopChan)을 실행하도록 보장하여, 불필요한 중복 종료 신호 전송 방지
			once.Do(func() { // func() is niladic: having no arguments.
				// fmt.Printf("채널을 닫는 구독자: subscriber %d\n", id) // 매번 랜덤
				close(stopChan) // exit 수신 시 채널을 닫아 종료 '신호'를 전달 (한 번만)
			})
		}
	})

	// 발행자의 마지막 메시지 확인 주제 (topic/last) 구독
	lastTopic := topic + "/last"
	client.Subscribe(lastTopic, 2, func(_ mqtt.Client, msg mqtt.Message) {
		expectedLastMsg = string(msg.Payload())
	})

	// MQTT 주제 구독 및 메시지 수신 핸들러 설정 (QoS=2)
	client.Subscribe(topic, 2, func(_ mqtt.Client, msg mqtt.Message) {
		select {
		case <-stopChan: // stopChan이 닫히면 채널이 닫힘을 감지하고 종료
			return
		default:
			// stopChan이 닫히지 않은 경우 메시지를 계속 수신
			lastReceivedMsg = string(msg.Payload())
			fmt.Printf("[Subscriber %d] Received from MQTT: %s\n", id, lastReceivedMsg)
			receivedCount++
		}
	})
	// stopChan 채널에서 값을 수신할 때까지 대기
	// -- Go에서는 채널이 닫혔을 때 <-stopChan를 통해 해당 타입의 기본값(zero value)이 반환된다
	<-stopChan
	results <- SubscriberResult{
		ID:              id,
		Receivedcnt:     receivedCount,
		Expectedcnt:     expectedCount,
		LastReceivedMsg: lastReceivedMsg,
		ExpectedLastMsg: expectedLastMsg,
	}
}

// 소켓 메시지를 수신하고 출력하는 함수
func receiveFromSocket(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	if conn != nil {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
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
	var once sync.Once // stopChan을 한 번만 닫기 위한 sync.Once

	results := make(chan SubscriberResult, *sn)
	stopChan := make(chan struct{}) // 종료 신호용 채널

	// 빈 구조체 테스트
	// test1 := struct{}{}
	// test2 := struct{}{}
	// fmt.Printf("빈 구조체1의 메모리 사이즈: %d\n", unsafe.Sizeof(test1)) // 메모리 크기 = 0 : 구조체 필드 없어서 메모리 차지 X
	// fmt.Printf("빈 구조체1의 메모리 주소: %v\n", unsafe.Pointer(&test1)) // 모든 빈 구조체가 동일한 주소 공유하도록 최적화 : 메모리 공간 점유 X

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

	wg.Wait()
	close(results)

	fmt.Println("\n구독자 번호 | 수신한 메시지 개수 | 기대한 메시지 개수 | 수신한 마지막 메시지 | 기대한 마지막 메시지")
	fmt.Println("----------------------------------------------------------------------------------------------")

	totalMessages := 0
	totalReceived := 0
	successfulCount := 0
	unsuccessfulSubscribers := []int{} // 빈 int 슬라이스 초기화

	// results 채널이 닫히기 전까지 구독자 결과를 하나씩 꺼내어 처리
	for result := range results { // result는 results 채널로부터 하나씩 꺼내온 SubscriberResult 구조체 인스턴스
		totalMessages += result.Expectedcnt
		totalReceived += result.Receivedcnt

		if result.Receivedcnt == result.Expectedcnt {
			successfulCount++
		} else {
			unsuccessfulSubscribers = append(unsuccessfulSubscribers, result.ID)
		}

		fmt.Printf("Subscriber %d | %d                 | %d                  | %s                  | %s\n",
			result.ID, result.Receivedcnt, result.Expectedcnt, result.LastReceivedMsg, result.ExpectedLastMsg)
	}

	fmt.Printf("\n모든 구독자(%d명) 중 %d명이 메시지를 정상적으로 수신했습니다.\n\n", *sn, successfulCount)
	fmt.Printf("정상 수신 구독자 수: %d\n", successfulCount)
	fmt.Printf("비정상 수신 구독자 수: %d\n", len(unsuccessfulSubscribers))
	for _, id := range unsuccessfulSubscribers { // index(무시)와 id
		fmt.Printf("- Subscriber%d\n", id)
	}

	fmt.Printf("\n총 구독자 수: %d\n", *sn)
	if totalMessages > 0 {
		fmt.Printf("성공적으로 수신한 메시지: %d/%d (%.1f%%)\n", totalReceived, totalMessages, (float64(totalReceived)/float64(totalMessages))*100)
	} else {
		fmt.Println("성공적으로 수신한 메시지: 0/0 (0%)")
	}

	fmt.Println("\n>> All subscribers shutting down.")
}
