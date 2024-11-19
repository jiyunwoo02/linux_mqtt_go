package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func publishMessages(client mqtt.Client, topic string, message string, n int, qos int, socketConn net.Conn) {
	// 반복 발행 횟수 -> topic/count 주제로 발행
	countTopic := topic + "/count"
	countMessage := fmt.Sprintf("%d", n)
	token := client.Publish(countTopic, byte(qos), false, countMessage)
	token.Wait()

	// 메시지 -> MQTT 브로커에 발행
	for i := 1; i <= n; i++ {
		// Sprintf: 형식화된 결과를 문자열로 반환 <-> Printf: 반환값이 없으며, 바로 콘솔에 출력
		msg := fmt.Sprintf("%s#%d", message, i) // Hello#1 형식 <- 메시지에 발행 순서 포함
		token := client.Publish(topic, byte(qos), false, msg)
		token.Wait()
		fmt.Printf("- Published: %s\n", msg)
	}

	// 마지막에 발행한 메시지 -> topic/last 주제로 발행
	lastTopic := topic + "/last"
	lastMessage := fmt.Sprintf("%s#%d", message, n)
	client.Publish(lastTopic, byte(qos), false, lastMessage)
	token.Wait()

	// 메시지 -> 소켓을 통해 전송 (옵션)
	if socketConn != nil {
		socketMessage := fmt.Sprintf("%s, n=%d\n", message, n) // Hello, n=3 형식
		_, err := socketConn.Write([]byte(socketMessage))  
		if err != nil {
			log.Printf("-- Error sending message via socket: %v", err)
		}
		fmt.Println("-- Socket message sent")
	}
	fmt.Println("All messages published.")
}

func main() {
	id := flag.String("id", "publisher1", "The id of the publisher")
	topic := flag.String("tpc", "test/topic", "MQTT topic")
	address := flag.String("add", "tcp://localhost:1883", "Address of the broker")
	qos := flag.Int("q", 2, "QoS level (0, 1, 2)")                            // qos 플래그 추가 (0, 1, 2)
	n := flag.Int("n", 1, "Number of messages to publish")                    // n 플래그 추가 (n : 발행하는 메시지의 반복 발행 횟수)
	port := flag.String("p", "", "Port to listen for subscriber connections") // p 플래그 추가 (p : 리슨 포트, TCP 소켓 서버 역할)
	flag.Parse()

	// QoS가 0, 1, 2가 아닌 값이 제공된다면?
	if *qos < 0 || *qos > 2 {
		// Fatalf is equivalent to [Printf] followed by a call to os.Exit(1).
		log.Fatalf("-- Invalid QoS value: %d. Allowed values are 0, 1, or 2.", *qos)
	}

	opts := mqtt.NewClientOptions().AddBroker(*address).SetClientID(*id)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("-- Failed to connect to broker: %v", token.Error())
	}
	defer client.Disconnect(250)

	var conn net.Conn

	// 1. 포트 제공시 -> 소켓 서버 시작 및 구독자 연결 대기
	if *port != "" {
		// 1) 서버 소켓 생성: 지정된 포트에서 클라이언트 요청 대기
		// net.Listen(protocol, address) : tcp protocol, IP주소(localhost):포트(-p)
		// listener 객체 : 클라이언트의 연결을 대기, 서버 소켓 역할 수행, Accept() 호출해 클라이언트의 연결 요청 수락
		listener, err := net.Listen("tcp", "localhost:"+*port) // The Listen function creates servers:
		if err != nil {
			log.Fatalf("-- Failed to start socket server on port %s: %v", *port, err)
		}
		defer listener.Close()

		timeoutDuration := 10 * time.Second
		// 10초 이내에 연결이 수립되지 않으면 자동으로 대기가 종료되도록 함
		listener.(*net.TCPListener).SetDeadline(time.Now().Add(timeoutDuration))
		fmt.Printf("-- Waiting for subscriber connection on port %s...\n", *port)

		// 3) 연결 요청 대기
		// accept()로 연결 수락을 시도하고, 타임아웃이 발생하면 accept()가 에러와 함께 리턴되어 발행자가 소켓을 닫고 MQTT 통신으로 전환
		conn, err = listener.Accept() // Accept waits for and returns the next connection to the listener.
		if err != nil {
			// 타임아웃 발생 시 메시지와 함께 MQTT로 전환
			fmt.Println("-- Subscriber connection timeout. Switching to MQTT only.")
			conn = nil       // MQTT 모드로 진행하기 위해 conn을 nil로 설정
			listener.Close() // 소켓 리스너를 닫아 이후 연결 시도 시 거부되도록 설정
			listener = nil   // 리스너를 nil로 설정하여 추가 참조 방지
		} else {
			fmt.Println("-- Subscriber connected on socket.")
		}
	} else { // 2. 포트가 제공되지 않은 경우
		fmt.Println("-- No port provided. Only MQTT publishing will occur.")
	}

	scanner := bufio.NewScanner(os.Stdin) // 표준 입력(키보드)을 줄 단위로 읽기 위한 스캐너 생성
	for {
		fmt.Print("Enter the message to publish (type 'exit' to quit): ")
		scanner.Scan()            // 사용자 입력을 대기하다가 엔터를 누르면 읽기 -> 읽고 내부 버퍼에 저장
		message := scanner.Text() // 읽은 입력을 문자열로 반환하여 변수에 저장
		if strings.ToLower(message) == "exit" {
			exitTopic := *topic + "/exit"
			client.Publish(exitTopic, 2, false, "exit")
			fmt.Println("-- Exiting publisher.")
			break
		}
		publishMessages(client, *topic, strings.TrimSpace(message), *n, *qos, conn)
	}
}
