package gelf

import (
	"fmt"
	"net"
)

type Config struct {
	Net string `json:"net"`
	Addr string `json:"addr"`
}

var gSendChannel chan []byte

func Start(config Config) error {
	if len(config.Addr) == 0 {
		fmt.Printf("No address for GELF logging, disabled\n")
		return nil
	}

	raddr, err := net.ResolveUDPAddr(config.Net, config.Addr)
	if err != nil {
		return err
	}

	conn, err := net.DialUDP(config.Net, nil, raddr)
	if err != nil {
		return err
	}

	gSendChannel = make(chan []byte, 16)
	go worker(gSendChannel, conn)

	fmt.Printf("GELF logger start, net = %s, addr = %s\n", config.Net, config.Addr)

	return nil
}

func Send(packet []byte) {
	if gSendChannel != nil {
		select {
			case gSendChannel <- packet:
		}
	}
}

func worker(c chan []byte, conn *net.UDPConn) {
	for {
		packet := <- c
		conn.Write(packet)
	}
}