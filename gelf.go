package gelf

import (
	"fmt"
	"net"
	"errors"
)

type Config struct {
	Enabled bool `json:"enabled"`
	Net string `json:"net"`
	Addr string `json:"addr"`
	Workers int `json:"workers"`
}

var gSendChannel chan []byte

func Start(config Config) error {
	if !config.Enabled {
		fmt.Printf("GELF logging is disabled\n")
		return nil
	}

	if len(config.Net) == 0 {
		return errors.New("Missing network family")
	}

	if len(config.Addr) == 0 {
		return errors.New("Missing address")
	}

	if config.Workers == 0 {
		config.Workers = 4
	} else if config.Workers < 1 || config.Workers > 16 {
		return fmt.Errorf("Bad worker count %d", config.Workers)	
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

	for i := 0; i < config.Workers; i++ {
		go worker(gSendChannel, conn)
	}

	fmt.Printf("GELF logger start, net = %s, addr = %s\n", config.Net, config.Addr)

	return nil
}

func SendBytes(packet []byte) {
	if gSendChannel != nil {
		select {
			case gSendChannel <- packet:
		}
	}
}

func SendString(packet string) {
	SendBytes([]byte(packet))
}

func worker(c chan []byte, conn *net.UDPConn) {
	for {
		packet := <- c
		conn.Write(packet)
	}
}
