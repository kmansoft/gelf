package gelf

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

/* ----- */

const (
	VERSION_1_1 = "1.1"
)

/* ----- */

type Config struct {
	Enabled      bool   `json:"enabled"`
	Net          string `json:"net"`
	Addr         string `json:"addr"`
	Workers      int    `json:"workers"`
	Echo         bool   `json:"echo"`
	Host         string `json:"host"`
	Compress     bool   `json:"compress"`
	MaxChunkSize int    `json:"max_chunk_size"`
}

/* ----- */

type Event interface {
	ToJson() ([]byte, error)
}

type BaseEvent struct {
	Version      string `json:"version"`
	Host         string `json:"host"`
	ShortMessage string `json:"short_message"`
	FullMessage  string `json:"full_message,omitempty"`
}

func (e *BaseEvent) ToJson() ([]byte, error) {
	return json.Marshal(e)
}

/* ----- */

func NewBaseEvent() BaseEvent {
	return BaseEvent{Version: VERSION_1_1, Host: gHost}
}

func GetVersion() string {
	return VERSION_1_1
}

func GetHost() string {
	return gHost
}

/* ----- */

var gSendChannel chan []byte
var gHost string

func Start(config Config) (err error) {
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

	if config.MaxChunkSize == 0 {
		config.MaxChunkSize = 1400
	} else if config.MaxChunkSize < 100 || config.MaxChunkSize > 8192 {
		return fmt.Errorf("Bad max chunk size %d", config.MaxChunkSize)
	}

	raddr, err := net.ResolveUDPAddr(config.Net, config.Addr)
	if err != nil {
		return
	}

	conn, err := net.DialUDP(config.Net, nil, raddr)
	if err != nil {
		return
	}

	fmt.Printf("GELF logger start, net = %s, addr = %s\n", config.Net, config.Addr)

	if len(config.Host) == 0 {
		config.Host, err = os.Hostname()
		if err != nil {
			return
		}
		fmt.Printf("GELF host = %q\n", config.Host)
	}

	gSendChannel = make(chan []byte, 16)
	gHost = config.Host

	for i := 0; i < config.Workers; i++ {
		w, err := newWorker(gSendChannel, conn, config)
		if err != nil {
			return err
		}
		go w.run()
	}

	err = nil
	return
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

func SendEvent(event Event) {
	b, err := event.ToJson()
	if err != nil {
		reportError(err)
	}

	SendBytes(b)
}

/* ----- */

type worker struct {
	c      chan []byte
	conn   *net.UDPConn
	config Config

	zbuf    bytes.Buffer
	zwriter *zlib.Writer

	rand *rand.Rand
	id   []byte
	cbuf bytes.Buffer
}

func newWorker(c chan []byte, conn *net.UDPConn, config Config) (*worker, error) {
	w := &worker{c: gSendChannel, conn: conn, config: config}
	w.zwriter = zlib.NewWriter(&w.zbuf)
	w.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	w.id = make([]byte, 8, 8)
	return w, nil
}

func (w *worker) run() {
	for {
		packet := <-w.c

		if w.config.Echo {
			fmt.Printf("gelf <- %s\n", string(packet))
		}

		var err error
		var tosend []byte
		if w.config.Compress {
			tosend, err = w.compress(packet)
			if err != nil {
				reportError(err)
				continue
			}
		} else {
			tosend = packet
		}

		total := len(tosend)
		max := w.config.MaxChunkSize

		if total > max {
			// Need to break into chunks
			chunkCount := (total + max - 1) / max

			if chunkCount > 128 {
				reportError(errors.New("packet has too many chunks"))
				continue
			}

			chunkOffset := 0
			w.rand.Read(w.id)

			for chunk := 0; chunk < chunkCount; chunk++ {
				w.cbuf.Reset()
				// magic header
				w.cbuf.WriteByte(0x1e)
				w.cbuf.WriteByte(0x0f)
				// id
				w.cbuf.Write(w.id)
				// chunk number and count
				w.cbuf.WriteByte(byte(chunk))
				w.cbuf.WriteByte(byte(chunkCount))
				// actual data
				chunkLen := total - chunkOffset
				if chunkLen > max {
					chunkLen = max
				}
				sl := tosend[chunkOffset : chunkOffset+chunkLen]
				w.cbuf.Write(sl)
				// send
				w.conn.Write(w.cbuf.Bytes())
				// next!
				chunkOffset = chunkOffset + max
			}
		} else {
			// Chunking is not needed
			w.conn.Write(tosend)
		}
	}
}

func (w *worker) compress(src []byte) ([]byte, error) {
	w.zbuf.Reset()
	w.zwriter.Reset(&w.zbuf)

	n, err := w.zwriter.Write(src)
	if n != len(src) {
		return nil, fmt.Errorf("Could only write %d of %d bytes", n, len(src))
	}
	if err != nil {
		return nil, err
	}

	w.zwriter.Close()
	return w.zbuf.Bytes(), nil
}

func reportError(err error) {
	fmt.Println(err)
}
