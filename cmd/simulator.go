package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/larryhou/unity-gocache/client"
	"go.uber.org/zap"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

var environ struct{
	secret string
	addr string
	port int

	queue   []*Context
	library []*client.Entity
	cutouts int

	idle chan *Context
	closed chan struct{}
	entreq chan *Context
	entity chan *client.Entity
}

type Context struct {
	work   chan int
	entpsh chan *client.Entity
	s      *client.Session
}

func (c *Context) Close() error {
	close(c.entpsh)
	close(c.work)
	if c.s != nil {return c.s.Close()}
	return nil
}

var logger *zap.Logger

func main() {
	flag.StringVar(&environ.secret, "secret", "larryhou", "command secret")
	flag.StringVar(&environ.addr, "addr", "127.0.0.1", "server address")
	flag.IntVar(&environ.port, "port", 9966, "server port")
	flag.Parse()

	if v, err := zap.NewDevelopment(); err != nil {panic(err)} else {logger = v}

	environ.idle = make(chan *Context)
	environ.closed = make(chan struct{})
	environ.entreq = make(chan *Context)

	go func() {
		for {
			select {
			case ctx := <-environ.idle:
				num := rand.Int()
				if environ.cutouts > 0 {
					environ.cutouts--
					ctx.Close()
				} else if num % 100 > 15 {ctx.work <- num} else {
					ctx.Close()
					environ.closed <- struct{}{} /* notify close event */
				}
			case <- environ.closed: go addClients(1)
			case ent := <-environ.entity: environ.library = append(environ.library, ent)
			case ctx := <- environ.entreq:
				for {
					if len(environ.library) > 0 {
						n := rand.Intn(len(environ.library))
						ctx.entpsh <- environ.library[n]
						break
					}
					time.Sleep(time.Second)
				}
			}
		}
	}()

	addClients(10)
	server, err := net.Listen("tcp", ":19966")
	if err != nil { panic(err) }

	for {
		if c, err := server.Accept(); err == nil { go handle(c) }
	}
}

func readString(c net.Conn) (string, error) {
	var buf bytes.Buffer
	b := make([]byte, 1)
	for {
		if _, err := c.Read(b); err != nil {return "", err}
		if b[0] == 0 {return buf.String(),nil}
		buf.WriteByte(b[0])
	}
}

func readInt(c net.Conn) (int, error) {
	num, err := readString(c)
	if err != nil {
		c.Write([]byte(fmt.Sprintf("read int err: %v", err)))
		return 0, err
	}

	num = strings.TrimSpace(num)
	if v, err := strconv.Atoi(num); err != nil {
		c.Write([]byte(fmt.Sprintf("wrong int value: %s", num)))
		return 0, err
	} else { return v, nil }
}

func handle(c net.Conn) {
	defer c.Close()
	if secret, err := readString(c); err != nil {return} else {
		if environ.secret != secret {
			c.Write([]byte(fmt.Sprintf("secret not match: %s", secret)))
			return
		}
	}

	buf := make([]byte, 64)
	if _, err := c.Read(buf[:3]); err != nil {
		c.Write([]byte(fmt.Sprintf("read command err: %v", err)))
		return
	}

	for {
		cmd := string(buf[:3])
		logger.Info("request", zap.String("cmd", cmd), zap.String("addr", c.RemoteAddr().String()))
		switch cmd {
		case "add":
			if num, err := readInt(c); err != nil {return} else {go addClients(num)}
		case "sub":
			if num, err := readInt(c); err != nil {return} else {go subClients(num)}
		default: return
		}
	}
}

func addClients(num int) {
	for i := 0; i < num; i++ {
		s := &client.Session{Addr: environ.addr, Port: environ.port}
		if err := s.Connect(); err != nil {
			s.Close()
			environ.closed <- struct{}{}
			logger.Error("connect err", zap.Error(err))
			continue
		}

		go runClient(s)
	}
}

func runClient(s *client.Session) {
	defer s.Close()
	ctx := &Context{s:s, work: make(chan int), entpsh: make(chan *client.Entity)}
	environ.idle <- ctx
	for {
		select {
		case num := <- ctx.work:
			if num == 0 {return}
			if num % 10 == 0 {
				if ent, err := s.Upload(); err == nil {
					environ.entity <- ent
					environ.idle <- ctx
				} else {
					logger.Error("upload err: %v", zap.Error(err))
				}
			} else {
				environ.entreq <- ctx /* send download request */
			}
		case ent := <-ctx.entpsh:
			if ent == nil {return}
			if err := s.Download(ent); err != nil {
				logger.Error("download err: %v",
					zap.String("guid", hex.EncodeToString(ent.Guid)),
					zap.String("hash", hex.EncodeToString(ent.Hash)), zap.Error(err))
			} else {
				environ.idle <- ctx
			}
		}
	}
}

func subClients(num int) {
	if num > 0 { environ.cutouts = num }
}
