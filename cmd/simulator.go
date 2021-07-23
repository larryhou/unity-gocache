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
	secret  string
	count   int
	close   float64
	down    float64
	addr    string
	port    int
	cmdPort int

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
	u      *client.Unity
}

func (c *Context) Close() error {
	close(c.entpsh)
	close(c.work)
	if c.u != nil {return c.u.Close()}
	return nil
}

var logger *zap.Logger

func main() {
	flag.IntVar(&environ.count, "count", 10, "initial client count")
	flag.Float64Var(&environ.close, "close", 0.15, "close ratio[0,1] after upload/download")
	flag.StringVar(&environ.secret, "secret", "larryhou", "command secret")
	flag.Float64Var(&environ.down, "down", 0.90, "download operation ratio[0,1]")
	flag.StringVar(&environ.addr, "addr", "127.0.0.1", "server address")
	flag.IntVar(&environ.port, "port", 9966, "server port")
	flag.IntVar(&environ.cmdPort, "cmd-port", 19966, "local command server port")
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
				} else if float64(num%100)/100 > environ.close {ctx.work <- num} else {
					ctx.Close()
					environ.closed <- struct{}{} /* notify close event */
				}
			case <-environ.closed: go addClients(1)
			case ent := <-environ.entity: environ.library = append(environ.library, ent)
			case ctx := <-environ.entreq:
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

	addClients(environ.count)
	server, err := net.Listen("tcp", fmt.Sprintf(":%d", environ.cmdPort))
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
		logger.Info("command", zap.String("name", cmd), zap.String("addr", c.RemoteAddr().String()))
		switch cmd {
		case "add":
			if num, err := readInt(c); err != nil {return} else {go addClients(num)}
		case "cut":
			if num, err := readInt(c); err != nil {return} else {go cutClients(num)}
		case "clo":
			if num, err := readInt(c); err != nil {return} else {
				if num > 100 { num = 100 } else if num < 0 { num = 0 }
				environ.close = float64(num) / 100
			}
		case "dow":
			if num, err := readInt(c); err != nil {return} else {
				if num > 100 { num = 100 } else if num < 0 { num = 0 }
				environ.down = float64(num) / 100
			}
		default: return
		}
	}
}

func addClients(num int) {
	for i := 0; i < num; i++ {
		u := &client.Unity{Addr: environ.addr, Port: environ.port}
		if err := u.Connect(); err != nil {
			u.Close()
			environ.closed <- struct{}{}
			logger.Error("connect err", zap.Error(err))
			continue
		}

		go runClient(u)
	}
}

func runClient(u *client.Unity) {
	defer u.Close()
	ctx := &Context{u: u, work: make(chan int), entpsh: make(chan *client.Entity)}
	environ.idle <- ctx
	for {
		select {
		case num := <-ctx.work:
			if num == 0 {return}
			if float64(num%100)/100 > environ.down {
				if ent, err := u.Upload(); err == nil {
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
			if err := u.Download(ent); err != nil {
				logger.Error("download err: %v",
					zap.String("guid", hex.EncodeToString(ent.Guid)),
					zap.String("hash", hex.EncodeToString(ent.Hash)), zap.Error(err))
			} else {
				environ.idle <- ctx
			}
		}
	}
}

func cutClients(num int) {
	if num > 0 { environ.cutouts = num }
}
