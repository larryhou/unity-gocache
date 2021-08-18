package main

import (
    "bytes"
    "flag"
    "fmt"
    "github.com/larryhou/unity-gocache/server"
    "net"
    "time"
)

func main() {
    var port int
    flag.IntVar(&port, "port", 1234, "server listen port")
    flag.Parse()

    if listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port)); err != nil {panic(err)} else {
        for {
            if c, err := listener.Accept(); err == nil {
                if t, ok := c.(*net.TCPConn); ok {t.SetNoDelay(true)}
                s := &server.Stream{Rwp: c}
                go func() {
                    defer c.Close()
                    buf := &bytes.Buffer{}
                    num := 0
                    t := time.NewTicker(time.Microsecond * 3)
                    defer t.Stop()
                    for range t.C {
                        buf.Reset()
                        buf.WriteString(fmt.Sprintf("%8d", num))
                        buf.WriteByte(' ')
                        buf.WriteString("nodelay message is written.\n")
                        if err := s.Write(buf.Bytes(), buf.Len()); err != nil {return}
                        num++
                    }
                }()
            }
        }
    }
}
