package main

import (
    "context"
    "flag"
    "fmt"
    "github.com/larryhou/unity-gocache/server"
    "io"
    "math/rand"
    "net"
    "syscall"
)

func main() {
    var addr string
    var port int
    flag.StringVar(&addr, "addr", "127.0.0.1", "server addresss")
    flag.IntVar(&port, "port", 4444, "server port")
    flag.Parse()

    setopts := func(fd int) {
        syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, 2048<<10)
        syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, 2048<<10)
        syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
        syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x103 /*TCP_SENDMOREACKS*/, 1)
        syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x00C /*TCP_QUICKACK*/, 1)
    }

    control := func(network, address string, c syscall.RawConn) error {
        return c.Control(func(fd uintptr) { setopts(int(fd)) })
    }

    if len(addr) == 0 {
        lc := &net.ListenConfig{Control: control}
        listener, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf(":%d", port))
        if err != nil {panic(err)}
        for {
            if c, err := listener.Accept(); err != nil {panic(err)} else {
                if tc, ok := c.(*net.TCPConn); ok {
                    tc.SetReadBuffer(2048<<10)
                    tc.SetWriteBuffer(2048<<10)
                }
                go func() {
                    defer c.Close()
                    buf := make([]byte, 64<<10)
                    s := &server.Stream{Rwp: c}
                    for {
                        rand.Read(buf[:128])
                        if err := s.Write(buf, len(buf)); err != nil {return}
                    }
                }()
            }
        }
    } else {
        dr := net.Dialer{Control: control}
        if c ,err := dr.Dial("tcp", fmt.Sprintf("%s:%d", addr, port)); err != nil {panic(err)} else {
            defer c.Close()
            for {
                buf := make([]byte, 1024<<10)
                s := server.Stream{Rwp: c}
                if err := s.Read(buf, len(buf)); err != nil && err != io.EOF {panic(err)}
            }
        }
    }
}