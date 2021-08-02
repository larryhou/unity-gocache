package main

import (
    "crypto/rand"
    "encoding/binary"
    "flag"
    "fmt"
    "github.com/larryhou/unity-gocache/server"
    rand2 "math/rand"
    "net"
    "strconv"
    "time"
)

var vars struct {
    port int
    addr string
    bufs int
    rand *rand2.Rand
}

func main() {
    flag.StringVar(&vars.addr, "addr", "127.0.0.1", "empty value implicit it's server")
    flag.IntVar(&vars.port, "port", 12345, "server port")
    flag.IntVar(&vars.bufs, "bufs", 1024, "send and receive buffer size")
    flag.Parse()

    {
        s := rand2.NewSource(time.Now().UnixNano())
        vars.rand = rand2.New(s)
    }

    if len(vars.addr) == 0 {
        if listener, err := net.Listen("tcp", ":" + strconv.Itoa(vars.port)); err != nil {panic(err)} else {
            for { if c, err :=listener.Accept(); err == nil { go response(c) } }
        }
    } else {
        if c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", vars.addr, vars.port)); err != nil {panic(err)} else {
            download(c)
        }
    }
}

func response(c net.Conn) {
    buf := make([]byte, vars.bufs)
    s := &server.Stream{Rwp: c}
    defer s.Close()

    if err := s.Read(buf, 8); err != nil {panic(err)}
    size := int64(binary.BigEndian.Uint64(buf))

    n := int64(0)
    for n < size {
        rand.Read(buf)
        num := int64(len(buf))
        if size - n < num { n = size - n }
        if err := s.Write(buf, int(num)); err != nil {return}
        n += num
    }
}

func download(c net.Conn) {
    buf := make([]byte, vars.bufs)
    s := &server.Stream{Rwp: c}
    defer s.Close()

    size := int64((vars.rand.Intn(10) + 1) << 20)
    binary.BigEndian.PutUint64(buf, uint64(size))
    if err := s.Write(buf, 8); err != nil {panic(err)}

    n := int64(0)
    for n < size {
        num := int64(len(buf))
        if size - n < num { num = size - n }
        if err := s.Read(buf, int(num)); err != nil {panic(err)}
        n += num
    }
}