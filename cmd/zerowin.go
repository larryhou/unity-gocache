package main

import (
    "flag"
    "fmt"
    "net"
)

func main() {
    var port int
    var addr string
    flag.StringVar(&addr, "addr", "localhost", "server addr")
    flag.IntVar(&port, "port", 1234, "server port")
    flag.Parse()

    block := make(chan struct{})
    if c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", addr, port)); err != nil {panic(err)} else {
        c.RemoteAddr()
        <- block
    }
}
