package main

import (
	"flag"
	"github.com/larryhou/unity-gocache/client"
)

func main() {
	c := &client.Session{}
	flag.StringVar(&c.Addr, "addr", "127.0.0.1", "server address")
	flag.IntVar(&c.Port, "port", 9966, "server port")
	flag.Parse()

	if err := c.Connect(); err != nil { panic(err) }
	c.Upload()
}
