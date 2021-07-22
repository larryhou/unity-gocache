package main

import (
    "flag"
    "github.com/larryhou/unity-gocache/server"
)

func main() {
    s := server.CacheServer{}
    flag.IntVar(&s.Port,"port", 9966, "server port")
    flag.StringVar(&s.Path, "path", "cache", "cache storage path")
    flag.Parse()

    if err := s.Listen(); err != nil { panic(err) }
}
