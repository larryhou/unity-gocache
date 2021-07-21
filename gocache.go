package main

import (
    "flag"
    "github.com/larryhou/unity-gocache/config"
    "github.com/larryhou/unity-gocache/server"
)

func main() {
    flag.IntVar(&config.Port,"port", 9966, "server port")
    flag.StringVar(&config.Path, "path", "cache", "cache storage path")
    flag.Parse()

    svr := server.CacheServer{}
    if err := svr.Listen(config.Port); err != nil { panic(err) }
}
