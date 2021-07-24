package main

import (
    "flag"
    "github.com/larryhou/unity-gocache/server"
    "net/http"
    _ "net/http/pprof"
)

func main() {
    s := server.CacheServer{}
    flag.IntVar(&s.Port,"port", 9966, "server port")
    flag.StringVar(&s.Path, "path", "cache", "cache storage path")
    flag.IntVar(&s.LogLevel, "log-level", 0, "log level debug=-1 info=0 warn=1 error=2 dpanic=3 panic=4 fatal=5")
    flag.UintVar(&s.MemCap, "mem-cap", 0, "memory cache capacity (uint:MB)")
    flag.Parse()

    go http.ListenAndServe(":9999", nil)
    if err := s.Listen(); err != nil { panic(err) }
}
