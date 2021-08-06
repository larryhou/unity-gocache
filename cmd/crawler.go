package main

import (
    "encoding/hex"
    "flag"
    "github.com/larryhou/unity-gocache/client"
    "github.com/larryhou/unity-gocache/server"
    "log"
    "os"
    "path"
    "sync"
)

type CrawlContext struct{
    sync.Mutex
    index int
    entities [][]byte
    output string
    addr string
    port int
}

func main() {
    context := &CrawlContext{}

    parallel := 0
    var source string
    flag.StringVar(&context.addr, "addr", "127.0.0.1", "server address")
    flag.IntVar(&context.port, "port", 9966, "server port")
    flag.StringVar(&source, "source", "", "guidhash source file")
    flag.StringVar(&context.output, "output", "cache", "download output path")
    flag.IntVar(&parallel, "parallel", 4, "parallel downloads")
    flag.Parse()

    var entities [][]byte
    if file, err := os.Open(source); err == nil {
        stream := &server.Stream{Rwp: file}
        for {
            uuid := make([]byte, 32)
            if err := stream.Read(uuid, cap(uuid)); err != nil {break}
            entities = append(entities, uuid)
        }
        file.Close()
    }

    if _, err := os.Stat(context.output); err != nil && os.IsNotExist(err) {
        if err := os.MkdirAll(context.output, 0700); err != nil {panic(err)}
    }

    context.entities = entities

    var group sync.WaitGroup
    for i := 0; i < parallel; i++ {
        group.Add(1)
        go crawl(context, &group)
    }

    group.Wait()
}

func crawl(context *CrawlContext, group *sync.WaitGroup) {
    c := client.Unity{Addr: context.addr, Port: context.port}
    if err := c.Connect(); err != nil {panic(err)}
    defer func() {
        c.Close()
        group.Done()
    }()

    for {
        var uuid []byte
        context.Lock()
        if context.index >= len(context.entities) {return}
        uuid = context.entities[context.index]
        context.index++
        context.Unlock()

        name := hex.EncodeToString(uuid[:16]) + "-" + hex.EncodeToString(uuid[16:])
        dir := path.Join(context.output, name[:2])
        if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
        if file, err := os.OpenFile(path.Join(dir, name + ".bin"), os.O_CREATE | os.O_WRONLY, 0700); err != nil {panic(err)} else {
            if err := c.Get(uuid, server.RequestTypeBin, file); err != nil {panic(err)}
            log.Println(file.Name())
            file.Close()
        }

        if file, err := os.OpenFile(path.Join(dir, name + ".info"), os.O_CREATE | os.O_WRONLY, 0700); err != nil {panic(err)} else {
            if err := c.Get(uuid, server.RequestTypeInf, file); err != nil {panic(err)}
            log.Println(file.Name())
            file.Close()
        }
    }
}
