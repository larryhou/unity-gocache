package main

import (
    "encoding/hex"
    "flag"
    "fmt"
    "github.com/larryhou/unity-gocache/client"
    "github.com/larryhou/unity-gocache/server"
    "os"
    "path"
    "sync"
)

type CrawlXContext struct{
    sync.Mutex
    index int
    entities [][]byte
    output string
    device string
    addr string
    port int
}

func main() {
    context := &CrawlXContext{}

    parallel := 0
    var source string
    flag.StringVar(&context.addr, "addr", "127.0.0.1", "server address")
    flag.IntVar(&context.port, "port", 9966, "server port")
    flag.StringVar(&source, "source", "", "guidhash source file")
    flag.StringVar(&context.output, "output", "", "download output path")
    flag.StringVar(&context.device, "device", "en0", "network interface for pcap")
    flag.IntVar(&context.index, "index", 0, "download index")
    flag.IntVar(&parallel, "parallel", 1, "parallel downloads")
    flag.Parse()

    if len(context.output) == 0 {
        context.output = fmt.Sprintf("%s_%d", context.addr, context.port)
    }

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
        if err := os.MkdirAll(context.output, 0766); err != nil {panic(err)}
    }

    context.entities = entities

    var group sync.WaitGroup
    for i := 0; i < parallel; i++ {
        group.Add(1)
        go crawlx(context, &group)
    }
    go client.Monitor(context.device, context.addr, context.port, &context.index)
    group.Wait()
}

func crawlx(context *CrawlXContext, group *sync.WaitGroup) {
    c := client.Unity{Addr: context.addr, Port: context.port}
    if err := c.Connect(false); err != nil {panic(err)}

    defer func() {
        c.Close()
        group.Done()
    }()

    guard := make(chan struct{}, 20)

    sent := 0
    go func() {
        for {
            if context.index >= len(context.entities) {return}
            var uuid []byte
            context.Lock()
            index := 0
            index = context.index
            context.index++
            context.Unlock()
            uuid = context.entities[index]
            guard <- struct{}{}
            if err := c.GetSend(uuid, server.RequestTypeBin); err != nil {panic(err)}
            sent++
            guard <- struct{}{}
            if err := c.GetSend(uuid, server.RequestTypeInf); err != nil {panic(err)}
            sent++
        }
    }()

    done := 0
    for {
        if sent > 0 && done >= sent {return}
        <-guard
        ctx, err := c.GetScan()
        if err != nil {panic(err)}
        done++
        if !ctx.Found || ctx.Size == 0 {continue}
        name := hex.EncodeToString(ctx.Uuid[:16]) + "-" + hex.EncodeToString(ctx.Uuid[16:])
        dir := path.Join(context.output, name[:2])
        if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) { os.MkdirAll(dir, 0766) }
        filename := path.Join(dir, name + "." + ctx.Type.FileExt())
        if file, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0766); err != nil {panic(err)} else {
            if err := c.GetRecv(ctx.Size, ctx.Type, file); err != nil {
                file.Close()
                os.Remove(file.Name())
                panic(err)
            }
            file.Close()
        }
    }
}
