package main

import (
    "encoding/hex"
    "flag"
    "fmt"
    "github.com/google/gopacket"
    "github.com/google/gopacket/layers"
    "github.com/google/gopacket/pcap"
    "github.com/larryhou/unity-gocache/client"
    "github.com/larryhou/unity-gocache/server"
    "os"
    "path"
    "strconv"
    "sync"
    "time"
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
    go monitorx(context)
    group.Wait()
}

func monitorx(context *CrawlXContext) {
    handle, err := pcap.OpenLive(context.device, 64<<10, true, pcap.BlockForever)
    if err != nil { panic(fmt.Sprintf("pcap open err: %v", err)) }

    filter := fmt.Sprintf("tcp and host %s and port %d", context.addr, context.port)
    if err := handle.SetBPFFilter(filter); err != nil {panic(fmt.Sprintf("pcap filter err: %v", err))}
    defer handle.Close()

    source := gopacket.NewPacketSource(handle, handle.LinkType())
    source.NoCopy = true

    file, err := os.OpenFile(fmt.Sprintf("%s_%d.csv", context.addr, context.port), os.O_CREATE | os.O_WRONLY, 0766)
    if err != nil {panic(fmt.Sprintf("open file err: %v", err))}
    defer file.Close()

    sep := ","
    base := time.Now()
    incoming := 0
    outgoing := 0
    var mutex sync.Mutex

    go func() {
        for {
            fmt.Printf("\r%7d INCOMING:%6.2fM/s  OUTGOING:%6.2fM/s", context.index+1, float64(incoming)/(1<<20), float64(outgoing)/(1<<20))
            mutex.Lock()
            incoming = 0
            outgoing = 0
            mutex.Unlock()
            time.Sleep(time.Second)
        }
    }()

    for packet := range source.Packets() {
        if packet.TransportLayer() == nil || packet.TransportLayer().LayerType() != layers.LayerTypeTCP {continue}
        tcp := packet.TransportLayer().(*layers.TCP)

        elapse := time.Now().Sub(base).Nanoseconds()
        size := len(tcp.Payload)
        if size > 0 {
            if _, err := file.WriteString(strconv.FormatInt(elapse, 10)); err != nil {panic(fmt.Sprintf("write err: %v", err))}
            file.WriteString(sep)
            file.WriteString(strconv.Itoa(int(tcp.SrcPort)))
            file.WriteString(sep)
            file.WriteString(strconv.Itoa(int(tcp.DstPort)))
            file.WriteString(sep)
            file.WriteString(strconv.Itoa(size))
            file.WriteString("\n")
            mutex.Lock()
            if tcp.SrcPort == layers.TCPPort(context.port) { incoming += size } else { outgoing += size }
            mutex.Unlock()
        }
    }
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
