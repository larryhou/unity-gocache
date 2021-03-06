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
    "io"
    "log"
    "os"
    "path"
    "strconv"
    "sync"
    "time"
)

type CrawlContext struct{
    sync.Mutex
    index int
    entities [][]byte
    output string
    device string
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
    flag.StringVar(&context.output, "output", "", "download output path")
    flag.StringVar(&context.device, "device", "en0", "network interface for pcap")
    flag.IntVar(&context.index, "index", 0, "download index")
    flag.IntVar(&parallel, "parallel", 4, "parallel downloads")
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
        if err := os.MkdirAll(context.output, 0700); err != nil {panic(err)}
    }

    context.entities = entities

    var group sync.WaitGroup
    for i := 0; i < parallel; i++ {
        group.Add(1)
        go crawl(context, &group)
    }
    go monitor(context)
    group.Wait()
}

func monitor(context *CrawlContext) {
    handle, err := pcap.OpenLive(context.device, 64<<10, true, pcap.BlockForever)
    if err != nil { panic(fmt.Sprintf("pcap open err: %v", err)) }

    filter := fmt.Sprintf("tcp and host %s and port %d", context.addr, context.port)
    if err := handle.SetBPFFilter(filter); err != nil {panic(fmt.Sprintf("pcap filter err: %v", err))}
    defer handle.Close()

    source := gopacket.NewPacketSource(handle, handle.LinkType())
    source.NoCopy = true

    file, err := os.OpenFile(fmt.Sprintf("%s_%d.csv", context.addr, context.port), os.O_CREATE | os.O_WRONLY, 0700)
    if err != nil {panic(fmt.Sprintf("open file err: %v", err))}
    defer file.Close()

    sep := ", "
    base := time.Now()
    incoming := 0
    outgoing := 0
    var mutex sync.Mutex

    go func() {
        for {
            log.Printf("INCOMING:%6.2fM/s  OUTGOING:%6.2fM/s", float64(incoming)/(1<<20), float64(outgoing)/(1<<20))
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
            file.WriteString(strconv.Itoa(size))
            file.WriteString("\n")
            mutex.Lock()
            if tcp.SrcPort == layers.TCPPort(context.port) { incoming += size } else { outgoing += size }
            mutex.Unlock()
        }
    }
}

func crawl(context *CrawlContext, group *sync.WaitGroup) {
    c := client.Unity{Addr: context.addr, Port: context.port}
    if err := c.Connect(); err != nil {panic(err)}
    defer func() {
        c.Close()
        group.Done()
    }()

    for {
        if context.index >= len(context.entities) {return}
        var uuid []byte
        context.Lock()
        index := 0
        index = context.index
        context.index++
        context.Unlock()
        uuid = context.entities[index]

        name := hex.EncodeToString(uuid[:16]) + "-" + hex.EncodeToString(uuid[16:])
        dir := path.Join(context.output, name[:2])
        if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
        filename := path.Join(dir, name + ".bin")
        if _, err := os.Stat(filename); err == nil || os.IsExist(err) {continue}
        size := client.Counter(0)
        if file, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0700); err != nil {panic(err)} else {
            if err := c.Get(uuid, server.RequestTypeBin, io.MultiWriter(file, &size)); err != nil {panic(err)}
            file.Close()
            if size == 0 { os.Remove(file.Name()) } else {log.Printf("%6d %s %d", index, file.Name(), size)}
        }

        size = 0
        if file, err := os.OpenFile(path.Join(dir, name + ".info"), os.O_CREATE | os.O_WRONLY, 0700); err != nil {panic(err)} else {
            if err := c.Get(uuid, server.RequestTypeInf, io.MultiWriter(file, &size)); err != nil {panic(err)}
            file.Close()
            if size == 0 { os.Remove(file.Name()) } else {log.Printf("%6d %s %d", index, file.Name(), size)}
        }
    }
}
