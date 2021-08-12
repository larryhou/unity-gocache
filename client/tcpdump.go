package client

import (
    "bytes"
    "fmt"
    "github.com/google/gopacket/layers"
    "github.com/larryhou/tcpdump"
    "os"
    "strconv"
    "sync"
    "time"
)

func Monitor(device,addr string, port int, index *int) {
    td := &tcpdump.TcpDump{
        Device: device,
        Filter: fmt.Sprintf("tcp and host %s and port %d", addr, port),
    }

    file, err := os.OpenFile(fmt.Sprintf("%s_%d.csv", addr, port), os.O_CREATE | os.O_WRONLY, 0766)
    if err != nil {panic(fmt.Sprintf("open file err: %v", err))}
    defer file.Close()

    base := time.Now()
    sep := byte(',')

    incoming := 0
    outgoing := 0
    var mutex sync.Mutex
    td.Handle = func(packet *tcpdump.Packet, buf *bytes.Buffer) {
        elapse := time.Now().Sub(base).Nanoseconds()
        for _, t := range packet.Stack {
            if t != layers.LayerTypeTCP {continue}
            tcp := packet.Tcp
            if size := len(tcp.Payload); size > 0 {
                buf.WriteString(strconv.FormatInt(elapse, 10))
                buf.WriteByte(sep)
                buf.WriteString(strconv.Itoa(int(tcp.SrcPort)))
                buf.WriteByte(sep)
                buf.WriteString(strconv.Itoa(int(tcp.DstPort)))
                buf.WriteByte(sep)
                buf.WriteString(strconv.Itoa(size))
                buf.WriteByte('\n')
                mutex.Lock()
                if _, err := file.Write(buf.Bytes()); err != nil {panic(fmt.Sprintf("write err: %v", err))}
                if tcp.SrcPort == layers.TCPPort(port) { incoming += size } else { outgoing += size }
                mutex.Unlock()
            }
        }
    }

    go func() {
        for {
            fmt.Printf("%7d INCOMING:%6.2fM/s  OUTGOING:%6.2fM/s", *index, float64(incoming)/(1<<20), float64(outgoing)/(1<<20))
            mutex.Lock()
            incoming = 0
            outgoing = 0
            mutex.Unlock()
            time.Sleep(time.Second)
        }
    }()

    if err := td.Start(4); err != nil {panic(err)}
}
