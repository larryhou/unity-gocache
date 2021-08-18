package client

import (
    "bytes"
    "fmt"
    "github.com/google/gopacket"
    "github.com/google/gopacket/layers"
    "github.com/google/gopacket/pcap"
    "os"
    "strconv"
    "sync"
    "time"
)

func Monitor(device,addr string, port int, index *int) error {
    handle, err := pcap.OpenLive(device, 2048, true, pcap.BlockForever)
    if err != nil { return fmt.Errorf("pcap open err: %v", err) }
    defer handle.Close()

    filter := fmt.Sprintf("tcp and host %s and port %d", addr, port)
    if err := handle.SetBPFFilter(filter); err != nil {return fmt.Errorf("pcap filter err: %v", err)}
    source := gopacket.NewPacketSource(handle, handle.LinkType())
    source.NoCopy = true

    file, err := os.OpenFile(fmt.Sprintf("%s_%d.csv", addr, port), os.O_CREATE | os.O_WRONLY, 0766)
    if err != nil {panic(fmt.Sprintf("open file err: %v", err))}
    defer file.Close()

    base := time.Now()
    sep := byte(',')

    incoming := 0
    outgoing := 0
    var mutex sync.Mutex

    go func() {
        for {
            fmt.Printf("\r%7d INCOMING:%6.2fM/s  OUTGOING:%6.2fM/s", *index, float64(incoming)/(1<<20), float64(outgoing)/(1<<20))
            mutex.Lock()
            incoming = 0
            outgoing = 0
            mutex.Unlock()
            time.Sleep(time.Second)
        }
    }()

    buf := new(bytes.Buffer)
    for packet := range source.Packets() {
        tcp, ok := packet.Layer(layers.LayerTypeTCP).(*layers.TCP)
        if !ok {continue}
        elapse := packet.Metadata().Timestamp.Sub(base).Microseconds()
        if size := len(tcp.Payload); size > 0 {
            buf.Reset()
            buf.WriteString(strconv.FormatInt(elapse, 10))
            buf.WriteByte(sep)
            buf.WriteString(strconv.Itoa(int(tcp.SrcPort)))
            buf.WriteByte(sep)
            buf.WriteString(strconv.Itoa(int(tcp.DstPort)))
            buf.WriteByte(sep)
            buf.WriteString(strconv.Itoa(size))
            buf.WriteByte(sep)
            if tcp.PSH {buf.WriteByte('P')}
            if tcp.ACK {buf.WriteByte('A')}
            if tcp.ECE {buf.WriteByte('E')}
            if tcp.CWR {buf.WriteByte('C')}
            if tcp.NS {buf.WriteByte('N')}
            buf.WriteByte('\n')
            mutex.Lock()
            if _, err := file.Write(buf.Bytes()); err != nil {panic(fmt.Sprintf("write err: %v", err))}
            if tcp.SrcPort == layers.TCPPort(port) { incoming += size } else { outgoing += size }
            mutex.Unlock()
        }
    }
    return nil
}