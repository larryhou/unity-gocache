package client

import (
	"fmt"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"sync"
	"time"
)

type TcpDump struct {
	Device string
	Filter string
	Handle func(*Packet)

	h *pcap.Handle
	p *sync.Pool
	c chan *Packet
}

type Packet struct {
	Time     time.Time
	Data     [2<<10]byte
	Size     int
	Stack    []gopacket.LayerType
	Ethernet *layers.Ethernet
	IPv4     *layers.IPv4
	IPv6     *layers.IPv6
	Dns      *layers.DNS
	Tcp      *layers.TCP
	Udp      *layers.UDP
}

func (t *TcpDump) Start(num int) error {
	t.p = &sync.Pool{New: func() interface{} {return new(Packet)}}
	handle, err := pcap.OpenLive(t.Device, 64<<10, true, pcap.BlockForever)
	if err != nil { return fmt.Errorf("pcap open err: %v", err) }
	if err := handle.SetBPFFilter(t.Filter); err != nil {return fmt.Errorf("pcap filter err: %v", err)}
	t.c = make(chan *Packet, num)
	for i := 0; i < num; i++ {
		t.p.Put(t.p.Get())
		go t.parse()
	}

	for {
		data, ci, err := t.h.ZeroCopyReadPacketData()
		if err != nil {continue}
		packet := t.p.Get().(*Packet)
		packet.Time = ci.Timestamp
		packet.Size = len(data)
		copy(packet.Data[:], data)
		t.c <- packet
	}
}

func (t *TcpDump) Close() {
	if t.h != nil {
		t.h.Close()
		t.h = nil
	}
	if t.c != nil {
		close(t.c)
		t.c = nil
	}
}

func (t *TcpDump) parse() {
	var (
		eth  layers.Ethernet
		ipv4 layers.IPv4
		ipv6 layers.IPv6
		dns  layers.DNS
		tcp  layers.TCP
		udp  layers.UDP
	)

	parser := gopacket.NewDecodingLayerParser(layers.LayerTypeEthernet, &eth, &ipv4, &ipv6, &dns, &tcp, &udp)
	parser.IgnoreUnsupported = true

	for packet := range t.c {
		if packet == nil {return}
		packet.Stack = packet.Stack[:0]
		if err := parser.DecodeLayers(packet.Data[:packet.Size], &packet.Stack); err != nil {continue}
		packet.Ethernet = &eth
		packet.IPv4 = &ipv4
		packet.IPv6 = &ipv6
		packet.Dns = &dns
		packet.Tcp = &tcp
		packet.Udp = &udp
		t.Handle(packet)
		t.p.Put(packet)
	}
}

