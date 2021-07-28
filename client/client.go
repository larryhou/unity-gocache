package client

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/larryhou/unity-gocache/server"
	"hash"
	"io"
	rand2 "math/rand"
	"net"
	"time"
)

type Unity struct {
	Addr   string
	Port   int
	Verify bool
	c      *server.Stream
	b      [32 << 10]byte
}

func (u *Unity) Close() error {
	if u.c != nil {
		return u.c.Close()
	}
	return nil
}

func (u *Unity) Connect() error {
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", u.Addr, u.Port))
	if err != nil {return err}
	u.c = &server.Stream{Rwp: c}
	if err := u.c.Write([]byte{'f', 'e'}, 2); err != nil {return err}
	ver := make([]byte, 8)
	if err := u.c.Read(ver, len(ver)); err != nil {return err}
	if _, err := hex.Decode(ver, ver); err != nil {return err}
	v := binary.BigEndian.Uint32(ver)
	if v != 0x000000fe { return fmt.Errorf("version not match: %08x", v) }
	return nil
}

func (u *Unity) Get(id []byte, t server.RequestType, w io.Writer) error {
	b := bytes.NewBuffer(u.b[:0])
	b.WriteByte('g')
	b.WriteByte(byte(t))
	b.Write(id[:32])
	if err := u.c.Write(b.Bytes(), b.Len()); err != nil {return err}
	cmd := u.b[:2]
	if err := u.c.Read(cmd, len(cmd)); err != nil {return err}
	if cmd[0] == '-' {
		return u.c.Read(u.b[:], 32)
	}
	if cmd[0] != '+' || cmd[1] != byte(t) {return fmt.Errorf("get cmd not match: %s", string(cmd))}
	sb := u.b[:16]
	if err := u.c.Read(sb, len(sb)); err != nil {return err}
	if _, err := hex.Decode(sb, sb); err != nil {return err}
	size := int64(binary.BigEndian.Uint64(sb))
	if err := u.c.Read(u.b[:], 32); err != nil {return err}
	if !bytes.Equal(u.b[:32], id) {return fmt.Errorf("cache id not match")}
	read := int64(0)
	for read < size {
		num := int64(len(u.b))
		if size - read < num { num = size - read }
		b := u.b[:num]
		if err := u.c.Read(b, int(num)); err != nil {return fmt.Errorf("read:%c %d != %d err: %v", t, read, size, err)} else {
			read += num
			for b := b; len(b) > 0; {
				if m, err := w.Write(b); err != nil {return err} else { b = b[m:] }
			}
		}
	}
	return nil
}

func (u *Unity) Put(t server.RequestType, size int64, r io.Reader) error {
	b := bytes.NewBuffer(u.b[:0])
	b.WriteByte('p')
	b.WriteByte(byte(t))
	sb := u.b[len(u.b)-8:]
	binary.BigEndian.PutUint64(sb, uint64(size))
	sh := u.b[len(u.b)-16:]
	hex.Encode(sh, sb)
	b.Write(sh)
	if err := u.c.Write(b.Bytes(), b.Len()); err != nil {return err}
	sent := int64(0)
	for sent < size {
		num := int64(len(u.b))
		if size - sent < num { num = size - sent }
		b := u.b[:num]
		if n, err := r.Read(b); err != nil {return err} else {
			sent += int64(n)
			if err := u.c.Write(b, n); err != nil {return err}
		}
	}
	return nil
}

func (u *Unity) STrx(id []byte) error {
	b := u.b[:]
	b[0] = 't'
	b[1] = 's'
	copy(b[2:], id[:32])
	return u.c.Write(b, 34)
}

func (u *Unity) ETrx() error {
	b := u.b[:]
	b[0] = 't'
	b[1] = 'e'
	return u.c.Write(b, 2)
}

func (u *Unity) Pump(size int64, w io.Writer) error {
	buf := make([]byte, 64<<10)
	sent := int64(0)
	for sent < size {
		num := int64(len(buf))
		if size - sent < num { num = size - sent }
		b := buf[:num]
		rand.Read(b[:64])
		sent += num
		for len(b) > 0 {
			if n, err := w.Write(b); err != nil {return err} else {b = b[n:]}
		}
	}
	return nil
}

type Entity struct {
	Guid []byte
	Hash []byte
	Asha []byte
	Isha []byte
	Size int64
}

func (u *Unity) Upload() (*Entity, error) {
	ent := &Entity{}
	id := make([]byte, 32)
	ent.Guid = id[:16]
	rand.Read(ent.Guid)
	ent.Hash = id[16:]
	rand.Read(ent.Hash)
	if err := u.STrx(id); err != nil {return nil, err}
	rand2.Seed(time.Now().UnixNano())
	size := (16<<10) + int64(rand2.Intn(2<<20))
	ent.Size = size
	{
		r, w := io.Pipe()
		go func() {
			defer w.Close()
			h := sha256.New()
			f := io.MultiWriter(w, h)
			if err := u.Pump(size, f); err != nil {return}
			ent.Asha = h.Sum(nil)
		}()

		u.Put(server.RequestTypeBin, size, r)

	}
	if rand2.Int() % 3 > 0 {
		r, w := io.Pipe()
		size := size / 10
		go func() {
			defer w.Close()
			h := sha256.New()
			f := io.MultiWriter(w, h)
			if err := u.Pump(size, f); err != nil {return}
			ent.Isha = h.Sum(nil)
		}()

		u.Put(server.RequestTypeInf, size, r)
	}

	return ent, u.ETrx()
}

type Counter int64
func (c *Counter) Write(p []byte) (int, error) {
	*c += Counter(len(p))
	return len(p), nil
}

func (u *Unity) Download(ent *Entity) error {
	id := make([]byte, 32)
	copy(id[:16], ent.Guid[:16])
	copy(id[16:], ent.Hash[:16])
	{
		var c Counter
		var w io.Writer
		var h hash.Hash
		if !u.Verify {w = &c} else {
			h = sha256.New()
			w = io.MultiWriter(&c, h)
		}
		if err := u.Get(id, server.RequestTypeBin, w); err != nil {return err}
		if h != nil {
			if c == 0 { return nil }
			if int64(c) != ent.Size {return fmt.Errorf("size not match: %d != %d", c, ent.Size)}
			s := h.Sum(nil)
			if len(ent.Asha) > 0 && !bytes.Equal(s, ent.Asha[:32]) {panic(fmt.Errorf("asha not match: %s != %s %s %d", hex.EncodeToString(s), hex.EncodeToString(ent.Asha), hex.EncodeToString(ent.Guid), c))}
		}
	}
	if len(ent.Isha) > 0 {
		var c Counter
		var w io.Writer
		var h hash.Hash
		if !u.Verify {w = &c} else {
			h = sha256.New()
			w = io.MultiWriter(&c, h)
		}
		if err := u.Get(id, server.RequestTypeInf, w); err != nil {return err}
		if h != nil {
			if c == 0 {return nil}
			s := h.Sum(nil)
			if len(ent.Isha) > 0 && !bytes.Equal(s, ent.Isha[:32]) {panic(fmt.Errorf("isha not match: %s != %s %s", hex.EncodeToString(s), hex.EncodeToString(ent.Isha), hex.EncodeToString(ent.Guid)))}
		}
	}
	return nil
}