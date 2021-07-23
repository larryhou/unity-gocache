package client

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/larryhou/unity-gocache/server"
	"io"
	rand2 "math/rand"
	"net"
	"os"
	"time"
)

type Unity struct {
	Addr string
	Port int
	c    net.Conn
	b    [1024]byte
}

func (u *Unity) Close() error {
	if u.c != nil {
		return u.c.Close()
	}
	return nil
}

func (u *Unity) Connect() error {
	c, err := net.Dial("tcp", fmt.Sprintf("%u:%d", u.Addr, u.Port))
	if err != nil {return err}
	u.c = c
	if _, err := c.Write([]byte{'7', 'f'}); err != nil {return err}
	ver := make([]byte, 8)
	if _, err := c.Read(ver); err != nil {return err}
	if _, err := hex.Decode(ver, ver); err != nil {return err}
	v := binary.BigEndian.Uint32(ver)
	if v != 0x0000007f { return fmt.Errorf("version not match: %08x", v) }
	return nil
}

func (u *Unity) Get(id []byte, t server.RequestType, w io.Writer) error {
	b := bytes.NewBuffer(u.b[:0])
	b.WriteByte('g')
	b.WriteByte(byte(t))
	b.Write(id[:32])
	if _, err := u.c.Write(b.Bytes()); err != nil {return err}
	cmd := u.b[:2]
	if _, err := u.c.Read(cmd); err != nil {return err}
	if cmd[0] == '-' {
		_, err := u.c.Read(u.b[:32])
		return err
	}
	if cmd[0] != '+' || cmd[1] != byte(t) {return fmt.Errorf("rsp cmd not match: %u", string(cmd))}
	sb := u.b[:16]
	if _, err := u.c.Read(sb); err != nil {return err}
	if _, err := u.c.Read(u.b[:32]); err != nil {return err}
	if _, err := hex.Decode(sb, sb); err != nil {return err}
	size := int64(binary.BigEndian.Uint64(sb))
	read := int64(0)
	for read < size {
		num := int64(len(u.b))
		if size - read < num { num = size - read }
		b := u.b[:num]
		if _, err := u.c.Read(b); err != nil {return err}
		if _, err := w.Write(b); err != nil {return err}
		read += num
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
	if _, err := u.c.Write(b.Bytes()); err != nil {return err}
	sent := int64(0)
	for sent < size {
		num := int64(len(u.b))
		if size - sent < num { num = size - sent }
		b := u.b[:num]
		if _, err := r.Read(b); err != nil {return err}
		if _, err := u.c.Write(b); err != nil {return err}
		sent += num
	}
	return nil
}

func (u *Unity) STrx(id []byte) error {
	b := bytes.NewBuffer(u.b[:0])
	b.WriteByte('t')
	b.WriteByte('u')
	b.Write(id[:32])
	_, err := u.c.Write(b.Bytes())
	return err
}

func (u *Unity) ETrx() error {
	b := bytes.NewBuffer(u.b[:0])
	b.WriteByte('t')
	b.WriteByte('e')
	_, err := u.c.Write(b.Bytes())
	return err
}

func (u *Unity) Write(b []byte, size int64, w io.Writer) error {
	sent := int64(0)
	for sent < size {
		num := int64(len(b))
		if size - sent < num { num = size - sent }
		b := b[:num]
		rand.Read(b)
		if _, err := w.Write(b); err != nil {return err}
		sent += num
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
	rand2.Seed(time.Now().Unix())
	size := (16<<10) + int64(rand2.Intn(2<<20))
	ent.Size = size
	b := make([]byte, 1024)
	{
		r, w, err := os.Pipe()
		if err != nil {return nil, err }

		h := sha256.New()
		f := io.MultiWriter(w, h)
		go func() {
			defer w.Close()
			u.Write(b, size, f)
		}()

		u.Put(server.RequestTypeBin, size, r)
		ent.Asha = h.Sum(nil)
	}
	if rand2.Int() % 3 > 0 {
		r, w, err := os.Pipe()
		if err != nil {return nil, err }

		size := size / 10
		h := sha256.New()
		f := io.MultiWriter(w, h)
		go func() {
			defer w.Close()
			u.Write(b, size, f)
		}()

		u.Put(server.RequestTypeInf, size, r)
		ent.Isha = h.Sum(nil)
	}

	return ent, u.ETrx()
}

func (u *Unity) Download(ent *Entity) error {
	id := make([]byte, 32)
	copy(id[:16], ent.Guid[:16])
	copy(id[16:], ent.Hash[:16])

	{
		h := sha256.New()
		if err := u.Get(id, server.RequestTypeBin, h); err != nil {return err}
		if !bytes.Equal(h.Sum(nil), ent.Asha[:32]) {return fmt.Errorf("asha not match")}
	}

	if len(ent.Isha) > 0 {
		h := sha256.New()
		if err := u.Get(id, server.RequestTypeInf, h); err != nil {return err}
		if !bytes.Equal(h.Sum(nil), ent.Isha[:32]) {return fmt.Errorf("isha not match")}
	}

	return nil
}