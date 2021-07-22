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
)

type Session struct {
	Addr string
	Port int
	c    net.Conn
	b    [1024]byte
}

func (s *Session) Close() error {
	if s.c != nil {
		return s.c.Close()
	}
	return nil
}

func (s *Session) Connect() error {
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", s.Addr, s.Port))
	if err != nil {return err}
	s.c = c
	if _, err := c.Write([]byte{'7', 'f'}); err != nil {return err}
	ver := make([]byte, 8)
	if _, err := c.Read(ver); err != nil {return err}
	if _, err := hex.Decode(ver, ver); err != nil {return err}
	v := binary.BigEndian.Uint32(ver)
	if v != 0x0000007f { return fmt.Errorf("version not match: %08x", v) }
	return nil
}

func (s *Session) Get(id []byte, t server.RequestType, w io.Writer) error {
	b := bytes.NewBuffer(s.b[:0])
	b.WriteByte('g')
	b.WriteByte(byte(t))
	b.Write(id[:32])
	if _, err := s.c.Write(b.Bytes()); err != nil {return err}
	cmd := s.b[:2]
	if _, err := s.c.Read(cmd); err != nil {return err}
	if cmd[0] == '-' {
		_, err := s.c.Read(s.b[:32])
		return err
	}
	if cmd[0] != '+' || cmd[1] != byte(t) {return fmt.Errorf("rsp cmd not match: %s", string(cmd))}
	sb := s.b[:16]
	if _, err := s.c.Read(sb); err != nil {return err}
	if _, err := s.c.Read(s.b[:32]); err != nil {return err}
	if _, err := hex.Decode(sb, sb); err != nil {return err}
	size := int64(binary.BigEndian.Uint64(sb))
	read := int64(0)
	for read < size {
		num := int64(len(s.b))
		if size - read < num { num = size - read }
		b := s.b[:num]
		if _, err := s.c.Read(b); err != nil {return err}
		if _, err := w.Write(b); err != nil {return err}
		read += num
	}
	return nil
}

func (s *Session) Put(t server.RequestType, size int64, r io.Reader) error {
	b := bytes.NewBuffer(s.b[:0])
	b.WriteByte('p')
	b.WriteByte(byte(t))
	sb := s.b[len(s.b)-8:]
	binary.BigEndian.PutUint64(sb, uint64(size))
	sh := s.b[len(s.b)-16:]
	hex.Encode(sh, sb)
	b.Write(sh)
	if _, err := s.c.Write(b.Bytes()); err != nil {return err}
	sent := int64(0)
	for sent < size {
		num := int64(len(s.b))
		if size - sent < num { num = size - sent }
		b := s.b[:num]
		if _, err := r.Read(b); err != nil {return err}
		if _, err := s.c.Write(b); err != nil {return err}
		sent += num
	}
	return nil
}

func (s *Session) STrx(id []byte) error {
	b := bytes.NewBuffer(s.b[:0])
	b.WriteByte('t')
	b.WriteByte('s')
	b.Write(id[:32])
	_, err := s.c.Write(b.Bytes())
	return err
}

func (s *Session) ETrx() error {
	b := bytes.NewBuffer(s.b[:0])
	b.WriteByte('t')
	b.WriteByte('e')
	_, err := s.c.Write(b.Bytes())
	return err
}

func (s *Session) Write(b []byte, size int64, w io.Writer) error {
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
}

func (s *Session) Upload() (*Entity, error) {
	ent := &Entity{}
	id := make([]byte, 32)
	ent.Guid = id[:16]
	rand.Read(ent.Guid)
	ent.Hash = id[16:]
	rand.Read(ent.Hash)
	if err := s.STrx(id); err != nil {return nil, err}
	size := (16<<10) + int64(rand2.Intn(2<<20))
	b := make([]byte, 1024)
	{
		r, w, err := os.Pipe()
		if err != nil {return nil, err }

		h := sha256.New()
		f := io.MultiWriter(w, h)
		s.Put(server.RequestTypeBin, size, r)

		go func() {
			defer w.Close()
			s.Write(b, size, f)
		}()

		ent.Asha = h.Sum(nil)
	}
	if rand2.Int() % 2 == 0 {
		r, w, err := os.Pipe()
		if err != nil {return nil, err }

		size := size / 10
		h := sha256.New()
		f := io.MultiWriter(w, h)
		s.Put(server.RequestTypeInf, size, r)

		go func() {
			defer w.Close()
			s.Write(b, size, f)
		}()

		ent.Isha = h.Sum(nil)
	}

	return ent, s.ETrx()
}

func (s *Session) Download(ent *Entity) error {
	id := make([]byte, 32)
	copy(id[:16], ent.Guid[:16])
	copy(id[16:], ent.Hash[:16])

	{
		h := sha256.New()
		if err := s.Get(id, server.RequestTypeBin, h); err != nil {return err}
		if !bytes.Equal(h.Sum(nil), ent.Asha[:32]) {return fmt.Errorf("asha not match")}
	}

	if len(ent.Isha) > 0 {
		h := sha256.New()
		if err := s.Get(id, server.RequestTypeInf, h); err != nil {return err}
		if !bytes.Equal(h.Sum(nil), ent.Isha[:32]) {return fmt.Errorf("isha not match")}
	}

	return nil
}