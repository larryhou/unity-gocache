package server

import (
    "bytes"
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "io"
    "math/rand"
    "net"
    "os"
    "path"
    "strconv"
    "time"
)

var logger *zap.Logger

type RequestType byte
const (
    RequestTypeInf RequestType = 'i'
    RequestTypeBin RequestType = 'a'
    RequestTypeRes RequestType = 'r'
)

func (r RequestType) extension() string {
    switch r {
    case RequestTypeInf: return "info"
    case RequestTypeBin: return "bin"
    case RequestTypeRes: return "resource"
    default: return "unknown"
    }
}

type Entity struct {
    guid string
    hash string
}

type Context struct {
    Entity
    command [2]byte
    id [32]byte
}

type Stream struct {
    Rwp io.ReadWriter
}

func (s *Stream) Name() string {
    if f, ok := s.Rwp.(*File); ok {return f.Name()}
    return ""
}

func (s *Stream) ReadString(buf []byte) (string, error) {
    if err := s.Read(buf, 2); err != nil {return "", err}
    n := int(binary.BigEndian.Uint16(buf))
    if n < cap(buf) {
        if err := s.Read(buf, n); err != nil {return "", err}
        return string(buf[:n]), nil
    } else {
        b := &bytes.Buffer{}
        for t := 0; t < n; {
            num := cap(buf)
            if n - t < num {num = n - t}
            if err := s.Read(buf, num); err != nil {return "", err}
            if _, err := b.Write(buf[:num]); err != nil {return "", err}
            t += num
        }
        return b.String(), nil
    }
}

func (s *Stream) WriteString(buf []byte, v string) error {
    n := len(v)
    binary.BigEndian.PutUint16(buf, uint16(n))
    if err := s.Write(buf, 2); err != nil {return err}
    for t := 0; t < n; {
        num := cap(buf)
        if n - t < num {num = n - t}
        copy(buf, v[t:t+num])
        if err := s.Write(buf, num); err != nil {return err}
        t += num
    }
    return nil
}

func (s *Stream) Read(p []byte, n int) error {
    for t := 0; t < n; {
        if i, err := s.Rwp.Read(p[t:n]); err != nil {return err} else {t+=i}
    }
    return nil
}

func (s *Stream) Write(p []byte, n int) error {
    for t := 0; t < n; {
        if i, err := s.Rwp.Write(p[t:n]); err != nil {return err} else {t+=i}
    }
    return nil
}

func (s *Stream) Close() error {
    if c, ok := s.Rwp.(io.Closer); ok { return c.Close() }
    return nil
}

type Air struct { }
func (i Air) Read(p []byte) (int, error)  { return len(p), nil }
func (i Air) Write(p []byte) (int, error) { return len(p), nil }

type CacheServer struct {
    Port     int
    Path     string
    LogLevel int
    CacheCap int
    DryRun   bool
    temp     string
}

func (s *CacheServer) Listen() error {
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
    if err != nil {return err}
    mcache.core.capacity = s.CacheCap
    s.temp = path.Join(s.Path, "temp")
    {
        l, err := zap.NewDevelopment(zap.IncreaseLevel(zapcore.Level(s.LogLevel)))
        if err != nil { panic(err) }
        logger = l
    }
    //go mcache.core.stat()
    for {
        c, err := listener.Accept()
        if err != nil { continue }
        go s.Handle(c)
    }
}

func (s *CacheServer) Send(c net.Conn, event chan *Context) {
    conn := &Stream{Rwp: c}
    addr := c.RemoteAddr().String()
    outgoing := int64(0)
    ts := time.Now()
    defer func() {
        c.Close()
        if outgoing > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(outgoing) / elapse
            logger.Info("closed w", zap.String("addr", addr), zap.Int64("size", outgoing), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
        } else { logger.Info("closed w", zap.String("addr", addr)) }
    }()

    buf := make([]byte, 64<<10)
    hdr := bytes.NewBuffer(buf[:0])
    for ctx := range event {
        cmd := string(ctx.command[:])
        switch cmd[0] {
        case 'g':
            t := RequestType(cmd[1])

            exists := true
            var in *Stream
            size := int64(0)
            filename := path.Join(s.Path, ctx.guid[:2], ctx.guid + "-" + ctx.hash + "." + t.extension())
            if s.DryRun {
                in = &Stream{Rwp: &Air{}}
                size = 2<<20
            } else {
                file, err := Open(filename, ctx.guid+ctx.hash+string(t))
                if err == nil { size = file.size } else { exists = false }
                in = &Stream{Rwp: file}
            }

            logger.Debug("get +++", zap.String("cmd", cmd), zap.String("guid", ctx.guid))

            hdr.Reset()
            if !exists {
                hdr.WriteByte('-')
                hdr.WriteByte(byte(t))
                logger.Debug("mis ---", zap.String("cmd", cmd), zap.String("guid", ctx.guid))
            } else {
                hdr.WriteByte('+')
                hdr.WriteByte(byte(t))
                sb := buf[len(buf)-8:]
                binary.BigEndian.PutUint64(sb, uint64(size))
                sh := buf[len(buf)-16:]
                hex.Encode(sh, sb)
                hdr.Write(sh)
            }

            hdr.Write(ctx.id[:]) /* guid + hash */
            if err := conn.Write(hdr.Bytes(), hdr.Len()); err != nil { logger.Error("send get + err", zap.Error(err));return }
            outgoing += int64(hdr.Len())
            if !exists {continue}
            if size == 0 {panic(filename)}

            logger.Debug("get >>>", zap.String("cmd", cmd), zap.String("guid", ctx.guid), zap.Int64("size", size))

            if file, ok := in.Rwp.(*File); ok && file.c {
                m := file.m
                if err := conn.Write(m.Bytes(), m.Len()); err != nil {
                    logger.Error("get sent cache err", zap.Int64("size", size), zap.Error(err))
                    return
                }
                outgoing += int64(m.Len())
                logger.Debug("get success", zap.String("cmd", cmd), zap.Int("sent", m.Len()), zap.String("file", filename), zap.Bool("cache", true))
                continue
            }

            sent := int64(0)
            for sent < size {
                num := int64(len(buf))
                if size - sent < num { num = size - sent }
                if err := in.Read(buf, int(num)); err != nil {
                    in.Close()
                    logger.Error("get read file err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                    return
                } else {
                    sent += num
                    if err := conn.Write(buf, int(num)); err != nil {
                        in.Close()
                        logger.Error("get sent body err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                        return
                    }
                }
            }
            in.Close()
            logger.Debug("get success", zap.String("cmd", cmd), zap.Int64("sent", sent), zap.String("file", filename))
            outgoing += sent
        }
    }
}

func (s *CacheServer) Handle(c net.Conn) {
    conn := &Stream{Rwp: c}
    addr := c.RemoteAddr().String()
    logger.Info("connected", zap.String("addr", addr))
    event := make(chan *Context)
    go s.Send(c, event)

    ts := time.Now()
    incoming := int64(0)
    defer func() {
        close(event)
        if incoming > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(incoming) / elapse
            logger.Info("closed r", zap.String("addr", addr), zap.Int64("size", incoming), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
        } else { logger.Info("closed r", zap.String("addr", addr)) }
    }()

    buf := make([]byte, 16<<10)

    ver := buf[:2]
    if err := conn.Read(ver, len(ver)); err != nil { logger.Error("read version err", zap.Error(err));return }

    v, _ := strconv.ParseInt(string(ver), 16, 32)
    if err := conn.Write([]byte(fmt.Sprintf("%08x", v)), 8); err != nil {
        logger.Error("echo version err", zap.Error(err))
        return
    }

    trx := &Entity{}
    for {
        cmd := buf[:2]
        if err := conn.Read(cmd, len(cmd)); err != nil {
            if err != io.EOF { logger.Error("read command err", zap.Error(err)) }
            return
        }

        incoming += 2
        switch cmd[0] {
        case 'q': return
        case 'g':
            cmd := string(cmd)
            id := buf[:32]
            if err := conn.Read(id, len(id)); err != nil { logger.Error("read get id err", zap.Error(err));return }
            incoming += int64(len(id))
            ctx := &Context{}
            copy(ctx.command[0:], cmd)
            ctx.guid = hex.EncodeToString(id[:16])
            ctx.hash = hex.EncodeToString(id[16:])
            copy(ctx.id[:], id)
            logger.Debug("get", zap.String("cmd", cmd), zap.String("guid", ctx.guid), zap.String("hash", ctx.hash))
            event <- ctx

        case 'p':
            t := RequestType(cmd[1])
            cmd := string(cmd)
            b := buf[:16]
            if err := conn.Read(b, len(b)); err != nil {logger.Error("put read size err", zap.Error(err));return}
            incoming += int64(len(b))
            size, err := strconv.ParseInt(string(b), 16, 32)
            if err != nil {logger.Error("put parse size err", zap.Error(err));return}
            logger.Debug("put", zap.String("cmd", cmd), zap.String("guid", trx.guid), zap.Int64("size", size))

            dir := path.Join(s.Path, trx.guid[:2])
            if _, err := os.Stat(dir); err != nil || os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
            filename := path.Join(dir, trx.guid + "-" + trx.hash + "." + t.extension())

            var out *Stream
            if s.DryRun { out = &Stream{Rwp: Air{}} } else {
                name := buf[:32]
                rand.Read(name)
                if _, err := os.Stat(s.temp); err != nil || os.IsNotExist(err) { os.MkdirAll(s.temp, 0700) }
                file, err := NewFile(path.Join(s.temp, hex.EncodeToString(name)), trx.guid+trx.hash+string(t), size)
                if err != nil {logger.Error("put init err", zap.String("file", filename), zap.Error(err));return}
                out = &Stream{Rwp: file}
            }

            received := int64(0)
            for received < size {
                num := int64(len(buf))
                if size - received < num { num = size - received }
                if err := conn.Read(buf, int(num)); err != nil {out.Close();os.Remove(out.Name());return} else {
                    received += num
                    if err := out.Write(buf, int(num)); err != nil {
                        out.Close()
                        os.Remove(out.Name())
                        logger.Error("put save err", zap.Int64("received", received), zap.Int64("size", size), zap.Error(err))
                        return
                    }
                }
            }
            out.Close()
            if _, ok := out.Rwp.(*File); ok {
                if err := os.Rename(out.Name(), filename); err != nil {
                    logger.Error("put failure", zap.String("cmd", cmd), zap.Int64("received", received), zap.String("file", filename), zap.Error(err))
                    return
                }
            }

            logger.Debug("put success", zap.String("cmd", cmd), zap.Int64("received", received), zap.String("file", filename))
            incoming += received

        case 't':
            switch cmd[1] {
            case 's':
                id := buf[:32]
                if err := conn.Read(id, len(id)); err != nil {logger.Error("trx read err", zap.Error(err));return}
                trx.guid = hex.EncodeToString(id[:16])
                trx.hash = hex.EncodeToString(id[16:])
                logger.Debug("trx open", zap.String("guid", trx.guid), zap.String("hash", trx.hash))
                incoming += int64(len(id))
            case 'e':
                logger.Debug("trx done", zap.String("guid", trx.guid), zap.String("hash", trx.hash))
            }
        default:
            logger.Error("unsupported command", zap.String("cmd", string(cmd)))
            return
        }
    }
}