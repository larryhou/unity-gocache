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

type CacheServer struct {
    Port     int
    Path     string
    LogLevel int
    MemCap   uint
    temp     string
}

func (s *CacheServer) Listen() error {
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
    if err != nil {return err}
    mcache.core.capacity = int64(s.MemCap) << 20
    s.temp = path.Join(s.Path, "temp")
    {
        l, err := zap.NewDevelopment(zap.IncreaseLevel(zapcore.Level(s.LogLevel)))
        if err != nil { panic(err) }
        logger = l
    }
    for {
        c, err := listener.Accept()
        if err != nil { continue }
        go s.Handle(c)
    }
}

func (s *CacheServer) Send(c net.Conn, event chan *Context) {
    addr := c.RemoteAddr().String()
    dsize := int64(0)
    ts := time.Now()
    defer func() {
        c.Close()
        if dsize > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(dsize) / elapse
            logger.Info("closed w", zap.String("addr", addr), zap.Int64("size", dsize), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
        } else { logger.Info("closed w", zap.String("addr", addr)) }
    }()

    buf := make([]byte, 1280)
    hdr := bytes.NewBuffer(buf[:0])
    for ctx := range event {
        cmd := string(ctx.command[:])
        switch cmd[0] {
        case 'g':
            t := RequestType(cmd[1])
            filename := path.Join(s.Path, ctx.guid[:2], ctx.guid + "-" + ctx.hash + "." + t.extension())
            logger.Debug("get +++", zap.String("cmd", cmd), zap.String("guid", ctx.guid))
            fi, err := os.Stat(filename)
            hdr.Reset()
            exist := false
            if err != nil && os.IsNotExist(err) {
                hdr.WriteByte('-')
                hdr.WriteByte(byte(t))
                logger.Debug("mis ---", zap.String("cmd", cmd), zap.String("guid", ctx.guid))
            } else {
                hdr.WriteByte('+')
                hdr.WriteByte(byte(t))
                sb := buf[len(buf)-8:]
                binary.BigEndian.PutUint64(sb, uint64(fi.Size()))
                sh := buf[len(buf)-16:]
                hex.Encode(sh, sb)
                hdr.Write(sh)
                exist = true
            }

            hdr.Write(ctx.id[:]) /* guid + hash */
            if _, err := c.Write(hdr.Bytes()); err != nil { logger.Error("send get + err", zap.Error(err));return }
            dsize += int64(hdr.Len())
            if !exist {continue}

            logger.Debug("get >>>", zap.String("cmd", cmd), zap.String("guid", ctx.guid), zap.Int64("size", fi.Size()))

            file, err := Open(filename, hex.EncodeToString(ctx.id[:]) + string(t))
            if err != nil {logger.Error("get read cache err", zap.String("file", filename), zap.Error(err));return }
            sent := int64(0)
            for size := fi.Size(); sent < size; {
                num := int64(len(buf))
                if size - sent < num { num = size - sent }
                b := buf[:num]
                if n, err := file.Read(b); err != nil {
                    file.Close()
                    logger.Error("get read file err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                    return
                } else {
                    sent += int64(n)
                    for b := b[:n]; len(b) > 0; {
                        if m, err := c.Write(b); err != nil {
                            file.Close()
                            logger.Error("get sent body err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                            return
                        } else { b = b[m:] }
                    }
                }
            }
            file.Close()
            if sent == fi.Size() { logger.Debug("get success", zap.String("cmd", cmd), zap.Int64("sent", sent), zap.String("file", filename)) }
            dsize += sent
        }
    }
}

func (s *CacheServer) Handle(c net.Conn) {
    addr := c.RemoteAddr().String()
    logger.Info("connected", zap.String("addr", addr))
    event := make(chan *Context)
    go s.Send(c, event)

    ts := time.Now()
    usize := int64(0)
    defer func() {
        close(event)
        if usize > 0 {
            elapse := time.Now().Sub(ts).Seconds()
            speed := float64(usize) / elapse
            logger.Info("closed r", zap.String("addr", addr), zap.Int64("size", usize), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
        } else { logger.Info("closed r", zap.String("addr", addr)) }
    }()

    buf := make([]byte, 1024)

    ver := buf[:2]
    if _, err := c.Read(ver); err != nil { logger.Error("read version err", zap.Error(err));return }

    v, _ := strconv.ParseInt(string(ver), 16, 32)
    if _, err := c.Write([]byte(fmt.Sprintf("%08x", v))); err != nil {
        logger.Error("echo version err", zap.Error(err))
        return
    }

    trx := &Entity{}
    for {
        cmd := buf[:2]
        if _, err := c.Read(cmd); err != nil {
            if err != io.EOF { logger.Error("read command err", zap.Error(err)) }
            return
        }

        usize += 2
        switch cmd[0] {
        case 'q': return
        case 'g':
            cmd := string(cmd)
            id := buf[:32]
            if _, err := c.Read(id); err != nil { logger.Error("read get id err", zap.Error(err));return }
            usize += int64(len(id))
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
            if _, err := c.Read(b); err != nil {logger.Error("put read size err", zap.Error(err));return}
            usize += int64(len(b))
            size, err := strconv.ParseInt(string(b), 16, 32)
            if err != nil {logger.Error("put parse size err", zap.Error(err));return}
            logger.Debug("put", zap.String("cmd", cmd), zap.String("guid", trx.guid), zap.Int64("size", size))

            dir := path.Join(s.Path, trx.guid[:2])
            if _, err := os.Stat(dir); err != nil || os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
            filename := path.Join(dir, trx.guid + "-" + trx.hash + "." + t.extension())
            name := buf[:32]
            rand.Read(name)
            if _, err := os.Stat(s.temp); err != nil || os.IsNotExist(err) { os.MkdirAll(s.temp, 0700) }
            file, err := NewFile(path.Join(s.temp, hex.EncodeToString(name)), trx.guid+trx.hash+string(t), size)
            if err != nil {logger.Error("put create file err", zap.String("file", filename), zap.Error(err));return}
            write := int64(0)
            for write < size {
                num := int64(len(buf))
                if size - write < num { num = size - write }
                b := buf[:num]
                if n, err := c.Read(b); err != nil {file.Close();os.Remove(file.Name());return} else {
                    write += int64(n)
                    for b = b[:n]; len(b) > 0; {
                        if m, err := file.Write(b); err != nil {
                            file.Close()
                            os.Remove(file.Name())
                            logger.Error("put write cache err", zap.Int64("write", write), zap.Int64("size", size), zap.Error(err))
                            return
                        } else { b = b[m:] }
                    }
                }
            }
            file.Close()
            if err := os.Rename(file.Name(), filename); err != nil {
                logger.Error("put failure", zap.String("cmd", cmd), zap.Int64("write", write), zap.String("file", filename), zap.Error(err))
                return
            }
            if write == size { logger.Debug("put success", zap.String("cmd", cmd), zap.Int64("write", write), zap.String("file", filename))}
            usize += write

        case 't':
            switch cmd[1] {
            case 's':
                id := buf[:32]
                if _, err := c.Read(id); err != nil {logger.Error("trx read err", zap.Error(err));return}
                trx.guid = hex.EncodeToString(id[:16])
                trx.hash = hex.EncodeToString(id[16:])
                logger.Debug("trx open", zap.String("guid", trx.guid), zap.String("hash", trx.hash))
                usize += int64(len(id))
            case 'e':
                logger.Debug("trx done", zap.String("guid", trx.guid), zap.String("hash", trx.hash))
            }
        default:
            logger.Error("unsupported command", zap.String("cmd", string(cmd)))
            return
        }
    }
}