package server

import (
    "bytes"
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "go.uber.org/zap"
    "io"
    "net"
    "os"
    "path"
    "strconv"
    "time"
)

var logger *zap.Logger

func init() {
    l, err := zap.NewDevelopment()
    if err != nil { panic(err) }
    logger = l
}

const (
    TimeoutRead = 3
    TimeoutWrite = 3
)

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
    Guid string
    Hash string
}

type CacheServer struct {
    Port int
    Path string
}

func (s *CacheServer) Listen() error {
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
    if err != nil {return err}

    for {
        c, err := listener.Accept()
        if err != nil {
            logger.Error(c.RemoteAddr().String(), zap.Error(err))
            continue
        }

        go s.Handle(c)
    }
}

func (s *CacheServer) Handle(c net.Conn) {
    ts := time.Now()
    down, up := int64(0), int64(0)
    defer func() {
        c.Close()
        elapse := time.Now().Sub(ts).Seconds()
        speed := float64(down) / elapse
        logger.Info("closed", zap.String("client", c.RemoteAddr().String()), zap.Int64("usize", up), zap.Int64("dsize", down), zap.Float64("speed", speed), zap.Float64("elapse", elapse))
    }()
    logger.Info("connected", zap.String("client", c.RemoteAddr().String()))

    buf := make([]byte, 1024)
    ver := buf[:2]
    if _, err := c.Read(ver); err != nil {
        logger.Error("read version err", zap.Error(err))
        return
    }

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

        up += 2

        switch cmd[0] {
        case 'q': return
        case 'g':
            t := RequestType(cmd[1])
            cmd := string(cmd)
            id := buf[:32]
            if _, err := c.Read(id); err != nil {
                logger.Error("read get id err", zap.Error(err))
                return
            }
            up += int64(len(id))
            guid := hex.EncodeToString(id[:16])
            hash := hex.EncodeToString(id[16:])
            filename := path.Join(s.Path, guid[:2], guid + "-" + hash + "." + t.extension())
            logger.Debug("get",zap.String("cmd", cmd), zap.String("guid", guid), zap.String("hash", hash))
            fi, err := os.Stat(filename)
            hdr := bytes.NewBuffer(buf[32:32])
            exist := false
            if err != nil && os.IsNotExist(err) {
                hdr.WriteByte('-')
                hdr.WriteByte(byte(t))
                logger.Debug("mis", zap.String("cmd", cmd), zap.String("guid", guid), zap.String("hash", hash))
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

            hdr.Write(id) /* guid + hash */
            if _, err := c.Write(hdr.Bytes()); err != nil { logger.Error("send get + err", zap.Error(err));return }
            down += int64(hdr.Len())
            if !exist {continue}

            file, err := os.Open(filename)
            if err != nil {logger.Error("get read cache err", zap.String("file", filename), zap.Error(err));return }
            sent, read := int64(0), int64(0)
            for size := fi.Size(); sent < size; {
                num := int64(len(buf))
                if size - sent < num { num = size - sent }
                b := buf[:num]
                if n, err := file.Read(b); err != nil {
                    if err == io.EOF { break }
                    file.Close()
                    logger.Error("get read body err", zap.Int64("read", read), zap.Int64("size", size), zap.Error(err))
                    return
                } else { read += int64(n) }
                if n, err := c.Write(b); err != nil {
                    logger.Error("get sent body err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                    return
                } else { sent += int64(n) }
            }
            file.Close()
            if sent == fi.Size() { logger.Debug("get send success", zap.Int64("size", fi.Size()), zap.Int64("sent", sent), zap.String("file", filename)) }
            down += sent

        case 'p':
            t := RequestType(cmd[1])
            cmd := string(cmd)
            b := buf[:16]
            if _, err := c.Read(b); err != nil {logger.Error("put read size err", zap.Error(err));return}
            up += int64(len(b))
            size, err := strconv.ParseInt(string(b), 16, 32)
            if err != nil {logger.Error("put parse size err", zap.Error(err));return}
            logger.Debug("put",zap.String("cmd", cmd), zap.String("guid", trx.Guid), zap.String("hash", trx.Hash))

            dir := path.Join(s.Path, trx.Guid[:2])
            if _, err := os.Stat(dir); err != nil || os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
            filename := path.Join(dir, trx.Guid + "-" + trx.Hash + "." + t.extension())
            file, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0700)
            if err != nil {logger.Error("put create file err", zap.String("file", filename), zap.Error(err));return}
            read, write := int64(0), int64(0)
            for read < size {
                num := int64(len(buf))
                if size - read < num { num = size - read }
                b := buf[:num]
                if n, err := c.Read(b); err != nil {
                    file.Close()
                    logger.Error("put read body err", zap.Int64("read", read), zap.Int64("size", size), zap.Error(err))
                    os.Remove(filename)
                    return
                } else { read += int64(n) }
                if n, err := file.Write(b); err != nil {
                    file.Close()
                    logger.Error("put write cache err", zap.Int64("write", write), zap.Int64("size", size), zap.Error(err))
                    os.Remove(filename)
                    return
                } else { write += int64(n) }
            }
            file.Close()
            if write == size { logger.Debug("put cache success", zap.Int64("size", size), zap.Int64("write", write), zap.String("file", filename))}
            up += read

        case 't':
            switch cmd[1] {
            case 's':
                id := buf[:32]
                if _, err := c.Read(id); err != nil {logger.Error("trx read err", zap.Error(err));return}
                trx.Guid = hex.EncodeToString(id[:16])
                trx.Hash = hex.EncodeToString(id[16:])
                logger.Debug("trx start", zap.String("guid", trx.Guid), zap.String("hash", trx.Hash))
                up += int64(len(id))
            case 'e':
                logger.Debug("trx end", zap.String("guid", trx.Guid), zap.String("hash", trx.Hash))
            }
        default:
            logger.Error("unsupported command", zap.String("cmd", string(cmd)))
            return
        }
    }
}