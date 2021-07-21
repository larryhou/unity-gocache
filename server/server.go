package server

import (
    "encoding/hex"
    "fmt"
    "github.com/larryhou/unity-gocache/config"
    "go.uber.org/zap"
    "io"
    "net"
    "os"
    "path"
    "strconv"
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

type Transaction struct {
    Guid string
    Hash string
}

type CacheServer struct {
}

func (s *CacheServer) Listen(port int) error {
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
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
    defer func() {
        c.Close()
        logger.Info("closed", zap.String("client", c.RemoteAddr().String()))
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

    trx := &Transaction{}
    for {
        cmd := buf[:2]
        if _, err := c.Read(cmd); err != nil {
            if err != io.EOF { logger.Error("read command err", zap.Error(err)) }
            return
        }

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
            guid := hex.EncodeToString(id[:16])
            hash := hex.EncodeToString(id[16:])
            filename := path.Join(config.Path, guid[:2], guid + "-" + hash + "." + t.extension())
            logger.Debug("get",zap.String("cmd", cmd), zap.String("guid", guid), zap.String("hash", hash))
            fi, err := os.Stat(filename)
            p := 0
            hdr := buf[32:]
            if err != nil && os.IsNotExist(err) {
                hdr[p] = '-'
                p++
                hdr[p] = byte(t)
                p++
                copy(hdr[p:], id)
                p += len(id)
                logger.Debug("cache miss", zap.String("cmd", cmd), zap.String("guid", guid), zap.String("hash", hash))
                if _, err := c.Write(hdr[:p]); err != nil { logger.Error("send get - err", zap.Error(err));return }
                continue
            }
            hdr[p] = '+'
            p++
            hdr[p] = byte(t)
            p++
            hs := fmt.Sprintf("%016x", fi.Size())
            for i := 0; i < len(hs); i++ {
                hdr[p] = hs[i]
                p++
            }
            copy(hdr[p:], id)
            p += len(id)
            //logger.Debug("sent get +", zap.String("header", string(hdr[:p])))
            if _, err := c.Write(hdr[:p]); err != nil { logger.Error("send get + err", zap.Error(err));return }

            file, err := os.Open(filename)
            if err != nil {logger.Error("read cache err", zap.String("file", filename), zap.Error(err));return }
            sent := int64(0)
            for size := fi.Size(); sent < size; {
                num := int64(len(buf))
                if size - sent < num { num = size - sent }
                if _, err := file.Read(buf[:num]); err != nil {
                    if err == io.EOF { break }
                    file.Close()
                    logger.Error("send body err", zap.Int64("sent", sent), zap.Int64("size", size), zap.Error(err))
                    return
                }
                sent += num
            }
            file.Close()
            logger.Debug("send success", zap.Int64("size", fi.Size()), zap.Int64("sent", sent), zap.String("file", filename))

        case 'p':
            t := RequestType(cmd[1])
            cmd := string(cmd)
            s := buf[:16]
            if _, err := c.Read(s); err != nil {
                logger.Error("read put size err", zap.Error(err))
                return
            }

            size, err := strconv.ParseInt(string(s), 16, 32)
            if err != nil {
                logger.Error("parse put size err", zap.Error(err))
                return
            }

            logger.Debug("put",zap.String("cmd", cmd), zap.String("guid", trx.Guid), zap.String("hash", trx.Hash))

            dir := path.Join(config.Path, trx.Guid[:2])
            if _, err := os.Stat(dir); err != nil || os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
            filename := path.Join(dir, trx.Guid + "-" + trx.Hash + "." + t.extension())
            file, err := os.OpenFile(filename, os.O_CREATE | os.O_WRONLY, 0700)
            if err != nil {
                logger.Error("create file err", zap.String("file", filename), zap.Error(err))
                return
            }

            read := int64(0)
            writ := int64(0)
            for read < size {
                num := int64(len(buf))
                if size - read < num { num = size - read }
                b := buf[:num]
                if _, err := c.Read(b); err != nil {
                    file.Close()
                    logger.Error("read body err", zap.Int64("read", read), zap.Int64("size", size), zap.Error(err))
                    os.Remove(filename)
                    return
                }
                if n, err := file.Write(b); err != nil {
                    file.Close()
                    logger.Error("write cache err", zap.Int64("write", writ), zap.Int64("size", size), zap.Error(err))
                    os.Remove(filename)
                    return
                } else {
                    writ += int64(n)
                }

                read += num
            }
            file.Close()
            logger.Debug("receive success", zap.Int64("size", size), zap.Int64("read", read), zap.String("file", filename))

        case 't':
            switch cmd[1] {
            case 's':
                id := buf[:32]
                if _, err := c.Read(id); err != nil {
                    logger.Error("read transaction err", zap.Error(err))
                    return
                }
                trx.Guid = hex.EncodeToString(id[:16])
                trx.Hash = hex.EncodeToString(id[16:])
                logger.Debug("transaction start", zap.String("guid", trx.Guid), zap.String("hash", trx.Hash))
            case 'e':
                logger.Debug("transaction end", zap.String("guid", trx.Guid), zap.String("hash", trx.Hash))
            }
        default:
            logger.Error("unsupported command", zap.String("cmd", string(cmd)))
            return
        }
    }
}