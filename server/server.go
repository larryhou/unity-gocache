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

    cmd := buf[:2]
    if _, err := c.Read(cmd); err != nil {
        if err != io.EOF { logger.Error("read command err", zap.Error(err)) }
        return
    }

    var trx Transaction

    switch cmd[0] {
    case 'q': return
    case 'g':
    case 'p':
        t := RequestType(cmd[1])
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

        dir := path.Join(config.Path, trx.Guid[:2])
        if _, err := os.Stat(dir); err != nil || os.IsNotExist(err) { os.MkdirAll(dir, 0700) }
        filename := path.Join(dir, trx.Guid + "-" + trx.Hash + "." + t.extension())
        file, err := os.OpenFile(filename, os.O_CREATE, 0700)
        if err != nil {
            logger.Error("create file err", zap.String("file", filename), zap.Error(err))
            return
        }

        read := 0
        for size := int(size); read < size; {
            num := len(buf)
            if size - read < num { num = size - read }
            b := buf[:num]
            if _, err := c.Read(b); err != nil {
                file.Close()
                logger.Error("read body err", zap.Int("read", read), zap.Int("size", size), zap.Error(err))
                os.Remove(filename)
                return
            }
            read += num
        }
        file.Close()
        logger.Debug("receive file success", zap.String("file", filename))

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