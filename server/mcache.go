package server

import (
    "bytes"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "go.uber.org/zap"
    "io"
    "os"
    "sort"
    "sync"
    "time"
    "unsafe"
)

type File struct {
    uuid string
    name string
    m    *bytes.Buffer
    f    *os.File
    w    io.Writer
    r    io.Reader
}

func (f *File) Read(p []byte) (int, error) {
    if f.r == nil { if f.m != nil {f.r = f.m} else {f.r = f.f} }
    return f.r.Read(p)
}

func (f *File) Write(p []byte) (int, error) {
    if f.w == nil {
        var w []io.Writer
        if f.m != nil { w = append(w, f.m) }
        if f.f != nil { w = append(w, f.f) }
        f.w = io.MultiWriter(w...)
    }
    return f.w.Write(p)
}

func (f *File) Close() error {
    defer func() {
        if err := f.tryCache(); err != nil && f.m != nil {
            f.m.Reset()
            mcache.pool.Put(f.m)
            logger.Debug("pool", zap.Uintptr("put", uintptr(unsafe.Pointer(f.m))))
            f.m = nil
        }
    }()
    if f.f != nil { return f.f.Close() }
    return nil
}

func (f *File) tryCache() error {
    if f.m != nil && f.f != nil {
        if s, err := os.Stat(f.name); err == nil {
            if s.Size() == int64(f.m.Len()) {
                mcache.core.put(f.uuid, f.m)
                return nil
            } else {return mcache.errors.incomplete}
        } else {return err}
    }
    return mcache.errors.unavailable
}

func (f *File) Name() string { return f.name }

type memEntity struct {
    data *bytes.Buffer
    uuid string
    size int64
    hit  int
    sha  string
    ts   int64
}

type memCache struct {
    capacity int64
    lookups  map[string]*memEntity
    library  []*memEntity
    size     int64
}

func (m *memCache) remove(uuid string) {
    if entity, ok := m.lookups[uuid]; ok {
        delete(m.lookups, entity.uuid)
        m.size -= int64(entity.data.Cap())
        for i := 0; i < len(m.library); i++ {
            e := m.library[i]
            if e.uuid == uuid {
                m.library = append(m.library[:i], m.library[i+1:]...)
                break
            }
        }
    }
}

func (m *memCache) put(uuid string, data *bytes.Buffer) {
    h := sha256.Sum256(data.Bytes())
    s := hex.EncodeToString(h[:])
    logger.Debug("mcache", zap.String("put", uuid), zap.String("sha", s), zap.Int("size", data.Len()), zap.Uintptr("ptr", uintptr(unsafe.Pointer(data))))
    m.remove(uuid) /* clean up old one */
    entity := &memEntity{uuid: uuid, data: data, size: int64(data.Len()), ts: time.Now().UnixNano(), sha: s}
    m.lookups[uuid] = entity
    m.library = append(m.library, entity)
    m.size += int64(data.Cap())
    if m.capacity < m.size {
        sort.Slice(m.library, func(i, j int) bool {
            ei := m.library[i]
            ej := m.library[j]
            if ei.hit != ej.hit { return ei.hit < ej.hit }
            return ei.ts < ej.ts
        })
        for i := 0; i < len(m.library); i++ {
            entity := m.library[i]
            size := int64(entity.data.Cap())
            if m.size - size > m.capacity {
                delete(m.lookups, entity.uuid)
                m.library = append(m.library[:i], m.library[i+1:]...)
                m.size -= size
                i--
            } else { break }
        }
    }
}

func (m *memCache) get(uuid string) (*bytes.Buffer, error) {
    if entity, ok := m.lookups[uuid]; ok {
        entity.hit++
        h := sha256.Sum256(entity.data.Bytes())
        s := hex.EncodeToString(h[:])
        if s != entity.sha {
            if f, err := os.OpenFile(uuid, os.O_CREATE|os.O_WRONLY, 0x700); err == nil {
                f.Write(entity.data.Bytes())
            }
        }
        logger.Debug("mcache", zap.String("get", uuid), zap.String("sha", s),
            zap.Uintptr("ptr", uintptr(unsafe.Pointer(entity.data))),
            zap.Int("size", entity.data.Len()),
            zap.Int("data", int(entity.size)),
            zap.Int("cap", entity.data.Cap()))
        return bytes.NewBuffer(entity.data.Bytes()), nil
    }
    return nil, mcache.errors.unavailable
}

var mcache struct {
    core   memCache
    pool   *sync.Pool
    limit  int64
    errors struct {
        unavailable error
        incomplete  error
    }
}

func init() {
    mcache.limit = 2 << 20 // 2M
    mcache.pool = &sync.Pool{
        New: func() interface {} { return new(bytes.Buffer) },
    }
    mcache.errors.unavailable = errors.New("not available for caching")
    mcache.errors.incomplete = errors.New("incomplete")
    mcache.core.lookups = make(map[string]*memEntity)
}

func Open(name string, uuid string) (*File, error) {
    if mcache.core.capacity > 0 {
        if data, err := mcache.core.get(uuid); err == nil {
            return &File{m: data, uuid: uuid}, nil
        }
    }
    file, err := os.Open(name)
    if err != nil {return nil, err}
    f := &File{f: file, name: name, uuid: uuid}
    if s, err := file.Stat(); err == nil && s.Size() < mcache.limit { f.m = mcache.pool.Get().(*bytes.Buffer) }
    return f, nil
}

func NewFile(name string, uuid string, size int64) (*File, error) {
    file, err := os.OpenFile(name, os.O_CREATE | os.O_WRONLY, 0700)
    if err != nil {return nil, err}
    f := &File{f: file, name: name, uuid: uuid}
    if mcache.core.capacity > 0 && size < mcache.limit { f.m = mcache.pool.Get().(*bytes.Buffer) }
    return f, nil
}
