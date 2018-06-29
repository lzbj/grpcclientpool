package connpool

import (
	"context"
	"errors"
	"github.com/golang/groupcache/lru"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var Debug = true

// ErrorPoolSize means the initial size is wring
var ErrorPoolSize = errors.New("the size of grpc connection pool should be greater than 0")

// ErrorPoolFull means grpc conn pool is full
var ErrorPoolFull = errors.New("the grpc connection pool is already full")

// ErrorPoolClosed means grpc conn poll is closed
var ErrorPoolClosed = errors.New("the grpc connection pool is already closed")

// ErrorTimeout means grpc client conn pool timed out
var ErrorTimeout = errors.New("the grpc connection pool is timed out")

// ErrorConnClosed means grpc client conn is closed
var ErrorConnClosed = errors.New("the grpc connection pool is closed")

// ErrorCloseConn means error happened during close grpc conn.
var ErrCloseConn = errors.New("error happened during close grpc client connection")

// PoolCapacity set the default pool capacity
const PoolCapacity = 4 << 1

const length = 4 << 1

var letters = []rune("abcdefghijklmnopqrstuvwxyz")

// Grpc connection pool
type GrpcConnPool struct {
	conns       chan grpcConn
	creator     ClientConCreator
	idleTimeout time.Duration
	addr        string
	maxDelay    time.Duration
	mu          sync.RWMutex
	cache       *lru.Cache
}

// ClientConCreator is grpc ClientConn creator
type ClientConCreator func(addr string, delay time.Duration) (*grpc.ClientConn, error)

// ClientConHealthChecker is grpc ClientConn healthy checker
type ClientConHealthChecker func(ctx context.Context, in interface{}, opts ...grpc.CallOption) (interface{}, error)

// grpcCliConn wraps a grpc Client connection
type grpcConn struct {
	*grpc.ClientConn
	pool      *GrpcConnPool
	timeUsed  time.Time
	unhealthy bool
	addr      string
	maxDelay  time.Duration
	// TODO: Heathy checker: add grpcClientConn heartbeat or health check here.
	// checker CliConHealthChecker
}

// NewGrpcClientConnPool creates a pool holds a bunch of live grpc client conns.
func NewGrpcConnPool(creator ClientConCreator, size int, address string, maxDelay time.Duration, idleTimeout time.Duration) (*GrpcConnPool, error) {
	if size <= 0 {
		return nil, ErrorPoolSize
	}

	pool := &GrpcConnPool{
		conns:       make(chan grpcConn, size),
		creator:     creator,
		idleTimeout: idleTimeout,
		addr:        address,
		maxDelay:    maxDelay,
		cache:       lru.New(size),
	}
	for i := 0; i < size; i++ {
		cliconn, err := creator(address, maxDelay)
		if err != nil {
			return nil, err
		}
		con := grpcConn{
			ClientConn: cliconn,
			pool:       pool,
			timeUsed:   time.Now(),
			addr:       address,
			maxDelay:   maxDelay,
		}
		pool.conns <- con
		key := genconnkey(i, length)
		pool.cache.Add(key, con)
	}
	if Debug {
		log.Debug().Msgf("created a pool with the size %d ", size)
	}
	return pool, nil
}

func genconnkey(index int, length uint) string {
	bs := make([]rune, length)
	for i := range bs {
		bs[i] = letters[rand.Intn(len(letters))]
	}
	return strconv.Itoa(index) + string(bs)
}

// Size return the size of the pool.
func (p *GrpcConnPool) Size() int {
	if p.PoolIsClosed() {
		return 0
	}
	return cap(p.conns)
}

// AvailableGrpcClientConn returns the available conns.
func (p *GrpcConnPool) Available() int {
	if p.PoolIsClosed() {
		return 0
	}
	return len(p.conns)
}

// ClosePool closes pool.
func (p *GrpcConnPool) ClosePool() error {
	p.mu.Lock()
	clients := p.conns
	close(p.conns)
	p.mu.RUnlock()
	if clients == nil {
		return nil
	}
	close(clients)
	for i := 0; i < p.Size(); i++ {
		client := <-clients
		if client.ClientConn == nil {
			continue
		}
		err := client.ClientConn.Close()
		if err != nil {
			if Debug {
				log.Info().Msg("error happended when try to close grpc client connection")
			}
			return err
		}
	}
	return nil
}

// GetGrpcCliCon returns a *grpcClientConn instance.
func (p *GrpcConnPool) Get(ctx context.Context) (*grpcConn, error) {
	cliConns := p.getConn()
	if cliConns == nil {
		return nil, ErrorPoolClosed
	}

	wraper := grpcConn{
		pool: p,
	}
	select {
	case con := <-cliConns:
		wraper = con
	case <-ctx.Done():
		return nil, ErrorTimeout
	}

	idleTimeout := p.idleTimeout
	if wraper.ClientConn != nil && idleTimeout > 0 && wraper.timeUsed.Add(idleTimeout).Before(time.Now()) {
		wraper.ClientConn.Close()
		wraper.ClientConn = nil
	}

	var err error
	if wraper.ClientConn == nil {
		wraper.ClientConn, err = p.creator(wraper.addr, wraper.maxDelay)
		if err != nil {
			cliConns <- grpcConn{
				pool: p,
			}
		}
	}
	return &wraper, nil
}

func (p *GrpcConnPool) getConn() chan grpcConn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.conns
}

// PoolIsClosed return whether pool is closed.
func (p *GrpcConnPool) PoolIsClosed() bool {
	return p == nil || p.getConn() == nil
}

// PutBackGrpcCliConn put cli con back to pool.
func (cc *grpcConn) Close() error {

	if cc.ClientConn == nil {
		return ErrorConnClosed
	}

	if cc == nil {
		return nil
	}

	if cc.pool.PoolIsClosed() {
		return ErrorPoolClosed
	}

	clicon := grpcConn{
		pool:       cc.pool,
		ClientConn: cc.ClientConn,
		timeUsed:   time.Now(),
		addr:       cc.addr,
		maxDelay:   cc.maxDelay,
	}

	//TODO: Add more clicon healthy check here.
	if clicon.unhealthy {
		err := clicon.ClientConn.Close()
		if err != nil {
			return err
		}
		clicon.ClientConn = nil
	}

	select {
	case cc.pool.conns <- clicon:
	default:
		return ErrorPoolFull
	}
	return nil
}
