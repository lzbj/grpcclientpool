package grpcpool

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"sync"
	"sync/atomic"
	"time"
	"wheels/log"
)

var debug = false

// ErrorPoolSize means the initial size is wring
var ErrorPoolSize = errors.New("the size of grpc connection pool should be greater than 0")

// ErrorPoolFull means grpc conn pool is full
var ErrorPoolAlreadyFull = errors.New("the grpc connection pool is already full")

// ErrorPoolClosed means grpc conn poll is closed
var ErrorPoolAlreadyClosed = errors.New("the grpc connection pool is already closed")

// ErrorTimeout means grpc client conn pool timed out
var ErrorTimeout = errors.New("the grpc connection pool is timed out")

// ErrorConnClosed means grpc client conn is closed
var ErrorConnAlreadyClosed = errors.New("the grpc connection is closed")

// ErrorCloseConn means error happened during close grpc conn.
var ErrCloseConn = errors.New("error happened during close grpc client connection")

var addr string
var options []grpc.DialOption

var reuseQuota int32 = 100000
var counter int32
var globalconn = &grpcConn{}

// Grpc connection pool
type GrpcConnPool struct {
	conns           chan *grpcConn
	creator         ClientConCreator
	idleTimeout     time.Duration
	maxLifeDuration time.Duration
	mu              sync.RWMutex
}

// ClientConCreator is grpc ClientConn creator
type ClientConCreator func() (*grpc.ClientConn, error)

// grpcCliConn wraps a grpc Client connection
type grpcConn struct {
	*grpc.ClientConn
	pool        *GrpcConnPool
	timeUsed    time.Time
	unhealthy   bool
	timeCreated time.Time
}

// NewGrpcClientConnPool creates a pool holds a bunch of live grpc client conns.
func NewGrpcConnPool(creator ClientConCreator, size int, idleTimeout time.Duration, maxLifeDuration time.Duration) (*GrpcConnPool, error) {
	if size <= 0 {
		return nil, ErrorPoolSize
	}

	pool := &GrpcConnPool{
		conns:           make(chan *grpcConn, size),
		creator:         creator,
		idleTimeout:     idleTimeout,
		maxLifeDuration: maxLifeDuration,
	}

	for i := 0; i < size; i++ {

		cliconn, err := creator()
		if err != nil {
			if debug {
				log.Debug("error happened during create grpc client connection")
				log.Errorf("%v", err)
				//Insert some placeholder
				pool.conns <- &grpcConn{
					pool: pool,
				}
			}
		}
		con := &grpcConn{
			ClientConn:  cliconn,
			pool:        pool,
			timeUsed:    time.Now(),
			timeCreated: time.Now(),
		}
		pool.conns <- con

	}

	if debug {
		log.Debug("created a pool with the size %d ", size)
	}
	return pool, nil
}

// Size return the size of the pool.
func (p *GrpcConnPool) Size() int {

	if p.PoolIsClosed() {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return cap(p.conns)

}

// AvailableGrpcClientConn returns the available conns.
func (p *GrpcConnPool) Available() int {

	if p.PoolIsClosed() {
		return 0
	}
	return len(p.conns)
}

func (p *GrpcConnPool) GetReuse(ctx context.Context) (*grpcConn, error) {
	if globalconn != nil && globalconn.ClientConn != nil {
		return globalconn, nil
	}
	con, err := p.Get(ctx)
	if err != nil {
		return nil, err
	}
	atomic.AddInt32(&counter, 1)
	if counter <= reuseQuota {
		globalconn = con
		return globalconn, nil
	}
	//exceeds the reuse limit
	//put the old back and create new one
	atomic.AddInt32(&counter, -reuseQuota)
	err = globalconn.Close()
	if err != nil {
		return nil, err
	}
	//get a new one
	con, err = p.Get(ctx)
	if err != nil {
		return nil, err
	}
	atomic.AddInt32(&counter, 1)
	globalconn = con
	return globalconn, err

}

// GetGrpcCliCon returns a *grpcClientConn instance.
func (p *GrpcConnPool) Get(ctx context.Context) (*grpcConn, error) {
	cliConns := p.getConn()
	if cliConns == nil {
		return nil, ErrorPoolAlreadyClosed
	}

	wraper := &grpcConn{
		pool: p,
	}

	select {
	case wraper = <-cliConns:
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
		go func() (*grpcConn, error) {
			wraper.ClientConn, err = p.creator()
			if err != nil {
				if debug {
					log.Debug("error happened during create grpc client connection")
				}
				select {
				case cliConns <- &grpcConn{
					pool: p,
				}:
				default:
					return nil, err
				}

				return nil, err
			}
			return wraper, nil
		}()

	}
	return wraper, nil
}

func (p *GrpcConnPool) getConn() chan *grpcConn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.conns
}

// PoolIsClosed return whether pool is closed.
func (p *GrpcConnPool) PoolIsClosed() bool {
	return p == nil || p.getConn() == nil
}

// ClosePool close all client Connections.
func (p *GrpcConnPool) ClosePool() error {
	if p.PoolIsClosed() {
		return ErrorPoolAlreadyClosed
	}

	conns := p.conns
	p.conns = nil

	if conns == nil {
		return nil
	}

	close(conns)
	for con := range conns {
		if con.ClientConn == nil {
			continue
		}
		err := con.ClientConn.Close()
		if err != nil {
			log.Debug("error happened during close client connection")
			return ErrCloseConn
		}
	}

	return nil

}

// PutBackGrpcCliConn put cli con back to pool.
func (cc *grpcConn) Close() error {

	if cc == nil {
		return nil
	}

	if cc.ClientConn == nil {
		return ErrorConnAlreadyClosed
	}

	if cc.pool.PoolIsClosed() {
		return ErrorPoolAlreadyClosed
	}

	// clone the resources.
	clicon := &grpcConn{
		pool:       cc.pool,
		ClientConn: cc.ClientConn,
		timeUsed:   time.Now(),
	}

	if cc.unhealthy {
		err := clicon.ClientConn.Close()
		if err != nil {
			return err
		}
		clicon.ClientConn = nil
	}

	select {
	case cc.pool.conns <- clicon:
	default:
		return ErrorPoolAlreadyFull
	}

	cc.ClientConn = nil
	return nil
}

func NewSyncPool(address string, opts ...grpc.DialOption) *sync.Pool {
	addr = address
	options = opts
	p := &sync.Pool{
		New: CreateConn,
	}
	return p
}

func CreateConn() interface{} {
	con, err := grpc.Dial(addr, options...)
	if err != nil {
		return nil
	}
	return con
}
