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

var Debug = true

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

// ErrorCreateConn means error happened during create grpc conn.
var ErrConnCreation = errors.New("error happened during create grpc client connection")

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
func NewGrpcConnPool(creator ClientConCreator, size int32, idleTimeout time.Duration, maxLifeDuration time.Duration) (*GrpcConnPool, error) {
	if size <= 0 {
		return nil, ErrorPoolSize
	}

	pool := &GrpcConnPool{
		conns:           make(chan *grpcConn, size),
		creator:         creator,
		idleTimeout:     idleTimeout,
		maxLifeDuration: maxLifeDuration,
	}

	var i int32
	for i = 0; i < size; i++ {

		cliconn, err := creator()
		if err != nil {
			if Debug {
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

	if Debug {
		log.Debug("created a pool with the size %d ", size)
	}
	return pool, nil
}

// Size return the size of the pool.
func (p *GrpcConnPool) Size() int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	size := int32(cap(p.conns))

	if p.PoolIsClosed() {
		return 0
	}
	return atomic.LoadInt32(&size)

}

// AvailableGrpcClientConn returns the available conns.
func (p *GrpcConnPool) Available() int32 {

	if p.PoolIsClosed() {
		return 0
	}
	avai := int32(len(p.conns))
	return atomic.LoadInt32(&avai)
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
				if Debug {
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
	var i int32
	for i = 0; i < p.Available(); i++ {
		con := <-conns
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
