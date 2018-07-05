package grpcpool

import (
	"context"
	//"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
	"net"
	"sync"
	"testing"
	"time"
)

const (
	address     = ":50052"
	defaultName = "world"
	runtimes    = 50
)

func TestNewGrpcClientConnPool(t *testing.T) {
	var poolSize int32
	poolSize = 16
	p, err := NewGrpcConnPool(func() (*grpc.ClientConn, error) {
		return grpc.Dial("example.com", grpc.WithInsecure())
	}, poolSize, 3*time.Second, 3*time.Second)

	if err != nil {
		t.Errorf("The pool returned an error :%s", err.Error())
	}
	if a := p.Available(); a != poolSize {
		t.Errorf("The pool available should be 16 but %d", a)
	}

	client, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error : %s", err.Error())
	}
	if client == nil {
		t.Error("client was nil")
	}
	if a := p.Available(); a != poolSize-1 {
		t.Errorf("The pool available should be 15 but %d", a)
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Close returned an error: %s", err.Error())
	}

	if a := p.Available(); a != poolSize {
		t.Errorf("The pool available should be 16 but %d", a)
	}

	err = client.Close()
	if err != ErrorConnAlreadyClosed {
		t.Errorf("Expected error \"%s\" but got \"%s\"",
			ErrorConnAlreadyClosed.Error(), err.Error())
	}

	ch1 := make(chan *grpcConn, poolSize)

	var wg sync.WaitGroup

	//consume all
	var i int32
	for i = 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			client, err := p.Get(context.Background())
			if err != nil {
				t.Errorf("Err was not nil: %s", err.Error())
			}

			ch1 <- client
			wg.Done()
		}()

	}

	wg.Wait()

	if a := p.Available(); a != 0 {
		t.Errorf("The pool available was %d but should be 0", a)
	}
	//return all
	for i = 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			conn := <-ch1
			conn.Close()
			wg.Done()
		}()
	}

	wg.Wait()

	if a := p.Available(); a != poolSize {
		t.Errorf("The pool available was %d but should be 0", a)
	}

}

// server is used to implement helloworld.GreeterServer.
type Server struct{}

// SayHello implements helloworld.GreeterServer
func (s *Server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func TestNewGrpcClientConnPoolSystem(t *testing.T) {
	var poolSize int32
	poolSize = 50

	lis, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &Server{})
	// Register reflection service on gRPC server.
	reflection.Register(s)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatalf("failed to serve: %v", err)
		}
	}()

	p, err := NewGrpcConnPool(func() (*grpc.ClientConn, error) {
		return grpc.Dial(address, grpc.WithInsecure())
	}, poolSize, 3*time.Second, 1*time.Second)

	var wg sync.WaitGroup
	var i int32
	for i = 0; i < runtimes; i++ {
		for i = 0; i < poolSize; i++ {
			wg.Add(1)
			go func(i int32) {
				// Set up a connection to the server.
				conn, err := p.Get(context.Background())
				if err != nil {
					t.Errorf("Get returned an error : %s", err.Error())
				}
				if err != nil {
					t.Fatalf("did not connect: %v", err)
				}

				c := pb.NewGreeterClient(conn.ClientConn)
				r, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: defaultName})
				if err != nil {
					t.Fatalf("did not connect: %v", err)
				}
				assert.Equal(t, r.Message, "Hello world")
				conn.Close()
				wg.Done()
			}(i + 1)

		}
		wg.Wait()

		t.Log("run test ")
		if a := p.Available(); a != poolSize {
			t.Errorf("The pool available was %d but should be %d", a, poolSize)
		}

	}

	defer p.ClosePool()

}

func TestNormalShortConnection(t *testing.T) {
	concurrentNumber := 50
	address := ":50053"
	lis, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &Server{})
	// Register reflection service on gRPC server.
	reflection.Register(s)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatalf("failed to serve: %v", err)
		}
	}()

	for i := 0; i < runtimes; i++ {
		var wg sync.WaitGroup
		for i := 0; i < concurrentNumber; i++ {
			wg.Add(1)
			go func() {
				// Set up a connection to the server.
				conn, err := grpc.Dial(address, grpc.WithInsecure())
				if err != nil {
					t.Fatalf("did not connect: %v", err)
				}
				defer conn.Close()
				c := pb.NewGreeterClient(conn)
				_, err = c.SayHello(context.Background(), &pb.HelloRequest{Name: defaultName})
				if err != nil {
					t.Logf("greeting:%s", err)
				}
				wg.Done()
			}()

		}
		wg.Wait()
	}
}
