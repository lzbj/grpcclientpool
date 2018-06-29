package connpool

import (
	"context"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNewGrpcClientConnPool(t *testing.T) {
	pooSize := 16
	p, err := NewGrpcConnPool(func(addr string, dalay time.Duration) (*grpc.ClientConn, error) {
		return grpc.Dial(addr, grpc.WithInsecure())
	}, pooSize, "example.com", 3*time.Second, 3*time.Second)

	if err != nil {
		t.Errorf("The pool returned an error :%s", err.Error())
	}
	if a := p.Available(); a != pooSize {
		t.Errorf("The pool available should be 16 but %d", a)
	}

	client, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error : %s", err.Error())
	}
	if client == nil {
		t.Error("client was nil")
	}
	if a := p.Available(); a != pooSize-1 {
		t.Errorf("The pool available should be 15 but %d", a)
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Close returned an error: %s", err.Error())
	}

	if a := p.Available(); a != pooSize {
		t.Errorf("The pool available should be 16 but %d", a)
	}

	err = client.Close()
	if err != ErrorPoolFull {
		t.Errorf("Expected error \"%s\" but got \"%s\"",
			ErrorPoolFull.Error(), err.Error())
	}

	ch1 := make(chan *grpcConn, pooSize)

	var wg sync.WaitGroup

	//consume all
	for i := 0; i < pooSize; i++ {
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
	for i := 0; i < pooSize; i++ {
		wg.Add(1)
		go func() {
			conn := <-ch1
			conn.Close()
			wg.Done()
		}()
	}

	wg.Wait()

	if a := p.Available(); a != pooSize {
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
	poolSize := 100
	address := ":50052"
	var defaultName = "world"
	var i int
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

	p, err := NewGrpcConnPool(func(addr string, delay time.Duration) (*grpc.ClientConn, error) {
		return grpc.Dial(addr, grpc.WithInsecure())
	}, poolSize, address, 3*time.Second, 1*time.Second)

	var wg sync.WaitGroup

	for i = 0; i < poolSize; i++ {
		wg.Add(1)
		go func(i int) {
			// Set up a connection to the server.
			t.Logf("%s  %s %d\n", time.Now().Format(time.RFC3339), "connecting ", i)
			conn, err := p.Get(context.Background())
			if err != nil {
				t.Errorf("Get returned an error : %s", err.Error())
			}
			if err != nil {
				t.Fatalf("did not connect: %v", err)
			}

			c := pb.NewGreeterClient(conn.ClientConn)
			t.Logf("%s  %d %s\n", time.Now().Format(time.RFC3339), i, "connected")
			r, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: defaultName})
			if err != nil {
				t.Fatalf("did not connect: %v", err)
			}
			t.Logf("response :%s", r.Message)
			assert.Equal(t, r.Message, "Hello world")
			conn.Close()
			wg.Done()
		}(i + 1)

	}
	wg.Wait()

	go s.Stop()

	t.Log("run test ")
	if a := p.Available(); a != poolSize {
		t.Errorf("The pool available was %d but should be %d", a, poolSize)
	}
}
