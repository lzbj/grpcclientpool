package grpcpool

import (
	"context"
	//"fmt"
	"fmt"
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
	defaultName = "world"
	runtimes    = 50
)

func TestNewGrpcClientConnPoolBasic(t *testing.T) {
	var poolSize = 16
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
	for i := 0; i < poolSize; i++ {
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
	for i := 0; i < poolSize; i++ {
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

	defer p.ClosePool()

}

// server is used to implement helloworld.GreeterServer.
type Server struct{}

// SayHello implements helloworld.GreeterServer
func (s *Server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func TestNewGrpcClientConnPool(t *testing.T) {
	t.Log("run test using our pool")
	poolSize := 50
	runtimes := 50
	routineNum := 50
	address := ":50052"

	p, _ := perfCommon(address, poolSize, runtimes, routineNum, false)
	if a := p.Available(); a != poolSize {
		t.Errorf("The pool available was %d but should be %d", a, poolSize)
	}
	t.Log("run test using our pool finished")
	defer p.ClosePool()

}

func TestNewGrpcClientConnPoolThroughput(t *testing.T) {
	t.Log("run test using our pool throughput")
	poolSize := 50
	runtimes := 50
	routineNum := 100000
	address := ":50054"
	p, _ := perfCommon(address, poolSize, runtimes, routineNum, false)

	if a := p.Available(); a != poolSize {
		t.Errorf("The pool available was %d but should be %d", a, poolSize)
	}
	t.Log("run test using our pool throughput finished")
	defer p.ClosePool()

}

func TestNewGrpcClientConnPoolReuse(t *testing.T) {
	t.Log("run test using our pool reuse")
	poolSize := 50
	runtimes := 50
	routineNum := 50
	address := ":50055"
	p, _ := perfCommon(address, poolSize, runtimes, routineNum, true)

	if a := p.Available(); a != poolSize {
		t.Errorf("The pool available was %d but should be %d", a, poolSize)
	}

	t.Log("run test using our pool reuse finished")
	defer p.ClosePool()

}

func TestNewGrpcClientConnPoolReuseThroughput(t *testing.T) {
	t.Log("run test using our pool reuse throughput")
	poolSize := 50
	runtimes := 50
	routineNum := 100000
	address := ":50053"
	p, _ := perfCommon(address, poolSize, runtimes, routineNum, true)

	if a := p.Available(); a != poolSize {
		t.Errorf("The pool available was %d but should be %d", a, poolSize)
	}
	t.Log("run test using our pool reuse throughput finished")
	defer p.ClosePool()

}

func TestNormalShortConnection(t *testing.T) {
	t.Log("run test using no pool")
	runtimes := 50
	routineNum := 50
	address := ":50057"
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
		for i := 0; i < routineNum; i++ {
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
	t.Log("run test using no pool finished")
}

func TestSystemPool(t *testing.T) {
	t.Log("run test using go buildin pool")

	address := ":50058"
	runtimes := 50
	routineNum := 50
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

	p := NewSyncPool(address, grpc.WithInsecure())

	var wg sync.WaitGroup

	for i := 0; i < runtimes; i++ {
		for i = 0; i < routineNum; i++ {
			wg.Add(1)
			go func(i int) {
				// Set up a connection to the server.
				conn := p.Get()
				gcon, ok := conn.(*grpc.ClientConn)
				if !ok {
					t.Fatal("convert error happend")
				}

				c := pb.NewGreeterClient(gcon)
				r, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: defaultName})
				if err != nil {
					t.Fatalf("did not connect: %v", err)
				}
				assert.Equal(t, r.Message, "Hello world")
				p.Put(conn)
				wg.Done()
			}(i + 1)

		}
		wg.Wait()

	}
	t.Log("run test using go buildin pool finished")
}

func perfCommon(address string, poolsize int, runtimes int, routinenumbers int, reuse bool) (*GrpcConnPool, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &Server{})
	// Register reflection service on gRPC server.
	reflection.Register(s)

	go func() {
		if err := s.Serve(lis); err != nil {
			fmt.Printf("failed to serve: %v", err)
		}
	}()

	p, err := NewGrpcConnPool(func() (*grpc.ClientConn, error) {
		return grpc.Dial(address, grpc.WithInsecure())
	}, poolsize, 3*time.Second, 1*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < runtimes; i++ {
		for i = 0; i < routinenumbers; i++ {
			wg.Add(1)
			go func(i int) {
				// Set up a connection to the server.
				var conn *grpcConn
				var err error
				if reuse {
					conn, err = p.GetReuse(context.Background())
				} else {
					conn, err = p.Get(context.Background())
				}

				if err != nil {
					fmt.Printf("Get returned an error : %s", err.Error())
				}
				if err != nil {
					fmt.Printf("did not connect: %v", err)
				}

				c := pb.NewGreeterClient(conn.ClientConn)
				_, err = c.SayHello(context.Background(), &pb.HelloRequest{Name: defaultName})
				if err != nil {
					fmt.Printf("did not connect: %v", err)
				}
				conn.Close()
				wg.Done()
			}(i + 1)

		}
		wg.Wait()
	}
	return p, nil
}
