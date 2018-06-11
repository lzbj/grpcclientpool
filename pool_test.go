package connpool

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
	"net"
	"testing"
	"time"
)

func TestNewGrpcClientConnPool(t *testing.T) {
	p, err := NewGrpcClientConnPool(func(addr string, dalay time.Duration) (*grpc.ClientConn, error) {
		return grpc.Dial(addr, grpc.WithInsecure())
	}, 16, "example.com", 3*time.Second, 3*time.Second)

	if err != nil {
		t.Errorf("The pool returned an error :%s", err.Error())
	}

	if a := p.AvailableGrpcClientConn(); a != 16 {
		t.Errorf("The pool available should be 16 but %d", a)
	}

	client, err := p.GetGrpcCliCon(context.Background())
	if err != nil {
		t.Errorf("Get returned an error : %s", err.Error())
	}
	if client == nil {
		t.Error("client was nil")
	}
	if a := p.AvailableGrpcClientConn(); a != 15 {
		t.Errorf("The pool available should be 15 but %d", a)
	}

	err = client.PutBackGrpcClientConn()
	if err != nil {
		t.Errorf("Close returned an error: %s", err.Error())
	}

	if a := p.AvailableGrpcClientConn(); a != 16 {
		t.Errorf("The pool available should be 16 but %d", a)
	}

	err = client.PutBackGrpcClientConn()
	if err != ErrorConnClosed {
		t.Errorf("Expected error \"%s\" but got \"%s\"",
			ErrorConnClosed.Error(), err.Error())
	}

	ch1 := make(chan *grpcClientConn, 16)

	//consume all
	for i := 0; i < 16; i++ {
		go func() {
			client, err := p.GetGrpcCliCon(context.Background())
			if err != nil {
				t.Errorf("Err was not nil: %s", err.Error())
			}

			ch1 <- client
		}()

	}

	select {
	case ch1 <- &grpcClientConn{}:

	default:
		if a := p.AvailableGrpcClientConn(); a != 0 {
			t.Errorf("The pool available was %d but should be 0", a)
		}
	}

	//return all
	for i := 0; i < 16; i++ {
		go func() {
			conn := <-ch1
			conn.PutBackGrpcClientConn()
		}()
	}

	select {

	case <-ch1:
	default:
		if a := p.AvailableGrpcClientConn(); a != 16 {
			t.Errorf("The pool available was %d but should be 0", a)
		}

	}

}

// server is used to implement helloworld.GreeterServer.
type Server struct{}

// SayHello implements helloworld.GreeterServer
func (s *Server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func TestTestNewGrpcClientConnPoolSystem(t *testing.T) {
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
		select {
		case <-time.Tick(5 * time.Second):
			s.Stop()
		}

	}()
	if err := s.Serve(lis); err != nil {
		t.Fatalf("failed to serve: %v", err)
	}

	p, err := NewGrpcClientConnPool(func(addr string, delay time.Duration) (*grpc.ClientConn, error) {
		return grpc.Dial(addr, grpc.WithInsecure())
	}, 16, address, 3*time.Second, 1*time.Second)
	for i = 0; i < p.AvailableGrpcClientConn(); i++ {
		go func() {
			// Set up a connection to the server.
			fmt.Printf("%s  %s\n", time.Now().Format(time.RFC3339), "connecting")
			conn, err := p.GetGrpcCliCon(context.Background())
			if err != nil {
				t.Errorf("Get returned an error : %s", err.Error())
			}
			if err != nil {
				t.Fatalf("did not connect: %v", err)
			}

			c := pb.NewGreeterClient(conn.ClientConn)
			fmt.Printf("%s  %s\n", time.Now().Format(time.RFC3339), "connected")
			r, err := c.SayHello(context.Background(), &pb.HelloRequest{Name: defaultName})
			if err != nil {
				t.Fatalf("did not connect: %v", err)
			}
			t.Logf("response :%s", r.Message)
			assert.Equal(t, r.Message, "hell world")
			conn.PutBackGrpcClientConn()
		}()

	}
	select {
	case p.getGrpcClientConn() <- grpcClientConn{}:

	default:
		if a := p.AvailableGrpcClientConn(); a != 16 {
			t.Errorf("The pool available was %d but should be 16", a)
		}

	}

}
