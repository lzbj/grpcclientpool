package connpool

import (
	"context"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestNewGrpcClientConnPool(t *testing.T) {
	p, err := NewGrpcClientConnPool(func(addr string, dalay time.Duration) (*grpc.ClientConn, error) {
		return &grpc.ClientConn{}, nil
	}, 16, "localhost:9091", 3*time.Second, 3*time.Second)

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

	ch1 := make(chan interface{}, 16)

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
	case ch1 <- new(interface{}):

	default:
		if a := p.AvailableGrpcClientConn(); a != 0 {
			t.Errorf("The pool available was %d but should be 0", a)
		}
	}

	for i := 0; i < 16; i++ {
		go func() {
			<-ch1
		}()
	}
}
