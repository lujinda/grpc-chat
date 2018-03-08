package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
	"utils"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "grpclb/helloword"
)

var name *string = flag.String("name", "guess", "what's your name?")
var address *string = flag.String("address", "127.0.0.1:8881", "server address")
var mutex sync.Mutex

func ConsoleLog(message string) {
	mutex.Lock()
	defer mutex.Unlock()
	fmt.Printf("\n------ %s -----\n%s\n> ", time.Now(), message)
}

func Input(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	line, _, err := reader.ReadLine()
	if err != nil {
		if err == io.EOF {
			return ""
		} else {
			panic(errors.Wrap(err, "Input"))
		}
	}
	return string(line)
}

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*address, grpc.WithInsecure(),
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithTimeout(time.Second*10))
	utils.CheckErrorPanic(errors.WithMessage(err, "grpc.Dial"))

	client := pb.NewGreeterClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("name", *name))
	stream, err := client.SayHello(ctx)
	utils.CheckErrorPanic(err)

	connected := make(chan bool)
	// 监听服务端通知
	go func() {
		var (
			reply *pb.HelloReply
			err   error
		)
		for {
			reply, err = stream.Recv()
			utils.CheckErrorPanic(err)
			ConsoleLog(reply.Message)
			if reply.MessageType == pb.HelloReply_CONNECT_FAILED {
				cancel()
				break
			}
			if reply.MessageType == pb.HelloReply_CONNECT_SUCCESS {
				connected <- true
			}
		}
	}()

	go func() {
		<-connected
		var (
			line string
			err  error
		)
		for {
			line = Input("")
			if line == "exit" {
				cancel()
				break
			}
			err = stream.Send(&pb.HelloRequest{
				Message: line,
			})
			fmt.Print("> ")
			utils.CheckErrorPanic(err)
		}
	}()

	<-ctx.Done()
	fmt.Println("Bye")
}
