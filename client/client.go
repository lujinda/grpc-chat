package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	grpclb "grpclb/etcdv3"
	"io"
	"log"
	"os"
	"sync"
	"time"
	"utils"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "grpclb/helloword"
)

var name *string = flag.String("name", "guess", "what's your name?")
var reg *string = flag.String("reg", "http://127.0.0.1:2479", "register etcd address")
var serv *string = flag.String("service", "chat_service", "service name")
var cert_file *string = flag.String("cert", "", "cert file")
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

type Robot struct {
	sync.Mutex
	conn        *grpc.ClientConn
	client      pb.GreeterClient
	chat_stream pb.Greeter_SayHelloClient
	ctx         context.Context
	cancel      context.CancelFunc
	token       string
}

func (robot *Robot) Cancel() {
	robot.cancel()
}

func (robot *Robot) Done() <-chan struct{} {
	return robot.ctx.Done()
}

func (robot *Robot) Connect() error {
	robot.Lock()
	defer robot.Unlock()

	if robot.conn != nil {
		robot.conn.Close()
	}
	r := grpclb.NewResolver(*serv)
	lb := grpc.RoundRobin(r)

	ctx, cancel := context.WithCancel(context.Background())
	robot.ctx = ctx
	robot.cancel = cancel

	options := []grpc.DialOption{
		grpc.WithDecompressor(grpc.NewGZIPDecompressor()),
		grpc.WithCompressor(grpc.NewGZIPCompressor()),
		grpc.WithBalancer(lb),
		grpc.WithBlock(),
		grpc.WithTimeout(10 * time.Second),
	}
	if *cert_file != "" {
		creds, err := credentials.NewClientTLSFromFile(*cert_file, "duoshoubang")
		utils.CheckErrorPanic(err)
		options = append(options, grpc.WithTransportCredentials(creds))
	} else {
		options = append(options, grpc.WithInsecure())
	}
	conn, err := grpc.DialContext(ctx, *reg, options...)
	utils.CheckErrorPanic(err)

	if err != nil {
		return errors.Wrap(err, "Client Connect")
	}

	client := pb.NewGreeterClient(conn)

	robot.conn = conn
	robot.client = client
	robot.chat_stream = nil
	return nil
}

func (robot *Robot) GetChatStream() pb.Greeter_SayHelloClient {
	robot.Lock()
	defer robot.Unlock()
	if robot.chat_stream != nil {
		return robot.chat_stream
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("token", robot.token))
	for {
		stream, err := robot.client.SayHello(ctx)
		if err != nil {
			fmt.Printf("get chat stream failed. %s", err.Error())
			time.Sleep(1 * time.Second)
		} else {
			robot.chat_stream = stream
			break
		}
	}

	return robot.chat_stream
}

func (robot *Robot) Login(username string) error {
	robot.Lock()
	defer robot.Unlock()
	reply, err := robot.client.Login(context.Background(), &pb.LoginRequest{
		Username: username,
	})
	if err != nil {
		return errors.Wrap(err, "Login")
	}
	robot.token = reply.GetToken()
	return nil
}

func NewRobot() *Robot {
	robot := &Robot{}
	utils.CheckErrorPanic(robot.Connect())

	return robot
}

func main() {
	flag.Parse()

	robot := NewRobot()
	utils.CheckErrorPanic(robot.Login(*name))
	ConsoleLog("登录成功")

	// 监听服务端通知
	go func() {
		var (
			reply *pb.HelloReply
			err   error
		)
		for {
			reply, err = robot.GetChatStream().Recv()
			reply_status, _ := status.FromError(err)
			if err != nil && reply_status.Code() == codes.Unavailable {
				ConsoleLog("与服务器的连接被断开, 进行重试")
				robot.Connect()
				ConsoleLog("重连成功")
				time.Sleep(time.Second)
				continue
			}
			utils.CheckErrorPanic(err)
			ConsoleLog(reply.Message)
			if reply.MessageType == pb.HelloReply_CONNECT_FAILED {
				log.Println("Connect failed.")
				robot.Cancel()
				break
			}
		}
	}()

	// 接受聊天信息并发送聊天内容
	go func() {
		var (
			line string
			err  error
		)
		for {
			line = Input("")
			if line == "exit" {
				robot.Cancel()
				break
			}
			err = robot.GetChatStream().Send(&pb.HelloRequest{
				Message: line,
			})
			fmt.Print("> ")
			if err != nil {
				ConsoleLog(fmt.Sprintf("there was error sending data. %s", err.Error()))
				continue
			}
		}
	}()
	<-robot.Done()

	fmt.Println("Bye")
}
