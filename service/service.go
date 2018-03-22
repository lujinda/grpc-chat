/*
 *
 *     Author        : tuxpy
 *     Email         : q8886888@qq.com.com
 *     Create time   : 3/7/18 9:18 AM
 *     Filename      : service.go
 *     Description   :
 *
 *
 */

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	grpclb "grpclb/etcdv3"
	pb "grpclb/helloword"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"utils"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Service implements grpclib/helloword.GreeterServer
type Service struct{}

// ConnectPool 管理本地的stream池. 该结构下的操作是线程安全的
type ConnectPool struct {
	sync.Map
}

// RemoteCommand 一个remote channel传输的数据
type RemoteCommand struct {
	Command string
	Args    map[string]string
}

// RemoteChannel 使用etcd3的PUT和watch实现远程channel.
type RemoteChannel struct {
	In  chan RemoteCommand
	Out chan RemoteCommand
	cli *clientv3.Client
}

// SessionManager 利用etcd3实现的session集群化管理器
type SessionManager struct {
	cli *clientv3.Client
}

// Session 将在Marshal成bytes存储为etcd3中
type Session struct {
	Name  string
	Token string
}

var connect_pool *ConnectPool
var remote_channel *RemoteChannel

var session_manger *SessionManager

// ReadyBroadCast 将消息广播给其他service. 因为做了负载均衡，一个client stream有可能落在不同节点，需要将行为广播给所有的节点
func ReadyBroadCast(from, message string) {
	remote_channel.Out <- RemoteCommand{
		Command: "broadcast",
		Args: map[string]string{
			"from":    from,
			"message": message,
		},
	}
}

// Get 根据token获取出一个Session
// 每次获取一个Session, 就将它的ttl刷新一遍
func (sm *SessionManager) Get(token string) (*Session, error) {
	key := fmt.Sprintf("%s/%s/session/%s", grpclb.Prefix, *srv, token)
	resp, err := sm.cli.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}
	kv := resp.Kvs[0]
	session := &Session{}
	err = json.Unmarshal(kv.Value, session)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal session data")
	}

	_, err = sm.cli.KeepAliveOnce(context.Background(), clientv3.LeaseID(kv.Lease))
	utils.CheckErrorPanic(err)

	return session, nil
}

// GetFromContext 根据一个grpc的context获取出Session.
// 本质上是根据ctx创建一个md, 然后获取token, 再调用SessionManager.Get
func (sm *SessionManager) GetFromContext(ctx context.Context) (*Session, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	tokens := md["token"]
	if len(tokens) == 0 {
		return nil, errors.New("Miss token")
	}

	return sm.Get(tokens[0])
}

// New 根据用户名新建一个Session
func (sm *SessionManager) New(name string) (*Session, error) {
	buf := make([]byte, 16)
	io.ReadFull(rand.Reader, buf)
	token := hex.EncodeToString(buf)

	key := fmt.Sprintf("%s/%s/session/%s", grpclb.Prefix, *srv, token)
	grant, err := sm.cli.Grant(context.Background(), 60*5) // token有效期5分钟
	if err != nil {
		return nil, errors.Wrap(err, "grant etcd lease ")
	}

	session := &Session{
		Name:  name,
		Token: token,
	}
	buf, err = json.Marshal(session)

	_, err = sm.cli.Put(context.Background(), key, string(buf), clientv3.WithLease(grant.ID))
	if err != nil {
		return nil, errors.Wrap(err, "etcd3 put")
	}

	return session, nil
}

// Get 根据用户名获取出该用户的stream
func (p *ConnectPool) Get(name string) pb.Greeter_SayHelloServer {
	if stream, ok := p.Load(name); ok {

		return stream.(pb.Greeter_SayHelloServer)
	} else {
		return nil
	}
}

// Add 添加一个stream
func (p *ConnectPool) Add(name string, stream pb.Greeter_SayHelloServer) {
	p.Store(name, stream)
}

// Del 删除一个stream
func (p *ConnectPool) Del(name string) {
	p.Delete(name)
}

// Broadcast 向连接上自己的所有客户端发送信息(除了发送者)
func (p *ConnectPool) BroadCast(from, message string) {
	log.Printf("BroadCast from: %s, message: %s\n", from, message)
	p.Range(func(username_i, stream_i interface{}) bool {
		username := username_i.(string)
		stream := stream_i.(pb.Greeter_SayHelloServer)
		if username == from {
			return true

		} else {
			log.Printf("From %s to %s\n", from, username)
			utils.CheckErrorPanic(stream.Send(&pb.HelloReply{
				Message:     message,
				MessageType: pb.HelloReply_NORMAL_MESSAGE,
				TS:          &timestamp.Timestamp{Seconds: time.Now().Unix()},
			}))
		}
		return true
	})
}

// Login 登录用户, 返回token给客户端, 并广播给所有已连接的客户一条欢迎的消息
func (s *Service) Login(ctx context.Context, in *pb.LoginRequest) (*pb.LoginReply, error) {
	// 当同一个用户名在多台节点上登录了, 就会出来一个聊天室中有多个同名的用户.
	// 可以在etcd中以用户名为主来创建一个key, 退出时删除该key来防止用户名大多节点中重复
	if connect_pool.Get(in.GetUsername()) != nil {
		return nil, errors.Errorf("username %s already exists", in.GetUsername())
	}
	session, err := session_manger.New(in.GetUsername())
	if err != nil {
		return nil, err
	}
	ReadyBroadCast(in.GetUsername(), fmt.Sprintf("Welcome %s!", in.GetUsername()))

	return &pb.LoginReply{
		Token:   session.Token,
		Success: true,
		Message: "success",
	}, nil
}

// SayHello 和客户端的stream进行通信
func (s *Service) SayHello(stream pb.Greeter_SayHelloServer) error {
	var (
		session *Session
		err     error
	)

	stream.Context().Value("session")
	peer, _ := peer.FromContext(stream.Context())
	log.Printf("Received new connection.  %s", peer.Addr.String())
	session, err = session_manger.GetFromContext(stream.Context()) // context中带有token, 取出session

	if err != nil {
		stream.Send(&pb.HelloReply{
			Message:     err.Error(),
			MessageType: pb.HelloReply_CONNECT_FAILED,
		})
		return nil
	}
	username := session.Name

	connect_pool.Add(username, stream)
	stream.Send(&pb.HelloReply{
		Message:     fmt.Sprintf("Connect success!"),
		MessageType: pb.HelloReply_CONNECT_SUCCESS,
	}) // 发送连接成功的提醒
	go func() {
		<-stream.Context().Done()
		connect_pool.Del(username) // 用户离开聊天室时, 从连接池中删除它
		ReadyBroadCast(username, fmt.Sprintf("%s leval room", username))
	}()
	for {
		req, err := stream.Recv()
		if err != nil {
			break
		}
		ReadyBroadCast(username, fmt.Sprintf("%s: %s", username, req.Message))
	}
	return nil
}

var (
	srv       = flag.String("service", "chat_service", "service name")
	port      = flag.Int("port", 8880, "listening port")
	reg       = flag.String("reg", "http://127.0.0.1:2479", "register etcd address")
	cert_file = flag.String("cert", "", "cert file")
	key_file  = flag.String("key", "", "key file")
)

// GetListen 获取服务端监听的地址
func GetListen() string {
	return fmt.Sprintf("0.0.0.0:%d", *port)
}

// NewSessionManager 根据etcd创建SessionManager
func NewSessionManager(cli *clientv3.Client) *SessionManager {
	return &SessionManager{
		cli: cli,
	}
}

// NewRemoteChannel 根据etcd创建remote channel
func NewRemoteChannel(cli *clientv3.Client) *RemoteChannel {
	qc := &RemoteChannel{
		cli: cli,
		In:  make(chan RemoteCommand, 1),
		Out: make(chan RemoteCommand, 1),
	}

	go func() {
		var command RemoteCommand
		var channel string = fmt.Sprintf("%s/%s/channel/remote", grpclb.Prefix, *srv)
		var buf bytes.Buffer
		var err error

		var dec *gob.Decoder

		rch := qc.cli.Watch(context.Background(), channel)
		for wresp := range rch {
			for _, ev := range wresp.Events {
				buf.Reset()
				dec = gob.NewDecoder(&buf)
				switch ev.Type {
				case mvccpb.PUT:
					buf.Write(ev.Kv.Value)
					err = dec.Decode(&command)
					if err != nil {
						log.Printf("recv an error message. %s\n", err.Error())
					} else {
						qc.In <- command
					}
				}
			}
		}
	}()

	go func() {
		var command RemoteCommand
		var channel string = fmt.Sprintf("%s/%s/channel", grpclb.Prefix, *srv)
		var buf bytes.Buffer
		var enc *gob.Encoder

		for {
			buf.Reset()
			enc = gob.NewEncoder(&buf)
			command = <-qc.Out
			utils.CheckErrorPanic(enc.Encode(command))
			qc.cli.Put(context.Background(),
				channel,
				buf.String())
		}
	}()

	return qc
}

// NewEtcd3Client 根据命令行参数--reg, 创建etcd3客户端
func NewEtcd3Client() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(*reg, ","),
	})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Create etcd3 client failed: %s", err.Error()))
	}

	return cli, nil
}

func StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if _, err := session_manger.GetFromContext(ss.Context()); err != nil {
		return status.Error(codes.Unauthenticated,
			fmt.Sprintf("Authenticated failed(%s)", err.Error()))
	}
	return handler(srv, ss)
}

func main() {
	var err error
	flag.Parse()
	connect_pool = &ConnectPool{}
	etcd_cli, err := NewEtcd3Client()

	utils.CheckErrorPanic(err)

	remote_channel = NewRemoteChannel(etcd_cli)
	session_manger = NewSessionManager(etcd_cli)

	go func() {
		var command RemoteCommand
		for command = range remote_channel.In {
			switch command.Command {
			case "broadcast":
				connect_pool.BroadCast(command.Args["from"], command.Args["message"])
			}
		}
	}()

	lis, err := net.Listen("tcp", GetListen())
	utils.CheckErrorPanic(err)
	fmt.Println("Listen on", GetListen())

	err = grpclb.Register(*srv, "127.0.0.1", *port, *reg, time.Second*3, 15) // 注册当前节点到etcd
	utils.CheckErrorPanic(err)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	go func() {
		s := <-ch
		log.Printf("receive signal '%v'\n", s)

		grpclb.UnRegister() // 程序被退出后，主动去unregister
		signal.Stop(ch)

		switch s := s.(type) {
		case syscall.Signal:
			syscall.Kill(os.Getpid(), s)

		default:
			os.Exit(1)
		}
	}()
	options := []grpc.ServerOption{
		grpc.StreamInterceptor(StreamInterceptor),
		grpc.RPCCompressor(grpc.NewGZIPCompressor()),
		grpc.RPCDecompressor(grpc.NewGZIPDecompressor()),
	}
	if *cert_file != "" {
		creds, err := credentials.NewServerTLSFromFile(*cert_file, *key_file)
		if err != nil {
			log.Fatalf("NewServerTLSFromFile has error. %s\n", err.Error())
		}
		options = append(options, grpc.Creds(creds))
	}

	s := grpc.NewServer(options...)
	pb.RegisterGreeterServer(s, &Service{})
	fmt.Println("Resiger server success")

	utils.CheckErrorPanic(s.Serve(lis))
}
