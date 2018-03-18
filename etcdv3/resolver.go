package etcdv3

import (
	"fmt"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"google.golang.org/grpc/naming"
)

type Resolver struct {
	ServiceName string
}

func NewResolver(name string) *Resolver {
	return &Resolver{
		ServiceName: name,
	}
}

func (re *Resolver) Resolve(target string) (naming.Watcher, error) {
	if re.ServiceName == "" {
		return nil, fmt.Errorf("grpclib: no service name provided")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(target, ","),
	})

	if err != nil {
		return nil, errors.Wrap(err, "grpclib: create etcd3 client faield")
	}

	return &Watcher{re: re, client: *client}, nil
}
