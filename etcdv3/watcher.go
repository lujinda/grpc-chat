package etcdv3

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"google.golang.org/grpc/naming"
)

type Watcher struct {
	re             *Resolver
	client         clientv3.Client
	is_initialized bool
}

func extract_addrs(resp *clientv3.GetResponse) []string {
	addrs := []string{}

	if resp == nil || resp.Kvs == nil {
		return addrs
	}

	for _, kv := range resp.Kvs {
		if v := kv.Value; v != nil {
			addrs = append(addrs, string(v))
		}
	}
	return addrs
}

func (w *Watcher) Close() {
	w.client.Close()
}

func (w *Watcher) Next() ([]*naming.Update, error) {
	prefix := fmt.Sprintf("%s/%s/", Prefix, w.re.ServiceName)

	if !w.is_initialized {
		resp, err := w.client.Get(context.Background(), prefix, clientv3.WithPrefix())
		w.is_initialized = true
		if err == nil {
			addrs := extract_addrs(resp)
			if l := len(addrs); l != 0 {
				updates := make([]*naming.Update, l)
				for i := range addrs {
					updates[i] = &naming.Update{
						Op: naming.Add, Addr: addrs[i],
					}
				}

				return updates, nil
			}
		}
	}

	rch := w.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				return []*naming.Update{{
					Op:   naming.Add,
					Addr: string(ev.Kv.Value),
				}}, nil

			case mvccpb.DELETE:
				return []*naming.Update{{
					Op:   naming.Delete,
					Addr: string(ev.Kv.Value),
				}}, nil
			}
		}
	}
	return nil, nil
}
