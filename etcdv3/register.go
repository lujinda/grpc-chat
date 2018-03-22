package etcdv3

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
)

func Register(name string, host string, port int, target string, interval time.Duration, ttl int) error {
	service_value := fmt.Sprintf("%s:%d", host, port)
	service_key = fmt.Sprintf("%s/%s/nodes/%s", Prefix, name, service_value)

	var err error
	client, err = clientv3.New(clientv3.Config{
		Endpoints: strings.Split(target, ","),
	})

	if err != nil {
		return fmt.Errorf("grpclb: create etcd3 client failed: %v", err)
	}

	go func() {
		var err error
		var get_resp *clientv3.GetResponse
		ticker := time.NewTicker(interval)
		grant_resp, _ := client.Grant(context.Background(), int64(ttl))
		for {
			get_resp, err = client.Get(context.Background(), service_key)
			if err != nil {
				log.Printf("grpclb: set service '%s' with ttl to etcd3 failed: %s", name, err.Error())

			} else if get_resp.Count == 0 {
				if _, err = client.Put(context.Background(), service_key, service_value, clientv3.WithLease(grant_resp.ID)); err != nil {
					log.Printf("grpclb: set service '%s' with ttl to etcd3 failed: %s", name, err.Error())
				}
			} else {
				if _, err = client.KeepAliveOnce(context.Background(), grant_resp.ID); err != nil {
					log.Printf("grpclb: refresh service '%s' with ttl to etcd3 failed: %s", name, err.Error())
				}
			}

			select {
			case <-stop_signal:
				return

			case <-ticker.C:
			}
		}
	}()
	return nil
}

func UnRegister() error {
	stop_signal <- true
	stop_signal = make(chan bool, 1)

	var err error
	if _, err = client.Delete(context.Background(), service_key); err != nil {

		log.Printf("grpclb: deregister '%s' failed: %s", service_key, err.Error())
	} else {
		log.Printf("grpclb: deregister '%s' success", service_key)
	}
	return err
}
