package etcdv3

import "github.com/coreos/etcd/clientv3"

var Prefix = "etcd3_naming"
var client *clientv3.Client

var service_key string

var stop_signal chan bool = make(chan bool, 1)
