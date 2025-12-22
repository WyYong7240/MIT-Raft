package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersionPair struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// 由于KV Server默认带了一个同步锁，并且需要线性处理每个请求，因此不计划采用sync.map
	KVMap map[string]ValueVersionPair
}

func MakeKVServer() *KVServer {
	// 初始化键值服务器
	kv := &KVServer{
		KVMap: make(map[string]ValueVersionPair, 0),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	// 先获取KVServer的锁
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// log.Printf("Server Get Called, Args, Key %s\n", args.Key)

	// 判断是否存在对应的键值对,如果存在，正常返回，否则返回ErrNoKey
	if vvpair, ok := kv.KVMap[args.Key]; ok {
		reply.Err = rpc.OK
		reply.Value = vvpair.Value
		reply.Version = vvpair.Version
		// log.Printf("Server Get Done, Ok\n")

	} else {
		// log.Printf("Server Get Done, NoKey\n")
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	// 先获取KVServer的锁
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// log.Printf("Server Put Called, Args, Key %s, Value %s, Version %d\n", args.Key, args.Value, args.Version)
	// 如果Put的键在Map中存在
	if vvpair, ok := kv.KVMap[args.Key]; ok {
		// 判断Put的版本号是否一致, 如果版本号一致，允许修改Value，并Version+1
		if args.Version == vvpair.Version {
			kv.KVMap[args.Key] = ValueVersionPair{
				Value:   args.Value,
				Version: args.Version + 1,
			}
			reply.Err = rpc.OK
			// log.Printf("Server Get Done, OK\n")
		} else {
			// 如果版本号不匹配，返回ErrVersion
			reply.Err = rpc.ErrVersion
			// log.Printf("Server Get Done, ErrVersion\n")
		}
	} else if args.Version == 0 {
		// 如果Put的键不存在，但是Put的Version是0，说明要创建一个新键
		kv.KVMap[args.Key] = ValueVersionPair{
			Value:   args.Value,
			Version: 1,
		}
		reply.Err = rpc.OK
		// log.Printf("Server Get Done, OK\n")
	} else {
		// 如果Put的键不存在，并且Version也不是0，应该返回ErrNoKey
		reply.Err = rpc.ErrNoKey
		// log.Printf("Server Get Done, ErrNoKey\n")
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
