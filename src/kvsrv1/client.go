package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	// 初始化调用RPC需要传入的两个参数
	getArgs := rpc.GetArgs{
		Key: key,
	}
	getReply := rpc.GetReply{}

	// 对于Get操作来说，不论多少次重试都没关系，因为Get操作不改变服务器状态
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &getArgs, &getReply)
		if ok {
			return getReply.Value, getReply.Version, getReply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	// 初始化调用RPC需要传入的两个参数
	putArgs := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	putReply := rpc.PutReply{}

	// 一直重试，直到Client收到Server的Reply
	retryCount := 0
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &putArgs, &putReply)
		retryCount++ // 增加重传计数
		// 如果是第一次发送，就收到了Server的回复，直接返回服务器的错误
		if ok && retryCount == 1 {
			return putReply.Err
		} else if ok && retryCount != 1 {
			// 如果收到了回复，但是不是第一次发送，即重试过，那么返回:可能错误
			// 收到的错误，也可能不是ErrVersion，如果是其他两种错误，就直接返回
			if putReply.Err != rpc.ErrVersion {
				return putReply.Err
			} else {
				return rpc.ErrMaybe
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
