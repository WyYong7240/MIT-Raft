package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	LockName  string // 对应该分层锁的名称
	ClientKey string // 对应该分层锁的状态，如果为空就是未上锁，如果不为空，应该是持有该锁的clientID
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:        ck,
		LockName:  l,
		ClientKey: kvtest.RandValue(8),
	}
	// You may add code here

	// 将分层锁创建在KVMap中，利用Put操作，一开始创建锁的时候，不上锁，也就是value为空
	// 如果添加锁失败，那么说明KVMap中已经有了该锁
	// 所以这里的Err不接收并判定情况也没事，只需要保证KVMap中有该锁就可以
	ck.Put(lk.LockName, "", 0)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// 获取对应的锁，如果对应的分层锁是上锁的状态，就一直Get，直到没有上错
	for {
		lockCode, lockVersion, lockErr := lk.ck.Get(lk.LockName)
		// 如果获取锁失败，或者锁状态不为未上锁，继续获取锁
		if lockErr != rpc.OK || lockCode != "" {
			continue
		}

		// 分层锁未上锁，并且获取到锁版本，可以上锁,将该客户端的钥匙（锁芯）放入KVMap，使得该锁目前只有该客户端可以解锁
		if lockErr = lk.ck.Put(lk.LockName, lk.ClientKey, lockVersion); lockErr == rpc.OK {
			// 如果上锁失败，继续获取锁，如果上锁成功，则跳出循环
			// 由于version的存在，当两个客户端同时执行上锁操作时，RPC的线性操作，导致后者执行上锁操作时，Version版本会不一样，导致上锁出错
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func (lk *Lock) Release() {
	// Your code here
	// 获取该锁的状态，判定该客户端持有的钥匙，是否可以匹配上该锁的锁芯
	lockCode, lockVersion, lockErr := lk.ck.Get(lk.LockName)
	// 如果获取锁失败，或者客户端发现自己不是持有该锁的人，应该返回解锁失败吧
	if lockErr != rpc.OK || lockCode != lk.ClientKey {
		log.Fatalf("Release Lock %s Failed: %v\n", lk.LockName, lockErr)
	}

	// 发现自己是该锁的持有者，合法释放该锁
	if lockErr = lk.ck.Put(lk.LockName, "", lockVersion); lockErr != rpc.OK {
		log.Fatalf("Release Lock %s Failed: %v\n", lk.LockName, lockErr)
	}
}
