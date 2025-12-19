package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerRequest struct {
	FileName        string // 当任务类型为Map时有意义，为处理完毕的文件名
	ReduceID        int    // 当任务类型为Reduce时有意义，为处理完毕的reduce桶
	DistributedTime int64  // 该任务被分配到worker的时间，用于判断worker是否超时
}

type WorkerReply struct {
	TaskType        string // 任务类型
	File            string // 当任务类型为Map时，有意义，为需要处理的文件名
	NReduce         int    // 当任务类型为Map时有意义，为需要将中间结果划分为的桶数
	TaskID          int    // 当任务类型为Map、Reduce时，有意义，Map:当前处理的是第几个Map任务；Reduce:当前处理的Reduce任务ID
	NMap            int    // 当任务类型为Reduce时，有意义，为需要接收的中间文件数
	DistributedTime int64  // 该任务被分配到Worker的时间
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
