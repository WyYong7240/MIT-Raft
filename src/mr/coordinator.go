package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mutex sync.Mutex

	// 任务列表
	MapTask    []TaskState
	ReduceTask []TaskState

	Phase   int // 阶段0对应map，1对应reduce，2对应完成任务
	NMap    int
	NReduce int
}

type TaskState struct {
	FileName  string    // 任务对应处理的文件名
	StartTime time.Time // 任务分配出去的时间，用于计算worker是否在范围时间内完成了工作
	WorkerID  int       // 记录是哪个Worker拿走了任务
	Finished  bool      // 判断任务是否完成
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// 如果协调器的Phase状态进入2，代表任务完成
	if c.Phase == 2 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
