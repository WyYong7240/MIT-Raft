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

const TimeOut = 10

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
	FileName  string // 任务对应处理的文件名
	StartTime int64  // 任务分配出去的时间，用于计算worker是否在范围时间内完成了工作
	WorkerID  int    // 记录是哪个Worker拿走了任务
	Finished  bool   // 判断任务是否完成
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//	func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//		reply.Y = args.X + 1
//		return nil
//	}

// 统计任务完成数量，用于协调器做状态切换
func FinishedTask(taskList []TaskState) int {
	finTask := 0
	for _, task := range taskList {
		if task.Finished {
			finTask += 1
		}
	}
	return finTask
}

func (c *Coordinator) AssignTask(args *WorkerRequest, reply *WorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 如果是协调器处于Map阶段，给Worker分配Map任务
	if c.Phase == 0 {
		// 如果MapTask中还有没完成的任务，就继续Map的配发，要求任务没有完成，并且也没有被分配
		// 增加判断被分配出去的任务超时未完成的也要重新被分配
		for i, task := range c.MapTask {
			// 如果任务未完成，并且未被分配，或者被分配出去但是超时了，就重新分配
			nowTime := time.Now().Unix()
			if !task.Finished && (task.StartTime == 0 || nowTime-task.StartTime >= TimeOut) {
				reply.TaskType = "map"
				reply.File = task.FileName
				reply.TaskID = i
				reply.NReduce = c.NReduce
				c.MapTask[i].StartTime = nowTime
				reply.DistributedTime = nowTime
			}
			// 如果已经为这个woker分配了一个任务 ，就退出分配循环
			if reply.TaskType != "" {
				break
			}
			// fmt.Printf("Assgin %s Task, File %s, TaskID %d\n", reply.TaskType, reply.File, reply.TaskID)
		}
	} else if c.Phase == 1 {
		for i, task := range c.ReduceTask {
			nowTime := time.Now().Unix()
			if !task.Finished && (task.StartTime == 0 || nowTime-task.StartTime >= TimeOut) {
				reply.TaskType = "reduce"
				reply.File = task.FileName
				reply.TaskID = i
				reply.NReduce = c.NReduce
				c.ReduceTask[i].StartTime = nowTime
				reply.DistributedTime = nowTime
			}
			// 如果已经为这个woker分配了一个任务 ，就退出分配循环
			if reply.TaskType != "" {
				break
			}
			// fmt.Printf("Assgin %s Task, File %s, TaskID %d\n", reply.TaskType, reply.File, reply.TaskID)
		}
	} else if c.Phase == 2 {
		// 通知worker退出
		reply.TaskType = "done"
	}
	return nil
}

func (c *Coordinator) TaskFin(args *WorkerRequest, reply *WorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	timeNow := time.Now().Unix()
	if c.Phase == 0 {
		// Worker向协调器报告工作状态，首先看这个任务是否Timeout
		taskStartTime := c.MapTask[args.TaskID].StartTime
		// 如果worker处理的任务的分配时间不等于该任务的起始时间，说明该任务被重新分配了，如果相等，还超时了那就按超时算
		if taskStartTime != args.DistributedTime || timeNow-taskStartTime > TimeOut {
			// fmt.Printf("Map Task TimeOut, TaskID: %d, FileName: %s\n", args.TaskID, args.File)
			return nil
		}
		// 如果任务没有超时，说明任务正常完成，在协调器中给该任务打上任务完成的标签
		c.MapTask[args.TaskID].Finished = true

		// 判断是否完成了所有的Map任务
		if finTaskNum := FinishedTask(c.MapTask); finTaskNum == c.NMap {
			c.Phase = 1
			// fmt.Printf("MapTask Finished, Coordinator into Phase %d\n", c.Phase)
			// 完成所有Map任务后，就要向Reduce待完成任务中添加任务
			for i := 0; i < c.NReduce; i++ {
				reduceTask := TaskState{
					FileName:  "",
					StartTime: 0,
					WorkerID:  i,
					Finished:  false,
				}
				c.ReduceTask[i] = reduceTask
			}
		}
	} else if c.Phase == 1 {
		taskStartTime := c.ReduceTask[args.TaskID].StartTime
		if taskStartTime != args.DistributedTime || timeNow-taskStartTime > TimeOut {
			// fmt.Printf("Reduce Task TimeOut, TaskID: %d\n", args.TaskID)
			return nil
		}
		// 如果任务没有超时，说明正常完成，标记完成Reduce任务
		c.ReduceTask[args.TaskID].Finished = true

		// 检测是否完成所有Reduce任务
		if finTaskNum := FinishedTask(c.ReduceTask); finTaskNum == c.NReduce {
			c.Phase = 2
			// fmt.Printf("ReduceTask Finished, Coordinator into Phase %d\n", c.Phase)
		}
	}
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
		// 同时删除中间文件夹和结果文件锁
		os.Remove("./intermediate")
		os.Remove("mr-out-result.lock")
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTask:    make([]TaskState, len(files)),
		ReduceTask: make([]TaskState, nReduce),
		Phase:      0,
		NMap:       len(files),
		NReduce:    nReduce,
	}

	// Your code here.
	for i, file := range files {
		MapTask := TaskState{
			FileName:  file,
			StartTime: 0,
			WorkerID:  -1,
			Finished:  false,
		}
		c.MapTask[i] = MapTask
	}

	c.server()
	// fmt.Println("Coordinator Start Success!")
	return &c
}
