package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const TimeOut = 10

type Coordinator struct {
	// 任务列表
	MapTaskChan    chan MapTask    // Map任务通道，类似线程安全的队列
	ReduceTaskChan chan ReduceTask // Reduce任务通道，类似线程安全的队列

	// 分配任务列表
	DistributedTask sync.Map // 已经被分配的Map任务，用于coordinator检查任务是否超时
	// DistributedReduceTask sync.Map

	Phase        int64 // 阶段0对应map，1对应reduce，2对应完成任务
	FinishedTask int64 // 每个阶段已经完成的任务数量
	NMap         int   // 共有N个Map任务，共产生NMap * NReduce个中间文件
	NReduce      int
}

type MapTask struct {
	FileName  string // Map处理的文件名
	MapID     int    // 该文件对应的Map任务ID，用于标识输出的中间文件
	StartTime int64  // 任务开始时间，也即任务被分配时间
}

type ReduceTask struct {
	ReduceID  int   // 该Reduce任务处理的中间文件ID，每个Map操作都有对应ID的中间文件
	StartTime int64 // 任务开始时间，也即任务被分配时间
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// 当有多个Worker Call Coordinator.AssignTask RPC时，会同时有多个AssignTask运行，因此需要线程安全访问
func (c *Coordinator) AssignTask(args *WorkerRequest, reply *WorkerReply) error {
	mrCoordinatorPhase := atomic.LoadInt64(&c.Phase)

	// 如果是协调器处于Map阶段，给Worker分配Map任务
	if mrCoordinatorPhase == 0 {
		// 首先检查是否存在超时的任务
		c.DistributedTask.Range(func(key, value interface{}) bool {
			// 先做类型转换
			taskFileName, _ := key.(string)
			mapTask, _ := value.(MapTask)
			nowTime := time.Now().Unix()
			// 如果该任务超时了，就重新进入chan，并将其在被分配map中删除
			if nowTime-mapTask.StartTime > TimeOut {
				c.MapTaskChan <- mapTask
				c.DistributedTask.Delete(taskFileName)
			}
			return true
		})

		// 从通道中取出任务，如果任务为空，就阻塞
		// 说是不能阻塞，想象，当最后一个Map任务被分配出去，MapTaskChan就是空的，并且状态Map也是空的
		// 但是其他Worker请求任务，由于Coordinator还在Map阶段，就会在这里阻塞住，那么当最后一个Map任务完成，其他阻塞在这里的Worker应该如何？
		// 因此，不能在这里阻塞住，如果没有任务分配，就让Worker进入等待状态
		if len(c.MapTaskChan) == 0 {
			reply.TaskType = "wait"
		} else {
			takeAMapTask := <-c.MapTaskChan
			nowTime := time.Now().Unix()

			// 给Worker Map工作的信息
			reply.TaskType = "map"
			reply.File = takeAMapTask.FileName
			reply.NReduce = c.NReduce
			reply.DistributedTime = nowTime
			reply.TaskID = takeAMapTask.MapID

			// 将该任务被分配出去，记录在Map中
			takeAMapTask.StartTime = nowTime
			c.DistributedTask.Store(takeAMapTask.FileName, takeAMapTask)

			// fmt.Printf("Assgin %s Task, File %s, TaskID %d\n", reply.TaskType, reply.File, reply.TaskID)
		}
	} else if mrCoordinatorPhase == 1 {
		// 首先检查是否存在超时的任务
		c.DistributedTask.Range(func(key, value interface{}) bool {
			// 先做类型转换
			reduceTaskID, _ := key.(int)
			reduceTask, _ := value.(ReduceTask)
			nowTime := time.Now().Unix()
			// 如果该任务超时了，就重新进入chan，并将其在被分配map中删除
			if nowTime-reduceTask.StartTime > TimeOut {
				c.ReduceTaskChan <- reduceTask
				c.DistributedTask.Delete(reduceTaskID)
			}
			return true
		})

		// 从通道中取出任务，如果任务为空，就阻塞
		// 说是不能阻塞，想象，当最后一个Map任务被分配出去，MapTaskChan就是空的，并且状态Map也是空的
		// 但是其他Worker请求任务，由于Coordinator还在Map阶段，就会在这里阻塞住，那么当最后一个Map任务完成，其他阻塞在这里的Worker应该如何？
		// 因此，不能在这里阻塞住，如果没有任务分配，就让Worker进入等待状态
		if len(c.ReduceTaskChan) == 0 {
			reply.TaskType = "wait"
		} else {
			takeAReduceTask := <-c.ReduceTaskChan
			nowTime := time.Now().Unix()

			// 给Worker Reduce工作的信息
			reply.TaskType = "reduce"
			reply.TaskID = takeAReduceTask.ReduceID
			reply.NMap = c.NMap
			reply.DistributedTime = nowTime

			// 将该任务被分配出去，记录在Map中
			takeAReduceTask.StartTime = nowTime
			c.DistributedTask.Store(takeAReduceTask.ReduceID, takeAReduceTask)

			// fmt.Printf("Assgin %s Task, File %s, TaskID %d\n", reply.TaskType, reply.File, reply.TaskID)
		}
	} else if mrCoordinatorPhase == 2 {
		// 通知worker退出
		reply.TaskType = "done"
	}
	return nil
}

func (c *Coordinator) TaskFin(args *WorkerRequest, reply *WorkerReply) error {
	mrCoordinatorPhase := atomic.LoadInt64(&c.Phase)

	timeNow := time.Now().Unix()
	if mrCoordinatorPhase == 0 {
		// Worker向协调器报告工作状态，首先看这个任务是否Timeout

		value, ok := c.DistributedTask.Load(args.FileName)
		if !ok {
			// 如果没有从被分配Map中获取到对应任务，说明Coordinator已经发现该任务超时，并且将该任务重新加入chan了
			return nil
		}
		mapTask, _ := value.(MapTask)
		taskStartTime := mapTask.StartTime

		// 就算在已分配任务列表中找到了对应元素，也不一定记录的是分配该worker了，因此仍然需要比对当前任务的开始时间和该worker收到的被分配时间，如果不一致就不是分配给该worker的
		// 如果worker处理的任务的分配时间不等于该任务的起始时间，说明该任务被重新分配了，如果相等，还超时了那就按超时算
		if taskStartTime != args.DistributedTime || timeNow-taskStartTime > TimeOut {
			// fmt.Printf("Map Task TimeOut, TaskID: %d, FileName: %s\n", args.TaskID, args.File)
			return nil
		}

		// 如果任务没有超时，说明任务正常完成，在协调器中将该任务从已分配任务列表中删除，并将任务完成计数+1
		c.DistributedTask.Delete(args.FileName)
		finishedTask := atomic.LoadInt64(&c.FinishedTask)
		atomic.StoreInt64(&c.FinishedTask, finishedTask+1)

		// 判断是否完成了所有的Map任务, 如果完成了所有的Map任务，并且ReduceTaskChan还没有被放入任务
		// 判断ReduceTaskChan是为了防止每个TaskFin对ReduceTaskChan在完成Map任务时都添加任务
		if finishedTask+1 == int64(c.NMap) && len(c.ReduceTaskChan) == 0 {

			// fmt.Printf("MapTask Finished, Coordinator into Phase %d\n", c.Phase)
			// 完成所有Map任务后，就要向Reduce待完成任务中添加任务
			for i := 0; i < int(c.NReduce); i++ {
				reduceTask := ReduceTask{
					ReduceID:  i,
					StartTime: -1,
				}
				c.ReduceTaskChan <- reduceTask
			}

			// 将协调器转换状态
			atomic.StoreInt64(&c.Phase, 1)
			// 将阶段任务完成数量归零
			atomic.StoreInt64(&c.FinishedTask, 0)
		}
	} else if mrCoordinatorPhase == 1 {
		// Worker向协调器报告工作状态，首先看这个任务是否Timeout
		value, ok := c.DistributedTask.Load(args.ReduceID)
		if !ok {
			// 如果没有从被分配Map中获取到对应任务，说明Coordinator已经发现该任务超时，并且将该任务重新加入chan了
			return nil
		}
		reduceTask, _ := value.(ReduceTask)
		taskStartTime := reduceTask.StartTime

		// 就算在已分配任务列表中找到了对应元素，也不一定记录的是分配该worker了，因此仍然需要比对当前任务的开始时间和该worker收到的被分配时间，如果不一致就不是分配给该worker的
		// 如果worker处理的任务的分配时间不等于该任务的起始时间，说明该任务被重新分配了，如果相等，还超时了那就按超时算
		if taskStartTime != args.DistributedTime || timeNow-taskStartTime > TimeOut {
			// fmt.Printf("Map Task TimeOut, TaskID: %d, FileName: %s\n", args.TaskID, args.File)
			return nil
		}

		// 如果任务没有超时，说明任务正常完成，在协调器中将该任务从已分配任务列表中删除，并将任务完成计数+1
		c.DistributedTask.Delete(args.ReduceID)
		finishedTask := atomic.LoadInt64(&c.FinishedTask)
		atomic.StoreInt64(&c.FinishedTask, finishedTask+1)

		// 判断是否完成了所有的Reduce任务, 如果完成了所有的Reduce任务，就切换协调器阶段
		if finishedTask+1 == int64(c.NReduce) {
			// 将协调器转换状态
			atomic.StoreInt64(&c.Phase, 2)
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
		// os.RemoveAll("./intermediate")

		// 让coordinator结束任务后再提供一段时间的rpc，让所有worker都能接收到taskType="done"的通知
		time.Sleep(2 * time.Second)
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskChan:    make(chan MapTask, len(files)),
		ReduceTaskChan: make(chan ReduceTask, nReduce),
		Phase:          0,
		NMap:           len(files),
		NReduce:        nReduce,
	}

	// 为Map任务通道添加任务
	for i, file := range files {
		mapTask := MapTask{
			FileName:  file,
			MapID:     i,
			StartTime: -1,
		}
		c.MapTaskChan <- mapTask
	}

	c.server()
	// fmt.Println("Coordinator Start Success!")
	return &c
}
