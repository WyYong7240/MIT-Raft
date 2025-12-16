package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/gofrs/flock"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.，包含两个字段key,value
type ByKey []KeyValue

// for sorting by key.，实现了sort.interface的三个接口方法
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		reply := WorkerReply{}
		// Woker先从协调器获取任务
		CallGetTask(&reply)

		// fmt.Printf("Woker Get %s Task, File %s, TaskID %d\n", reply.TaskType, reply.File, reply.TaskID)

		if reply.TaskType == "map" {

			if err := DoMap(&reply, mapf); err != nil {
				log.Fatalf("Map Task Failed, FileName: %s: %v", reply.File, err)
			}

			// 完成Map任务后，向协调器发送完成任务信号
			args := WorkerRequest{
				File:   reply.File,
				TaskID: reply.TaskID,
			}
			CallTaskFinished(&args)
		} else if reply.TaskType == "reduce" {
			if err := DoReduce(&reply, reducef); err != nil {
				log.Fatalf("Reduce Task Failed, ReduceID: %d: %v", reply.TaskID, err)
			}

			// 完成Reduce任务后，向协调器发送完成任务信号
			args := WorkerRequest{
				File:   reply.File,
				TaskID: reply.TaskID,
			}
			CallTaskFinished(&args)
		} else if reply.TaskType == "done" {
			// log.Printf("All Task Done!\n")
			break
		}

		// 一个工作完成后，睡眠1秒，避免与其他进程产生冲突
		time.Sleep(time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Woker向协调器请求任务
func CallGetTask(reply *WorkerReply) {
	args := WorkerRequest{}
	ok := call("Coordinator.AssignTask", &args, reply)
	if ok {
		// fmt.Printf("Woker Call Get Task Success!\n")
	} else {
		// fmt.Printf("Woker Call Get Task Failed!\n")
	}
}

func CallTaskFinished(args *WorkerRequest) {
	reply := WorkerReply{}
	ok := call("Coordinator.TaskFin", args, &reply)
	if ok {
		// fmt.Printf("Woker Call Finish Task Success!\n")
	} else {
		// fmt.Printf("Woker Call Finish Task Failed!\n")
	}
}

// Woker的Map操作
func DoMap(reply *WorkerReply, mapf func(string, string) []KeyValue) error {
	// 打开Map任务的文件
	file, err := os.Open(reply.File)
	if err != nil {
		log.Fatalf("MapTask Can't Open File %s", reply.File)
	}
	// 读取文件的内容，然后就可以关闭文件了
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("MapTask Read File %s Content Error!", reply.File)
	}
	file.Close()

	// 开始对文件内容进行Map操作,map操作返回<k,v> list
	// 以wc.go为例，将文本内容每个单词都构建为一个键值对，键是单词，值是自定义值，mapf返回键值对列表，没有做融合操
	intermediate := mapf(reply.File, string(content))

	// map阶段输出，需要将中间结果划分为nReduce个存储桶，以此将中间结果交给下面nReduce个reduce工作,
	// 将当前mapf返回的键值列表根据键划分为nReduce个工作
	nBuckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range intermediate {
		// 使用键哈希获取该keyvalue的reduceID，并将其放入对应的桶内
		reduceId := ihash(kv.Key) % reply.NReduce
		nBuckets[reduceId] = append(nBuckets[reduceId], kv)
	}

	// 将Map结果分组后，选择将其以临时文件的方式存储在系统中，方便传给接下来的Reduce工作
	for i := 0; i < reply.NReduce; i++ {
		// 由于所有的Map操作都产生Nreduce个中间文件，计划所有的Map操作产生共Nreduce个中间文件，因此每个中间文件都需要上文件锁
		// 所有产生的中间文件都在创建的intermediate文件夹中，待reduce操作完成后，会删除该文件夹和中间文件
		os.Mkdir("./intermediate", 0755)
		resultFileName := "./intermediate/mr-intermediate-" + strconv.Itoa(i)
		fileLock := flock.New(resultFileName + ".lock")
		lockCtx, err := fileLock.TryLock()
		if err != nil {
			// fmt.Println("Get File Lock Timeout: %v", err)
		}
		if !lockCtx {
			// fmt.Println("Get File Lock error")
		}
		defer fileLock.Unlock()

		// tempFile, err := os.CreateTemp("intermediate", "mr-map-intermediate")
		// 如果存在文件，就接着写，不存在就创建
		tempFile, _ := os.OpenFile(resultFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("MapWoker Can't Create Temp File: %v", err)
		}

		// 创建一个JSON编码器
		encoder := json.NewEncoder(tempFile)
		// 将第i个Reduce工作的内容编码并写入临时文件
		err = encoder.Encode(nBuckets[i])
		if err != nil {
			log.Fatalf("MapWoker Encoder Error: %v", err)
		}
		tempFile.Close()
	}
	return nil
}

// Woker的Reduce操作
func DoReduce(reply *WorkerReply, reducef func(string, []string) string) error {
	// 读取中间文件存储的键值对
	intermediate := []KeyValue{}

	reduceID := reply.TaskID
	intermediateFileName := "./intermediate/mr-intermediate-" + strconv.Itoa(reduceID)
	file, err := os.Open(intermediateFileName)
	if err != nil {
		log.Fatalf("ReduceWoker Load intermediate File Failed, TaskID: %d: %v", reduceID, err)
	}
	// log.Printf("ReduceWorker Load intermediate File Success, TaskID: %d\n", reduceID)
	decoder := json.NewDecoder(file)
	for {
		var fileKV []KeyValue
		if err := decoder.Decode(&fileKV); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			log.Fatalf("ReduceWoker Read FileKV Failed: %v", err)
		}
		intermediate = append(intermediate, fileKV...)
	}
	// log.Printf("ReduceWorker Read intermediate File Success, TaskID: %d\n", reduceID)

	// 按键排序
	sort.Sort(ByKey(intermediate))

	// log.Printf("ReduceWorker Sort intermediate File Success, TaskID: %d\n", reduceID)

	// 先创建reduce结果文件，然后再将结果写入; 由于是多个进程将结果写入一个文件，需要先获取这个文件的文件锁
	resultFileName := "mr-out-result"
	fileLock := flock.New(resultFileName + ".lock")
	lockCtx, err := fileLock.TryLock()
	if err != nil {
		// fmt.Println("Get File Lock Timeout: %v", err)
	}
	if !lockCtx {
		// fmt.Println("Get File Lock error")
	}
	defer fileLock.Unlock()

	// reduceResult, _ := os.Create("mr-reduce-result")
	reduceResult, _ := os.OpenFile(resultFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 找到所有与intermediate[i].key相同的键值对
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// 将键相同的键值对的value放到同一个List中，
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// log.Printf("ReduceWorker reduce from %d to %d, Key: %s\n", i, j, intermediate[i].Key)

		// 将该键值的键和同键值交给reduce函数处理
		output := reducef(intermediate[i].Key, values)
		// 将结果写入文件
		fmt.Fprintf(reduceResult, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Remove(intermediateFileName)
	os.Remove(intermediateFileName + ".lock")

	return nil
}
