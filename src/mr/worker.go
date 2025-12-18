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
	// 不断循环，worker不断向coordinator请求任务
	for {
		reply := WorkerReply{}
		// Woker先从协调器获取任务,如果调用rpc失败，说明coordinator已经退出，worker可以退出
		if ok := CallGetTask(&reply); !ok {
			// time.Sleep(time.Second)
			// continue
			log.Printf("Coordinator Missed!\n")
			return
		}
		// fmt.Printf("Woker Get %s Task, File %s, TaskID %d\n", reply.TaskType, reply.File, reply.TaskID)

		if reply.TaskType == "map" {
			if err := DoMap(&reply, mapf); err != nil {
				// 不应该使用Fatalf，这样会杀死worker进程，worker需要能够处理Map出错的情况，处理结果就是放弃该任务，重新请求任务
				// log.Fatalf("Map Task Failed, FileName: %s: %v", reply.File, err)
				log.Printf("Map Task Failed, FileName: %s: %v\n", reply.File, err)
				continue
			}
			// 完成Map任务后，向协调器发送完成任务信号
			args := WorkerRequest{
				FileName:        reply.File,
				DistributedTime: reply.DistributedTime,
			}
			// 如果调用rpc失败，等待1s后重试,但是就不是调用taskFin rpc了，重新请求任务,这个和请求任务感觉不一样，还是重试
			if ok := CallTaskFinished(&args); !ok {
				time.Sleep(time.Second)
				continue
			}
		} else if reply.TaskType == "reduce" {
			if err := DoReduce(&reply, reducef); err != nil {
				// log.Fatalf("Reduce Task Failed, ReduceID: %d: %v", reply.TaskID, err)
				log.Printf("Reduce Task Failed, ReduceID: %d: %v\n", reply.TaskID, err)
				continue
			}
			// 完成Reduce任务后，向协调器发送完成任务信号
			args := WorkerRequest{
				ReduceID:        reply.TaskID,
				DistributedTime: reply.DistributedTime,
			}
			// 如果调用rpc失败，等待1s后重试,但是就不是调用taskFin rpc了，重新请求任务
			if ok := CallTaskFinished(&args); !ok {
				time.Sleep(time.Second)
				continue
			}
		} else if reply.TaskType == "done" {
			// 如果coordinator说所有任务都完成了，worker退出
			// log.Printf("All Task Done!\n")
			return
		}
		// 针对Coordinator给出的TaskType=wait等待类型任务，不用单拎if分支处理，if分支没有wait就自动重试，达到了等待的效果

		// 一个工作完成后，睡眠1秒，避免与其他进程产生冲突
		time.Sleep(time.Second)
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
		log.Println("dialing", err)
		return false
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("calling", err)
	return false
}

// Woker向协调器请求任务
func CallGetTask(reply *WorkerReply) bool {
	args := WorkerRequest{}
	ok := call("Coordinator.AssignTask", &args, reply)
	if ok {
		// fmt.Printf("Woker Call Get Task Success!\n")
		return true
	} else {
		// fmt.Printf("Woker Call Get Task Failed!\n")
		return false
	}
}

func CallTaskFinished(args *WorkerRequest) bool {
	reply := WorkerReply{}
	ok := call("Coordinator.TaskFin", args, &reply)
	if ok {
		// fmt.Printf("Woker Call Finish Task Success!\n")
		return true
	} else {
		// fmt.Printf("Woker Call Finish Task Failed!\n")
		return false
	}
}

// Woker的Map操作
func DoMap(reply *WorkerReply, mapf func(string, string) []KeyValue) error {
	// 打开Map任务的文件
	file, err := os.Open(reply.File)
	if err != nil {
		// 嵌套函数中同样，不应该使用fatalf，而是放弃该任务，重新请求任务
		// log.Fatalf("MapTask Can't Open File %s\n", reply.File)
		log.Printf("MapTask Can't Open File %s\n", reply.File)
		return err
	}
	// 读取文件的内容，然后就可以关闭文件了
	content, err := io.ReadAll(file)
	if err != nil {
		// log.Fatalf("MapTask Read File %s Content Error!\n", reply.File)
		log.Printf("MapTask Read File %s Content Error!\n", reply.File)
		return err
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
		// 所有产生的中间文件都在创建的intermediate文件夹中，待reduce操作完成后，会删除该文件夹和中间文件
		// 根据Map Reduce设计，每个Map操作都会产生nReduce个中间文件，因此需要mapID来标识不同的Map操作产生的中间文件
		os.Mkdir("./intermediate", 0755)

		// 由于论文要求，每个map任务都输出nReduce个文件，而非所有map任务共享nReduce个中间文件
		// 根据实验要求，可以使用创建临时文件的方式防止worker崩溃后的内容被查看
		tempFile, err := os.CreateTemp("", "mr-reduce-temp")
		if err != nil {
			// log.Fatalf("MapWoker Can't Create Temp File: %v\n", err)
			log.Printf("MapWoker Can't Create Temp File: %v\n", err)
			return err
		}

		// 创建一个JSON编码器，编码器写入文件方式也是官方推荐的
		encoder := json.NewEncoder(tempFile)
		// 将第i个Reduce工作的内容编码并写入临时文件
		// 经过测试，将JSON数组整体写入，比较容易出现JSON非完整错误，将JSON一条条写入文件
		for _, inter := range nBuckets[i] {
			err = encoder.Encode(inter)
			if err != nil {
				// log.Fatalf("MapWoker Encoder Error: %v\n", err)
				log.Printf("MapWoker Encoder Error: %v\n", err)
				return err
			}
		}
		tempFile.Close()

		resultFileName := fmt.Sprintf("./intermediate/mr-%d-%d", reply.TaskID, i)
		os.Rename(tempFile.Name(), resultFileName)
	}
	return nil
}

// Woker的Reduce操作
func DoReduce(reply *WorkerReply, reducef func(string, []string) string) error {
	// 读取中间文件存储的键值对
	intermediate := []KeyValue{}
	reduceID := reply.TaskID

	// 读取该reduce操作所需要的所有中间文件的键值对内容
	for i := 0; i < reply.NMap; i++ {
		intermediateFileName := fmt.Sprintf("./intermediate/mr-%d-%d", i, reduceID)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			// log.Fatalf("ReduceWoker Load intermediate File %s Failed: %v\n", intermediateFileName, err)
			log.Printf("ReduceWoker Load intermediate File %s Failed: %v\n", intermediateFileName, err)
			return err
		}
		// log.Printf("ReduceWorker Load intermediate File Success, TaskID: %d\n", reduceID)
		// 创建该中间文件的json解码器
		decoder := json.NewDecoder(file)
		for {
			// 由于解码器每次只能读取一个完整的json记录, 因此读取一整个文件的json内容，需要循环
			var fileKV KeyValue
			// 如果读到文件尾，就退出该读文件的循环
			if err := decoder.Decode(&fileKV); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				// log.Fatalf("ReduceWorker Read %s FileKV Failed: %v\n", intermediateFileName, err)
				log.Printf("ReduceWorker Read %s FileKV Failed: %v\n", intermediateFileName, err)
				return err
			}
			// 将该中间文件的键值对内容添加到reduce操作需要处理的中间值列表中
			intermediate = append(intermediate, fileKV)
			// log.Printf("ReduceWorker Read intermediate File Success, TaskID: %d\n", reduceID)
		}
	}

	// 将所有要处理的中间值键值对按键排序
	sort.Sort(ByKey(intermediate))
	// log.Printf("ReduceWorker Sort intermediate File Success, TaskID: %d\n", reduceID)

	// 由于论文要求，每个reduce任务都输出一个文件，而非所有reduce任务共享一个输出文件
	// 因此每个reduce任务都需要创建一个输出文件，根据实验要求，可以使用创建临时文件的方式防止worker崩溃后的内容被查看
	tempFile, err := os.CreateTemp("", "mr-reduce-temp")
	if err != nil {
		// log.Fatalf("ReduceWorker Can't Create Temp File: %v\n", err)
		log.Printf("ReduceWorker Can't Create Temp File: %v\n", err)
		return err
	}

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

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// 该reduce任务完成后，修改临时文件名，并关闭文件
	tempFile.Close()
	os.Rename(tempFile.Name(), "mr-out-"+strconv.Itoa(reduceID))
	return nil
}
