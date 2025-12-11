package main

//
// simple sequential MapReduce.
// 实现一个单机版的MapReduce框架，用于加载一个插件.so文件，对多个输入文件执行MapReduce操作，并将结果输出到mr-out-0
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"plugin"
	"sort"

	"6.5840/mr"
)

// for sorting by key.，包含两个字段key,value
type ByKey []mr.KeyValue

// for sorting by key.，实现了sort.interface的三个接口方法
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	// 检查命令行参数，至少需要3个参数，程序名、插件.so文件路径、至少一个输出文件
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	// 加载插件中的Map和Reduce函数,从第一个命令行参数加载动态链接库,返回两个函数
	// mapf: func(filename string, content string) []mr.keyvalue
	// reducef: func(key string, values []string) string
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	// 读取每个输入的文件，将文件交给Map，累积Map输出的中间结果
	//
	// 初始化一个空的中间结果切片
	intermediate := []mr.KeyValue{}
	// 遍历所有输入文件，从第二个命令行参数开始
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename) // 打开文件
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file) // 读取全部内容为[]byte，转为string
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))      // 将文件名和文件内容交给Map，得到一组KeyValue
		intermediate = append(intermediate, kva...) // 将这些键值对追加到中间结果中
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	// 这里模拟了Map阶段，不像分布式系统那样分片，所有的中间结果都在内存中
	//

	// 对中间结果按键排序
	sort.Sort(ByKey(intermediate))

	// 输出文件名固定为mr-out-0
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	// 使用双指针i、j遍历排序后的中间结果
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 找到所有与intermediate[i].key相同的键值对
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// 将相同键值的对的value放在同一个string切片中
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 结果相当于是一个键值对多个value了
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
// 从动态库中加载出两个函数
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename) // 打开动态库文件
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map") // 在插件文件中查找名为Map的符号
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue) // 断言其为对应的函数类型
	xreducef, err := p.Lookup("Reduce")                // reduce 函数同理
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
