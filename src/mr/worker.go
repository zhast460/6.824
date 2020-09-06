package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var nReduce = 10 // TODO: can improve to read from rpc, instead of hardcode

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args, reply := Args{}, Reply{}
		if !call("Master.GetJob", &args, &reply) {
			return
		}

		switch reply.HasJob {
		case 0:
			time.Sleep(time.Second)
		case 1:
			runMapJob(reply.FileName, mapf)
			//fmt.Println("completed map " + reply.FileName)

		case 2:
			runReduceJob(reply.N, reducef)
			//fmt.Println("completed reduce " + reply.FileName)

		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func runMapJob(filename string, mapf func(string, string) []KeyValue) {

	//fmt.Println("Starting map job " + filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	m := make(map[int][]KeyValue)
	for _, kv := range kva {
		n := ihash(kv.Key) % nReduce
		if _, ok := m[n]; !ok {
			m[n] = []KeyValue{}
		}
		m[n] = append(m[n], kv)
	}

	for k, v := range m {
		ofile, err := os.Create(fmt.Sprintf("mr-%v-%v", trim(filename), k))
		if err != nil {
			fmt.Println(err)
		}
		enc := json.NewEncoder(ofile)
		enc.Encode(&v)
		ofile.Close()
	}

	call("Master.CompleteJob", &Args{FileName: filename, HasJob: 1}, &Reply{})
}

func trim(s string) string {
	tokens := strings.Split(s, "/")
	return tokens[len(tokens)-1]
}

func runReduceJob(n int, reducef func(string, []string) string) {

	//fmt.Printf("Starting reduce job %v\n", n)

	pat := fmt.Sprintf("mr-*-%v", n)
	matches, _ := filepath.Glob(pat)
	intermediate := []KeyValue{}
	for _, match := range matches {
		file, err := os.Open(match)
		if err != nil {
			fmt.Println(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kva []KeyValue
			if err := dec.Decode(&kva); err != nil {
				break
			}
			intermediate = append(intermediate, kva...)
		}

		file.Close()
		// err = os.Remove(match)
		// if err != nil {
		// 	fmt.Println(err)
		// }
	}

	sort.Sort(ByKey(intermediate))

	//fmt.Printf("reduce job %v has len %v of intermediates\n", n, len(intermediate))

	i := 0
	var sb strings.Builder
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		sb.WriteString(fmt.Sprintf("%v %v\n", intermediate[i].Key, output))

		//fmt.Printf("reduce job %v, key: %v, value: %v\n", n, intermediate[i].Key, output)

		// this is the correct format for each line of Reduce output.

		i = j
	}

	ofile, _ := os.Create(fmt.Sprintf("mr-out-%v", n)) // TODO: rename when completed writing
	fmt.Fprintf(ofile, sb.String())
	ofile.Close()

	call("Master.CompleteJob", &Args{N: n, HasJob: 2}, &Reply{})
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	//fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
