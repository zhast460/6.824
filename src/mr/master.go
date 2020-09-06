package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu         sync.Mutex
	mapJobs    map[string]int64 // job -> start time, -1 for not yet started
	reduceJobs map[int]int64
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetJob(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// fmt.Println("before get map job. ")
	// fmt.Println(m.mapJobs)

	if len(m.mapJobs) > 0 {
		for k, v := range m.mapJobs {
			if v == -1 {
				reply.HasJob = 1
				reply.FileName = k
				m.mapJobs[k] = time.Now().Unix()
				return nil
			}
		}

		// if map job not all completed, do not assign reduce job
		reply.HasJob = 0
		return nil
	}

	// fmt.Println("before get reduce job. ")
	// fmt.Println(m.reduceJobs)

	if len(m.reduceJobs) > 0 {
		for k, v := range m.reduceJobs {
			if v == -1 {
				reply.HasJob = 2
				reply.N = k
				m.reduceJobs[k] = time.Now().Unix()
				return nil
			}
		}
	}

	reply.HasJob = 0
	return nil
}

func (m *Master) CompleteJob(args *Args, reply *Reply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.HasJob == 1 {
		// fmt.Println("before delete map")
		// fmt.Println(m.mapJobs)
		// fmt.Println("deleting " + args.FileName)
		delete(m.mapJobs, args.FileName)
	} else if args.HasJob == 2 {
		// fmt.Println("before delete reduce")
		// fmt.Println(m.reduceJobs)
		// fmt.Printf("deleting %v\n", args.N)
		delete(m.reduceJobs, args.N)
	}
	return nil
}

// func (m *Master) MapJobCompleted(fileName string) {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	delete(m.mapJobs, fileName)
// }

// func (m *Master) ReduceJobCompleted(n int) {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
// 	delete(m.reduceJobs, n)
// }

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done(args *Args, reply *Reply) error {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	// if some worker crashed, re-issue the job.
	for k, v := range m.mapJobs {
		if v > -1 && time.Now().Unix()-v > 10 {
			m.mapJobs[k] = -1
		}
	}
	for k, v := range m.reduceJobs {
		if v > -1 && time.Now().Unix()-v > 10 {
			m.reduceJobs[k] = -1
		}
	}

	reply.Done = (len(m.mapJobs) == 0 && len(m.reduceJobs) == 0)
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mapJobs = make(map[string]int64)
	m.reduceJobs = make(map[int]int64)

	for _, file := range files {
		m.mapJobs[file] = -1
	}

	for i := 0; i < nReduce; i++ {
		m.reduceJobs[i] = -1
	}

	m.server()
	return &m
}
