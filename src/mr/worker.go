package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % ReduceCount to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Map(mapf func(string, string) []KeyValue, reply JobReply) {
	// read input file and run map function
	file, err := os.Open(reply.MapFilename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.MapIndex)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.MapIndex)
	}
	file.Close()

	kvList := mapf(reply.MapFilename, string(content))

	// split key value pairs into buckets by hash
	bucketedKvList := make([][]KeyValue, reply.ReduceCount)
	for _, kv := range kvList {
		hash := ihash(kv.Key) % reply.ReduceCount
		bucketedKvList[hash] = append(bucketedKvList[hash], kv)
	}

	// write each bucket into a file
	for i := range reply.ReduceCount {
		if len(bucketedKvList[i]) > 0 {
			oname := fmt.Sprintf("mr-%d-%d", reply.MapIndex, i)
			ofile, _ := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE, 0666)
			defer ofile.Close()
			encoder := json.NewEncoder(ofile)
			for _, kv := range bucketedKvList[i] {
				encoder.Encode(&kv)
			}
		}
	}

	// ping coordinator when finished
	args := JobArgs{
		ID:       reply.ID,
		IsMap:    true,
		MapIndex: reply.MapIndex,
	}
	call("Coordinator.FinishedJob", &args, &reply)
}

func Reduce(reducef func(string, []string) string, reply JobReply) {
	// read corresponding reduce bucket files
	reduceMap := make(map[string][]string)
	for i := range reply.MapCount {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceIndex)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file: %v", err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			reduceMap[kv.Key] = append(reduceMap[kv.Key], kv.Value)
		}
	}

	// run the reduce functions and write output to a file
	oname := fmt.Sprintf("mr-out-%d", reply.ReduceIndex)
	ofile, _ := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE, 0666)
	defer ofile.Close()
	for k, v := range reduceMap {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}

	// ping coordinator when finished
	args := JobArgs{
		ID:          reply.ID,
		IsMap:       false,
		ReduceIndex: reply.ReduceIndex,
	}
	call("Coordinator.FinishedJob", &args, &reply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// keep polling for jobs from coordinator until coordinator has exited 
	for {
		args := JobArgs{}
		reply := JobReply{}
		call("Coordinator.RequestJob", &args, &reply)

		if reply.IsMap {
			Map(mapf, reply)
		} else {
			Reduce(reducef, reply)
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// exits if call fails (happens when coordinator is finished)
func call(rpcname string, args interface{}, reply interface{}) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		// fmt.Printf("call failed! worker exiting\n")
		os.Exit(0)
	}
}
