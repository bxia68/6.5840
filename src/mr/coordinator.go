package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Status int

const (
	inProgress = iota
	finished
)

type Coordinator struct {
	// Your definitions here.
	filenames []string
	nReduce   int
	mapWg     sync.WaitGroup
	reduceWg  sync.WaitGroup
	statusLock sync.Mutex
	jobStatus  map[uint64]Status
	done       atomic.Bool
	jobQueue   chan JobReply
	jobCounter atomic.Uint64
}

func FinishChecker(c *Coordinator, reply JobReply) {
	// sleep and then retry job if it hasn't finished
	time.Sleep(10 * time.Second)
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	if c.jobStatus[reply.ID] != finished {
		c.jobQueue <- reply
	}
	delete(c.jobStatus, reply.ID)
}

func (c *Coordinator) RequestJob(args *JobArgs, reply *JobReply) error {
	*reply = <-c.jobQueue
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.jobStatus[reply.ID] = inProgress
	go FinishChecker(c, *reply)

	return nil
}

func (c *Coordinator) FinishedJob(args *JobArgs, reply *JobReply) error {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	if c.jobStatus[args.ID] == inProgress {
		c.jobStatus[args.ID] = finished
		if args.IsMap {
			c.mapWg.Done()
		} else {
			c.reduceWg.Done()
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	b := c.done.Load()
	time.Sleep(time.Second)
	return b
}

func InitWg(c *Coordinator) {
	c.mapWg.Add(len(c.filenames))
	c.reduceWg.Add(c.nReduce)
}

func QueueMapJobs(c *Coordinator) {
	for i, filename := range c.filenames {
		id := c.jobCounter.Add(1)
		mapJob := JobReply{
			ID:          id,
			IsMap:       true,
			ReduceCount: c.nReduce,
			MapIndex:    i,
			MapFilename: filename,
		}
		c.jobQueue <- mapJob
	}
}

func QueueReduceJobs(c *Coordinator) {
	c.mapWg.Wait()
	fmt.Print("map jobs finished. queueing reduce jobs. \n")
	for i := range c.nReduce {
		id := c.jobCounter.Add(1)
		reduceJob := JobReply{
			ID:          id,
			IsMap:       false,
			ReduceIndex: i,
			MapCount:    len(c.filenames),
			ReduceCount: c.nReduce,
		}
		c.jobQueue <- reduceJob
	}
}

func DoneChecker(c *Coordinator) {
	c.reduceWg.Wait()
	// fmt.Println("all jobs finished!")
	c.done.Store(true)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		filenames: files,
		nReduce:   nReduce,
		jobStatus: make(map[uint64]Status),
		jobQueue:  make(chan JobReply, max(len(files), nReduce)),
	}

	InitWg(&c)
	QueueMapJobs(&c)
	go QueueReduceJobs(&c)
	go DoneChecker(&c)

	c.server()
	return &c
}
