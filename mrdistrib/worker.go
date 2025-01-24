package mrdistrib

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"

	"umich.edu/eecs491/proj1/mapreduce"
)

const workerDebug = 0

func workerDPrintf(format string, a ...interface{}) (n int, err error) {
	if workerDebug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type Worker struct {
	name       string
	mapFunc    func(string) []mapreduce.KeyValue
	reduceFunc func(string, []string) string
	term       chan bool
	doJobReq   chan DoJobArgs
	doJobRes   chan DoJobReply
	numReq  chan bool
	numRes  chan int
}

//////////////// Worker tasks

// // RunWorker
//
// REQUIRES:
//
//	ctrlPort: the net address on which the map/reduce controller is
//	          listening
//	me: the net address of this worker
//	mapFunc: the function for performing a Map task
//	reduceFunc: the function for performing a Reduce task
//	term: termination channel, closed to end the Worker
//	waitFor: wait group use to wait for all Workers to end
//	         must be incremented by the caller prior to the call
//
// MODIFIES: waitfor
//
// EFFECTS:
//
//	runs the Worker, listening on port "me", until
//	"term" is closed. Notifies caller of exit via waitFor
//
// Notice the pair of goroutines here. This is necessary because
// Listen blocks indefinitely, and will not return until a request
// comes in *or* the socket is closed. So, one goroutine does the
// Listening, and a second is the "watcher"--it waits for the closure
// of the termination channel, and then in turn closes the
// socket. That forces the server itself to exit, signaling that on
// waitgroup.
//
// The caller can use this pattern to ensure that the unix-domain
// sockets are cleaned up properly by (a) incrementing waitFor before
// calling RunWorker and (b) waiting on waitFor after closing the
// termination channel.
func RunWorker(ctrlPort string,
	me string,
	mapFunc func(string) []mapreduce.KeyValue,
	reduceFunc func(string, []string) string,
	term chan bool,
	waitFor *sync.WaitGroup) *Worker {

	rpcDPrintf("Starting worker %s\n", me)

	wk := new(Worker)
	wk.name = me
	wk.mapFunc = mapFunc
	wk.reduceFunc = reduceFunc
	wk.term = term
	wk.doJobReq = make(chan DoJobArgs)
	wk.doJobRes = make(chan DoJobReply)
	wk.numReq = make(chan bool)
	wk.numRes = make(chan int)

	// Serialize all Worker RPCs
	go wk.performer()

	// Set up listen socket
	rpcSrv := rpc.NewServer()
	rpcSrv.Register(wk)
	os.Remove(me) // Allow later re-use of this socket

	l, err := net.Listen("unix", me)
	errCheck(err)

	// Launch monitor
	go func() {
		defer l.Close()
		<-term
	}()

	// Launch listener
	go func() {
		rpcDPrintf("%s is listening\n", me)
		for {
			conn, err := l.Accept()
			if err != nil {
				rpcDPrintf("%s is no longer listening\n", me)
				waitFor.Done()
				break
			}
			go rpcSrv.ServeConn(conn)
		}
	}()

	// Register worker--must happen after server is established
	regArg := RegisterArgs{
		Worker: me,
	}
	var regRes RegisterReply

	success := Call(ctrlPort, "Manager.Register", regArg, &regRes)
	if !success {
		log.Printf("Cannot register worker %s with %s", me, ctrlPort)
	}

	return wk
}

// Worker.performer -- serialize all inbound RPC calls
// Terminates on closing of the term channel
func (wk *Worker) performer() {
	completed := 0

	for {
		select {
		case job := <-wk.doJobReq:
			var rep DoJobReply
			switch job.Operation {
			case Map:
				mapreduce.DoMap(job.JobNumber, job.File,
					job.NumOtherPhase, wk.mapFunc)
			case Reduce:
				mapreduce.DoReduce(job.JobNumber, job.File,
					job.NumOtherPhase, wk.reduceFunc)
			}
			rep.OK = true
			completed += 1
			wk.doJobRes <- rep
		case <-wk.numReq:
			wk.numRes <- completed
		case <-wk.term:
			return
		}
	}
}

// RPC Entry Points
func (wk *Worker) DoJob(arg DoJobArgs, res *DoJobReply) error {
	wk.doJobReq <- arg
	*res = <-wk.doJobRes

	return nil
}

// Utility entry point; used by testing infrastructure
func (wk *Worker) NumJobs() int {
	wk.numReq <- true
	return <-wk.numRes
}
