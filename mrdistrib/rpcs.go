package mrdistrib

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

// Error check; can be used anywhere in the package
func errCheck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const rpcDebug = 0

func rpcDPrintf(format string, a ...interface{}) (n int, err error) {
	if rpcDebug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// RPC arguments and replies Field names must be externalized
// (i.e. they must start with capital letters.)

/////////////////////////// MANAGER

// The Manager accepts worker registrations, and assigns incomplete
// Map and Reduce jobs to available workers. Failures are assumed to
// be transient; if a job assignment fails, that worker may (and
// should) be contacted again.

type RegisterArgs struct {
	Worker string
}

type RegisterReply struct {
	OK bool
}

/////////////////////////// WORKER

// Workers register themselves with the controller, perform Map and
// Reduce jobs, and can be remotely shut down.

// DoJob: assign a Map or Reduce job to the worker. Assumes presence
// of a shared file system, and uses job numbers/phase sizes to name
// intermediate files.
//
// REQUIRES: Controller never assigns the same job to two different
// workers at the same time; otherwise there are shared writes to the
// intermediate output file.

// DoJob: Perform either a single Map or Reduce job

const (
	Map    = "Map"
	Reduce = "Reduce"
)

type JobType string

type DoJobArgs struct {
	File          string
	Operation     JobType
	JobNumber     int // This job's number
	NumOtherPhase int // total number of jobs in other phase
}

type DoJobReply struct {
	OK bool
}

///////////////////////////////////////// Utility routines

// The RPC subsystem includes three entry points.
//
// Call() invokes a remote procedure call that may fail
//        failure can be due to the (simulated) network,
//        or because the server operation fails for some reason
//
// Set() establishes the failure parameters for one (simulated)
//       network endpoint
//
// Clear() removes the failure parameters for one (simulated) network
//         endpoint, and forgets that said enpoint exists.

// Behind the scenes there is a goroutine that manages the endpoint
// state for us. That routine is initialized, and thereafter can be
// invoked via a request/response channel pair.

// command enums (with convenience functions for printing)
type CtrlCmd int

const (
	SET CtrlCmd = iota + 1
	CLEAR
	CHECK
)

func (c CtrlCmd) String() string {
	return [...]string{"SET", "CLEAR", "CHECK"}[c-1]
}

func (c CtrlCmd) EnumIndex() int {
	return int(c)
}

// Control request
type CtrlReq struct {
	Cmd        CtrlCmd
	Name       string
	FailProb   float64 // Valid only for SET, [0..1] prob. of failure
	Delay      int     // Valid only for SET, seconds to sleep
	Limited    bool    // Valid only for SET, does (not) have TTL
	TimeToLive int     // Valid only for SET/Limited; TTL count
}

type CtrlRes struct {
	Known   bool // Valid only for CHECK or CLEAR
	Succeed bool // Valid only for CHECK and Known
	Delay   int  // Valid only for CHECK and Known
}

// State for the RPC controller goroutine

var (
	rpcInitialized sync.Once
	reqChan        chan CtrlReq
	resChan        chan CtrlRes
)

// Run in its own goroutine
// Serialized on reqChan/resChan pair
// Once the rpcController launches, it lives until the process exits
func rpcController() {

	targets := make(map[string]CtrlReq)

	for {
		req := <-reqChan
		// Proceess Command
		var res CtrlRes

		switch req.Cmd {
		case SET:
			rpcDPrintf("Setting parameters: %v\n", req)
			targets[req.Name] = req
		case CLEAR:
			rpcDPrintf("Clearing parameters: %v\n", req)
			_, ok := targets[req.Name]
			res.Known = ok
			if res.Known {
				delete(targets, req.Name)
			}
		case CHECK:
			rpcDPrintf("Checking: %v\n", req)
			state, ok := targets[req.Name]
			res.Known = ok
			if res.Known {
				// Set delay
				res.Delay = state.Delay
				// True if dice roll succeeds...
				if (rand.Int63() % 1000) >= int64(state.FailProb*1000) {
					rpcDPrintf("Passed probability filter\n")
					res.Succeed = true
					// ...and it is not over-limit
					if state.Limited {
						if state.TimeToLive == 0 {
							rpcDPrintf("But not TTL filter\n")
							res.Succeed = false
						} else {
							// Account for one more "used" RPC
							rpcDPrintf("And passed TTL filter\n")
							state.TimeToLive -= 1
							targets[req.Name] = state
						}
					}
				}
			}
		}
		rpcDPrintf("Controller result %v\n", res)
		resChan <- res
	}
}

func rpcInitialize() {
	rpcInitialized.Do(func() {
		rpcDPrintf("Initializing\n")
		reqChan = make(chan CtrlReq)
		resChan = make(chan CtrlRes)
		go rpcController()
	})
}

// Call() sends an RPC to the rpcname handler on server named in srv
// with arguments args, waits for the reply, and leaves the reply in
// reply. the reply argument should be the address of a reply
// structure.
//
// # Call() uses the channels in srv
//
// Call() returns true if the server responded, and false if call()
// was not able to obtain a reply from the server. Therefore reply's
// contents are valid if and only if call() returned true.
//
// Use Call() to send all RPCs.  Don't change this function.
//
// The type "interface{}" is the (static) generic go type, but objects
// retain their dynamic types.
func Call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {

	rpcInitialize()

	rpcDPrintf("Calling %s on %s\n", rpcname, srv)

	// Should this call succeed
	check := CtrlReq{Cmd: CHECK, Name: srv}
	reqChan <- check
	proceed := <-resChan

	if proceed.Known {
		if !proceed.Succeed {
			rpcDPrintf("Call failed\n")
			return false
		}
		if proceed.Delay != 0 {
			rpcDPrintf("Call sleeping %d\n", proceed.Delay)
			time.Sleep(time.Duration(proceed.Delay) * time.Second)
		}
	}

	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		rpcDPrintf("Call succeeded\n")
		return true
	}

	rpcDPrintf("%v", err)
	return false
}

// SetConn() registers an RPC endpoint, with failure probability and/or a
// time-to-live counter for the nubmer of RPCs returned until
// failure.
//
// Setting failProb to zero means "do not fail". Setting it to one (or
// greater) means "disconnect".
//
// If TTL is non-zero and positive, will allow ttl successful RPCs,
// and then fail all subsequent RPCs until another SetConn clears or
// resets it.
//
// An endpoint that is not registered does not fail due to simulated
// network errors.

func SetConn(srv string, failProb float64, ttl int, delay int) {

	rpcInitialize()

	var req CtrlReq

	req.Cmd = SET
	req.Name = srv
	req.FailProb = failProb
	req.Delay = delay
	if ttl > 0 {
		req.Limited = true // default is false for zero-value
		req.TimeToLive = ttl
	}

	reqChan <- req
	<-resChan
}

// Clear() removes the named endpoint from the registered set. Calling
// with an unregistered endpoing is a no-op.

func Clear(srv string) {

	rpcInitialize()

	var req CtrlReq

	req.Cmd = CLEAR
	req.Name = srv

	reqChan <- req
	<-resChan
}
