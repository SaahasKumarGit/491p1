package mrdistrib

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"

	"umich.edu/eecs491/proj1/mapreduce"
)

const mgrDebug = 0

func mgrDPrintf(format string, a ...interface{}) (n int, err error) {
	if mgrDebug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Archiecture:
//
// RunManager launces a distributed MR job. The caller must have
// initialized the MR job, up to and including splitting the input
// into the component input files for the Map jobs.
//
// RunManager creates the following goroutines:
//
//     * An RPC server pair for worker registration
//     * The Manager goroutine
//
// Note that the worker registration server does not need to
// serlialize RPC calls. Those are handled by a goroutine in
// manager_impl, via an unbuffered channel. Unlike most server
// serializers, this does not have a response channel, because a
// response is not required.
//
// Manager is a simple shell that calls ManagerImpl
// synchronously. ManagerImpl must set up a mechanism to (a) handle
// worker registrations and (b) assign Map and Reduce jobs to
// available workers. A worker should be considered available once it
// is registered, *even if* calls to it have failed in the
// past. ManagerImpl should not return until all assigned jobs have
// been completed. See the spec for hints about how to structure
// ManagerImpl. ManagerImpl should not return until any goroutines it
// has spawned have terminated; use the termination channel/WaitGroup
// idiom for this.
//
// RunManager returns a termination channel that is closed when the
// dsitributed map reduce job is complete. Manager closes this channel
// when ManagerImpl returns. As a side effect, that closed channel
// also terminates the worker registration pair launced by
// RunManager.
//
// The caller is expected to wait on that termination channel, and
// must call Merge to combine the outputs of the Reduce jobs after
// that channel is closed.

type Manager struct {
	name     string
	register chan string
}

func RunManager(task mapreduce.MapReduce, port string) chan bool {
	termination := make(chan bool)    // used to signal caller task is done
	registration := make(chan string) // used by student to register workers

	mgr := new(Manager)
	mgr.name = port
	mgr.register = registration

	// Set up listen socket
	rpcSrv := rpc.NewServer()
	rpcSrv.Register(mgr)
	os.Remove(port) // Allow later re-use of this socket

	l, err := net.Listen("unix", port)
	errCheck(err)

	// Account for listener and listener-monitor
	var wg sync.WaitGroup
	wg.Add(2)
	
	// Launch listener-monitor
	go func() {
		defer wg.Done()
		<-termination
		l.Close()
	}()

	// Launch listener
	go func() {
		defer wg.Done()
		mgrDPrintf("%s is listening\n", port)
		for {
			conn, err := l.Accept()
			if err != nil {
				mgrDPrintf("%s is no longer listening\n", port)
				break
			}
			go rpcSrv.ServeConn(conn)
		}
	}()

	// Launch manager shell: call student code, close termination
	// channel when done
	go func() {
		task.Split()
		ManagerImpl(task, registration)
		task.Merge()
		close(termination) // terminates monitor and listener
		wg.Wait() // Wait for them to exit
	}()

	return termination
}

// RPC handler
//
// Note that this handler is not serialized, and copies of it may be
// running concurrently. However, that is safe to do so because (a)
// the Manager state is read-only and (b) any channel type 
// is safe to use concurrently.

func (mgr *Manager) Register(arg RegisterArgs, res *RegisterReply) error {
	mgr.register <- arg.Worker
	res.OK = true

	return nil
}
