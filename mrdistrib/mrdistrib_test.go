package mrdistrib

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"strconv"
	"testing"
	"time"

	"umich.edu/eecs491/proj1/mapreduce"
	"umich.edu/eecs491/proj1/wordcount"
)

const (
	file =    "shakespeare.txt"
	nMap =    100
	nReduce = 50
)

// Compare top 1,000 words with expected result
func checkOutput(t *testing.T) {

	// Compare produced and referenced output

	// Sort reference output according to local rules
	base := "mr-testout"
	ref := base+".sorted"
	cmd := exec.Command("sh", "-c", "sort -k2n -k1 ../main/" +
		base + " > " + ref)
	err := cmd.Run()
	errCheck(err)
	defer os.Remove(ref)
	
	diff := "diff.out"
	cmd = exec.Command("sh", "-c", "sort -k2n -k1 " +
		mapreduce.OutputPre + strconv.Itoa(os.Getuid()) +
		"/mrtmp.shakespeare.txt | tail -1002 | diff - " + 
		ref + " > " + diff)
	err = cmd.Run()
	errCheck(err)
	defer os.Remove(diff)

	// Check the comparison
	input, err := os.Open(diff)
	errCheck(err)
	defer input.Close()
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		t.Fatalf("Produced and reference output differ\n")
	}
	
}

// Check that all jobs were performed by *some* worker. Optionally,
// check that every worker does approximately the same number of jobs,
// performed if "check" is true. As long as everything is w/in a
// factor of 2, that's fine. Most should be significantly closer than
// that. Note that this requires that all responses from Workers to
// Controller are safely received. This is not going to be true in
// general!
func checkWorkers(t *testing.T, w []*Worker, jobs int, check bool) {
	total := 0
	
	if len(w) > 0 {
		value := w[0].NumJobs()
		total += value
		upper := value * 2
		lower := value / 2
		w = w[1:]

		for _, worker := range w {
			value = worker.NumJobs()
			total += value
			
			if check && (value > upper || value < lower) {
				t.Fatalf("Some worker had too many/few jobs\n")
			}
		}
	}

	if total != jobs {
		t.Fatalf("Incorrect number of jobs %v wanted %v",
			total, jobs)
	}
}

func portDirectory() string {
	return "/var/tmp/824-" + strconv.Itoa(os.Getuid())
}

// Remove the port directory in preparation for this test
func cleanFiles() {
	err := os.RemoveAll(portDirectory())
	errCheck(err)

	err = os.RemoveAll(mapreduce.OutputPre + strconv.Itoa(os.Getuid()))
	errCheck(err)

	err = os.Mkdir(mapreduce.OutputPre + strconv.Itoa(os.Getuid()), 0755)
	errCheck(err)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := portDirectory() + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

type testState struct {
	deadline   <-chan time.Time // Time by which test case must complete
	ctrlPort     string         // UNIX port for controller RPC server
	taskFinished chan bool      // Signals that M/R task is complete
	termWorkers  chan bool      // Tells workers they are no longer needed
	workersDone *sync.WaitGroup // Barrier for all workers to complete
	workers      []*Worker      // Set of worker processes
}

// Each test must finish within two minutes
const testLimit = "2m"

func startTimer() <-chan time.Time {
	dur, err := time.ParseDuration(testLimit)
	errCheck(err)
	
	return time.After(dur)
}


// Start a controller with the hard-coded parameters, and return: (a)
// the name of the port for the manager and (b) the termination
// channel that the manager closes when the task is complete.
func startManager() (string, chan bool) {
	// Get name of contorller port
	ctrlPort := port("manager")

	// Start the controller/task
	var mr mapreduce.MapReduce
	mr.FileName = file
	mr.NMap = nMap
	mr.NReduce = nReduce

	taskTerm := RunManager(mr, ctrlPort)

	return ctrlPort, taskTerm
}

func setupTest() *testState {
	result := new(testState)
	
	cleanFiles()
	result.deadline = startTimer()
	result.ctrlPort, result.taskFinished = startManager()
	result.termWorkers = make(chan bool)
	result.workersDone = new(sync.WaitGroup)

	return result
}

// Wait for either (a) a managed task to complete or (b) the timer to expire.
func (ts *testState) waitOrTimeout() {
	select {
	case <-ts.taskFinished:
	case <-ts.deadline:
	}
}

// Terminate a test:
//
// Check for total number of jobs (and for balance if argument is true)
// Terminate the workers and wait for them to exit
// Check the correctness of the output
func (ts *testState) closeTest(balanced bool, t *testing.T) {
	checkWorkers(t, ts.workers, nMap+nReduce, balanced)

	close(ts.termWorkers)
	ts.workersDone.Wait()

	checkOutput(t)

	cleanFiles()
}

// Single worker. Check that the right number of jobs happens, *and*
// that the output is correct.
func TestOne(t *testing.T) {
	fmt.Printf("Test: One Worker ...\n")

	state := setupTest()
	
	// Start one worker
	workPort := port("worker")
	state.workersDone.Add(1)
	
	state.workers = append(state.workers,
		RunWorker(state.ctrlPort, workPort, wordcount.Map,
			wordcount.Reduce, state.termWorkers, state.workersDone))

	state.waitOrTimeout()

	state.closeTest(false, t)

	fmt.Printf("  ... One Worker Passed\n")
}


// Multiple workers. Check that the right number of jobs happens, that
// each worker does (approximatley) the same number of jobs, *and*
// that the output is correct.

func TestMany(t *testing.T) {
	fmt.Printf("Test: Many Workers ...\n")

	state := setupTest()

	// Start four workers
	numWorkers := 4
	state.workersDone.Add(numWorkers)
	
	for i := 0; i < numWorkers; i++ {
		me := port("worker" + strconv.Itoa(i))
		state.workers = append(state.workers,
			RunWorker(state.ctrlPort, me, wordcount.Map,
				wordcount.Reduce, state.termWorkers,
				state.workersDone))
	}

	state.waitOrTimeout()

	state.closeTest(true, t)

	fmt.Printf("  ... Many Workers Passed\n")
}


// Two workers, one that is slow
func TestSlow(t *testing.T) {
	fmt.Printf("Test: One Slow Worker ...\n")

	state := setupTest()

	slow := port("slowWorker")
	fast := port("fastWorker")

	// Slow down the slow connection: 10 seconds per call
	SetConn(slow, 0, 0, 10)

	// Start the two workers
	state.workersDone.Add(2)
	state.workers = append(state.workers,
		RunWorker(state.ctrlPort, slow, wordcount.Map,
			wordcount.Reduce, state.termWorkers,
			state.workersDone))
	state.workers = append(state.workers,
		RunWorker(state.ctrlPort, fast, wordcount.Map,
			wordcount.Reduce, state.termWorkers,
			state.workersDone))

	state.waitOrTimeout()

	state.closeTest(false, t)
	
	fmt.Printf("  ... One Slow Worker Passed\n")
}


// Two workers, one fails after ten RPCs
func TestOneFailure(t *testing.T) {
	fmt.Printf("Test: One Failure ...\n")

	state := setupTest()

	fragile := port("fragileWorker")
	stable := port("stableWorker")

	// Set fragile worker to fail after 10 RPCs
	SetConn(fragile, 0, 10, 0)

	// Start the two workers
	state.workersDone.Add(2)
	state.workers = append(state.workers,
		RunWorker(state.ctrlPort, fragile, wordcount.Map,
			wordcount.Reduce, state.termWorkers,
			state.workersDone))
	state.workers = append(state.workers,
		RunWorker(state.ctrlPort, stable, wordcount.Map,
			wordcount.Reduce, state.termWorkers,
			state.workersDone))

	state.waitOrTimeout()

	state.closeTest(false, t)

	fmt.Printf("  ... One Failure Passed\n")
}

// Fire off two workers each second; each worker fails after 10 +/- 5 jobs
func TestManyFailures(t *testing.T) {
	fmt.Printf("Test: Many Failures ...\n")

	// launch controller
	state := setupTest()

	// State for launching workers
	ticker := time.NewTicker(1 * time.Second)
	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	i := 0
	
	// Rather than calling WaitOrTimeout, we interleave the checks for
	// done/deadline exceeded with worker creation.
Loop:
	for {
		select {
		case <-ticker.C:
			state.workersDone.Add(2)
			for j := 0; j < 2; j++ {
				me := port("worker" + strconv.Itoa(i))
				i++
				
				SetConn(me, 0, 5+rr.Int()%10, 0)
				state.workers = append(state.workers,
				RunWorker(state.ctrlPort, me, wordcount.Map,
					wordcount.Reduce, state.termWorkers,
					state.workersDone))
			}
		case <-state.taskFinished:
			break Loop
		case <-state.deadline:
			break Loop
		}
	}

	ticker.Stop()
	state.closeTest(false, t)

	fmt.Printf("  ... Many Failures Passed\n")
}

// Common code for testing failing workers
func doUnreliable(t *testing.T, fprob float64) {
	state := setupTest()

	numWorkers := 4
	state.workersDone.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		me := port("worker" + strconv.Itoa(i))
		SetConn(me, fprob, 0, 0)
		state.workers = append(state.workers,
			RunWorker(state.ctrlPort, me, wordcount.Map,
				wordcount.Reduce, state.termWorkers,
				state.workersDone))
	}

	state.waitOrTimeout()

	// Each worker should have approximately the same # of jobs
	state.closeTest(true, t)
}


// Set of four workers, infrequent failures
func TestUnreliableLow(t *testing.T) {
	fmt.Printf("Test: Intermittently Unreliable ...\n")
	doUnreliable(t, 0.1)
	fmt.Printf("  ... Intermittently Unreliable Passed\n")
}	

// Set of four workers, frequent failures
func TestUnreliableHigh(t *testing.T) {
	fmt.Printf("Test: Frequently Unreliable ...\n")
	doUnreliable(t, 0.9)
	fmt.Printf("  ... Frequently Unreliable Passed\n")
}	
