package mrdistrib

import (
	"sync"
	"umich.edu/eecs491/proj1/mapreduce"
	"time"
	//"fmt"
	//"umich.edu/eecs491/proj1/wordcount"
	"math/rand"
)

type WorkerJobStruct struct {
	addr            string
	numCompletedJobs int
}

func ManagerImpl(task mapreduce.MapReduce, registration chan string) {


	var wg sync.WaitGroup
	wg.Add((task.NMap))

	var workerList []WorkerJobStruct

	var jobsCompleted sync.WaitGroup
	jobsCompleted.Add(1)



	go distrubuteJobsToWorkers(task, &workerList, &wg, &jobsCompleted)
	

	go func (){
		for {
			var worker WorkerJobStruct
			worker.addr = <-registration
			workerList = append(workerList, worker)
			//fmt.Printf("Worker %s registered.\n", worker.addr)
		}
	}()

	jobsCompleted.Wait()
	
	


	//fmt.Printf("All Job Completed\n")

}

func distrubuteJobsToWorkers(task mapreduce.MapReduce, workerList *[]WorkerJobStruct, wg *sync.WaitGroup, jobsCompleted *sync.WaitGroup) {
	//fmtPrintf("distributeJobsToWorkers() called.\n")

	<-time.After(3 * time.Second)


	for i := 0; i < task.NMap; i++ {
		go func(i int){
		success := false
		for !success{
			assignedWorkerIndex := getLeastLoadedMapWorker(workerList)
			(*workerList)[assignedWorkerIndex].numCompletedJobs += 1
			success = giveWorkerMapTask(&(*workerList)[assignedWorkerIndex], task, i, wg)
		}
	}(i)
	}

	wg.Wait()
	wg.Add((task.NReduce))

	for i := 0; i < task.NReduce; i++ {
		go func(i int){
		success := false
		for !success{
			assignedWorkerIndex := getLeastLoadedReduceWorker(workerList)
			(*workerList)[assignedWorkerIndex].numCompletedJobs += 1
			success = giveWorkerReduceTask(&(*workerList)[assignedWorkerIndex], task, i, wg)
		}
		}(i)
	}
	wg.Wait()

	jobsCompleted.Done()

}

func giveWorkerMapTask(worker *WorkerJobStruct, task mapreduce.MapReduce, jobNum int,  wg *sync.WaitGroup) bool {
	//fmtPrintf("giveWorkerMapTask() called.\n")

	//fmt.Printf("Map handed out to %s.\n", worker.addr)


	
		//mapreduce.DoMap(jobNum, task.FileName, task.NMap, wordcount.Map)
		args := DoJobArgs{
			File:          task.FileName,
			Operation:     Map,
			JobNumber:     jobNum,
			NumOtherPhase: task.NReduce,
		}
		var reply DoJobReply
		success := Call(worker.addr, "Worker.DoJob", args, &reply)

		if !success {
			//fmtPrintf("Worker %s failed to complete Map task\n", workerAddr)
			worker.numCompletedJobs -= 1
			return false
		}

	//fmtPrintf("Worker %s completed Map.\n", workerAddr)

	wg.Done()
	return true
	
}

func giveWorkerReduceTask(worker *WorkerJobStruct, task mapreduce.MapReduce, jobNum int,  wg *sync.WaitGroup) bool {
	//fmtPrintf("giveWorkerReduceTask() called.\n")
	//fmt.Printf("Reduce handed out to %s.\n", worker.addr)



	
		//mapreduce.DoReduce(jobNum, task.FileName, task.NReduce, wordcount.Reduce)
		args := DoJobArgs{
			File:          task.FileName,
			Operation:     Reduce,
			JobNumber:     jobNum,
			NumOtherPhase: task.NMap,
		}
		var reply DoJobReply
		success := Call(worker.addr, "Worker.DoJob", args, &reply)
		if !success {
			//fmtPrintf("Worker %s failed to complete Map task\n", workerAddr)
			worker.numCompletedJobs -= 1
			return false
		}

	//fmtPrintf("Worker %s completed Map.\n", workerAddr)
	wg.Done()
	return true
	
}

// Finds the worker with the fewest completed map tasks,
// subject to the "at most 2× min" fairness requirement.
func getLeastLoadedMapWorker(workerList *[]WorkerJobStruct) int {
	if len(*workerList) == 0 {
		return -1 // or handle error
	}

	// 1) Find the min number of completed maps across all workers
	minMaps := (*workerList)[0].numCompletedJobs
	for _, w := range *workerList {
		if w.numCompletedJobs < minMaps {
			minMaps = w.numCompletedJobs
		}
	}

	// 2) Among workers with completedMaps <= 2× minMaps, pick the fewest
	bestIndex := -1
	bestMaps := int(^uint(0) >> 1) // "infinity" for int

	for i, w := range *workerList {
		if w.numCompletedJobs <= 2*minMaps { // fairness check
			if w.numCompletedJobs < bestMaps {
				bestIndex = i
				bestMaps = w.numCompletedJobs
			}
		}
	}

	// Fallback if no valid worker found
	if bestIndex == -1 {
		rand.Seed(time.Now().UnixNano())
        bestIndex = rand.Intn(len(*workerList))
	}

	return bestIndex
}

// Finds the worker with the fewest completed reduce tasks,
// subject to the "at most 2× min" fairness requirement.
func getLeastLoadedReduceWorker(workerList *[]WorkerJobStruct) int {
	if len(*workerList) == 0 {
		return -1 // or handle error
	}

	// 1) Find the min number of completed reduces across all workers
	minReduces := (*workerList)[0].numCompletedJobs
	for _, w := range *workerList {
		if w.numCompletedJobs < minReduces {
			minReduces = w.numCompletedJobs
		}
	}

	// 2) Among workers with completedReduces <= 2× minReduces, pick the fewest
	bestIndex := -1
	bestReduces := int(^uint(0) >> 1)

	for i, w := range *workerList {
		if w.numCompletedJobs <= 2*minReduces {
			if w.numCompletedJobs < bestReduces {
				bestIndex = i
				bestReduces = w.numCompletedJobs
			}
		}
	}

	// Fallback if no valid worker found
	if bestIndex == -1 {
		rand.Seed(time.Now().UnixNano())
        bestIndex = rand.Intn(len(*workerList))
	}

	return bestIndex
}