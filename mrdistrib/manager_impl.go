package mrdistrib

import (
	//"fmt"
	"sync"
	"umich.edu/eecs491/proj1/mapreduce"
	"umich.edu/eecs491/proj1/wordcount"
)

type WorkerJobStruct struct {
	addr            string
	numCompletedMaps int
	numCompletedReduces int
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
			//fmtPrintf("Worker %s registered.\n", workerAddr)
		}
	}()

	jobsCompleted.Wait()
	
	


	//fmt.Printf("All Job Completed\n")

}

func distrubuteJobsToWorkers(task mapreduce.MapReduce, workerList *[]WorkerJobStruct, wg *sync.WaitGroup, jobsCompleted *sync.WaitGroup) {
	//fmtPrintf("distributeJobsToWorkers() called.\n")
	for len(*workerList) < 1{

	}
	for i := 0; i < task.NMap; i++ {
		success := false
		for !success{
			assignedWorkerIndex := getLeastLoadedMapWorker(workerList)
			success = giveWorkerMapTask(&(*workerList)[assignedWorkerIndex], task, i, wg)
		}
	}

	wg.Wait()
	wg.Add((task.NReduce))

	for i := 0; i < task.NReduce; i++ {
		success := false
		for !success{
			assignedWorkerIndex := getLeastLoadedReduceWorker(workerList)
			success = giveWorkerReduceTask(&(*workerList)[assignedWorkerIndex], task, i, wg)
		}
	}

	jobsCompleted.Done()

}

func giveWorkerMapTask(worker *WorkerJobStruct, task mapreduce.MapReduce, jobNum int,  wg *sync.WaitGroup) bool {
	//fmtPrintf("giveWorkerMapTask() called.\n")

	//fmt.Printf("Map handed out to %s.\n", worker.addr)

	
		mapreduce.DoMap(jobNum, task.FileName, task.NMap, wordcount.Map)
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
			worker.numCompletedMaps += 1
			return false
		}

	//fmtPrintf("Worker %s completed Map.\n", workerAddr)

	wg.Done()
	worker.numCompletedMaps += 1
	return true
	
}

func giveWorkerReduceTask(worker *WorkerJobStruct, task mapreduce.MapReduce, jobNum int,  wg *sync.WaitGroup) bool {
	//fmtPrintf("giveWorkerReduceTask() called.\n")
	//fmt.Printf("Reduce handed out to %s.\n", worker.addr)

	
		mapreduce.DoReduce(jobNum, task.FileName, task.NReduce, wordcount.Reduce)
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
			worker.numCompletedReduces += 1
			return false
		}

	//fmtPrintf("Worker %s completed Map.\n", workerAddr)
	wg.Done()
	worker.numCompletedReduces += 1
	return true
	
}

// Finds the worker with the fewest completed map tasks,
// subject to the "at most 2× min" fairness requirement.
func getLeastLoadedMapWorker(workerList *[]WorkerJobStruct) int {
	if len(*workerList) == 0 {
		return -1 // or handle error
	}

	// 1) Find the min number of completed maps across all workers
	minMaps := (*workerList)[0].numCompletedMaps
	for _, w := range *workerList {
		if w.numCompletedMaps < minMaps {
			minMaps = w.numCompletedMaps
		}
	}

	// 2) Among workers with completedMaps <= 2× minMaps, pick the fewest
	bestIndex := -1
	bestMaps := int(^uint(0) >> 1) // "infinity" for int

	for i, w := range *workerList {
		if w.numCompletedMaps <= 2*minMaps { // fairness check
			if w.numCompletedMaps < bestMaps {
				bestIndex = i
				bestMaps = w.numCompletedMaps
			}
		}
	}

	// Fallback if no valid worker found
	if bestIndex == -1 {
		// e.g., choose the first worker
		return 0
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
	minReduces := (*workerList)[0].numCompletedReduces
	for _, w := range *workerList {
		if w.numCompletedReduces < minReduces {
			minReduces = w.numCompletedReduces
		}
	}

	// 2) Among workers with completedReduces <= 2× minReduces, pick the fewest
	bestIndex := -1
	bestReduces := int(^uint(0) >> 1)

	for i, w := range *workerList {
		if w.numCompletedReduces <= 2*minReduces {
			if w.numCompletedReduces < bestReduces {
				bestIndex = i
				bestReduces = w.numCompletedReduces
			}
		}
	}

	// Fallback if no valid worker found
	if bestIndex == -1 {
		return 0
	}

	return bestIndex
}
