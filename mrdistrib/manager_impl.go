package mrdistrib


import (
	"fmt"
	"umich.edu/eecs491/proj1/mapreduce"
	"umich.edu/eecs491/proj1/wordcount"
	
)



func ManagerImpl(task mapreduce.MapReduce, registration chan string) {


	workerAddr := <-registration


	for i := 0; i < task.NMap; i++ {
		mapreduce.DoMap(i, task.FileName, task.NReduce, wordcount.Map)
		args := DoJobArgs{
			File:          task.FileName,
			Operation:     Map,
			JobNumber:     i,
			NumOtherPhase: task.NReduce,
		}
		var reply DoJobReply
		success := Call(workerAddr, "Worker.DoJob", args, &reply)
		if !success {
			fmt.Printf("Failed to assign Map task %d to worker %s\n", i, workerAddr)
		}
		
	}
	
	fmt.Printf("Managers handed out\n")

  // Repeat for reduce:
    for i := 0; i < task.NReduce; i++ {
        //workerAddr := <-registration
        mapreduce.DoReduce(i, task.FileName, task.NMap, wordcount.Reduce)
		args := DoJobArgs{
			File:          task.FileName,
			Operation:     Reduce,
			JobNumber:     i,
			NumOtherPhase: task.NMap,
		}
		var reply DoJobReply
		success := Call(workerAddr, "Worker.DoJob", args, &reply)
		if !success {
			fmt.Printf("Failed to assign Reduce task %d to worker %s\n", i, workerAddr)
		}
    }

	fmt.Printf("Reducers handed out\n")
    // 2) Possibly the harness calls Merge() after manager is done
    //    If not guaranteed, do: task.Merge()

    fmt.Printf("Manager Completed\n")
}