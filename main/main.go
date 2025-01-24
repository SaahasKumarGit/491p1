package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"umich.edu/eecs491/proj1/mapreduce"
	"umich.edu/eecs491/proj1/wordcount"
)

func errCheck(err error, v ...any) {
	if err != nil {
		log.Fatal("WC:", v, err)
	}
}

// Invoke a sequential map-reduce job
// Usage: go run main.go [file] [# map jobs] [# reduce jobs]
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("Usage: go run main.go  [file] [# map jobs] [# reduce jobs]\n")
	} else {
		nMap, err := strconv.ParseInt(os.Args[2], 0, 0)
		errCheck(err, "Parsing # map jobs")
		nReduce, err := strconv.ParseInt(os.Args[3], 0, 0)
		errCheck(err, "Parsing # reduce jobs")

		mapreduce.DoSequential(int(nMap), int(nReduce), os.Args[1],
			wordcount.Map, wordcount.Reduce)
	}
}
