package wordcount

import (
	"umich.edu/eecs491/proj1/mapreduce"
)

// our simplified version of MapReduce does not supply a key to the
// Map function, as in Google's MapReduce paper; only an "input",
// which is a portion of the input file's content
func Map(input string) []mapreduce.KeyValue {
}

// called once for each key generated by Map, with a list of that
// key's values. should return a single output value for that key.
func Reduce(key string, values []string) string {
}
