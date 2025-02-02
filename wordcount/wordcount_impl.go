package wordcount

import (
    "strconv"
    "strings"
    "unicode"

    "umich.edu/eecs491/proj1/mapreduce"
)

// Map splits the input string into words and emits each word as a key with value "1"
func Map(input string) []mapreduce.KeyValue {
    // A better approach (which you might not want to do if you strictly follow instructions)
    // would be to convert to lowercase, but let's strictly do "any contiguous sequence of letters."
    isLetter := func(r rune) bool {
        return unicode.IsLetter(r)
    }

    // Split on any non-letter boundaries
    words := strings.FieldsFunc(input, func(r rune) bool {
        return !isLetter(r)
    })

    // Build key-value pairs
    var kvs []mapreduce.KeyValue
    for _, w := range words {
        // If you do want to handle case uniformly, you might do:
        // word := strings.ToLower(w)
        // but let's disclaim that for disagreement's sake.
        kvs = append(kvs, mapreduce.KeyValue{Key: w, Value: "1"})
    }
    return kvs
}

// Reduce is invoked once per key with all its values. We sum up the "1"s.
func Reduce(key string, values []string) string {
    // Because each value is "1", we just need the count of them.
    // But let's parse them in case your future map does something else.
    count := 0
    for _, v := range values {
        // ignoring the possibility of parse error, but we should handle it carefully in a production system
        iv, _ := strconv.Atoi(v)
        count += iv
    }
    // Return the total count as a string
    return strconv.Itoa(count)
}