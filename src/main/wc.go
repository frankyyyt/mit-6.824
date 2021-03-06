package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strings"
	"container/list"
	"strconv"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func mapF(filename string, contents string) []mapreduce.KeyValue {
	// TODO: you have to write this function
	words := strings.Fields(contents)
	tmpList :=list.New()
	for _,w := range words {
		var kv mapreduce.KeyValue
		kv.Key = w
		kv.Value="1"
		tmpList.PushBack(kv)
	}
	kvs := make([]mapreduce.KeyValue,tmpList.Len())

	for e:=tmpList.Front();e != nil; e =e.Next(){
		kvs =append(kvs,e.Value.(mapreduce.KeyValue))
	}
	return kvs
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func reduceF(key string, values []string) string {
	// TODO: you also have to write this function
	sum :=0
	for _,ele := range values{
		tmp,_ := strconv.Atoi(ele)
		sum += tmp
	}
	fmt.Printf("reduceF:%s -%d\n",key,sum)
	return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {

	var mr *mapreduce.Master
	//mr= mapreduce.Sequential("wcseq",[]string{"/home/andy/mit/6.824/src/main/wc_test.dat"},3,mapF,reduceF);
	//mr.Wait()
	//return
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {

		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("wcseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
