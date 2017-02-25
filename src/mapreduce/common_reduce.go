package mapreduce


import (
	"fmt"
	"encoding/json"
	"os"
	"container/list"
	"sort"
	"log"
)
// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	fmt.Println("do reduce")
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	kvsMap :=make(map[string]*list.List)
	for i:=0;i<nMap;i=i+1{
		reduceFile :=reduceName(jobName,i,reduceTaskNumber)
		f,_ := os.Open(reduceFile)
		defer f.Close()
		finf,err :=f.Stat()
		if err != nil{
			log.Fatal("file is not exists")
			fmt.Printf("%s\n",finf.Name())
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			err :=dec.Decode(&kv)
			if err !=nil{
				break
			}
			l,ok := kvsMap[kv.Key]
			if ok{
				l.PushBack(kv.Value)
			}else{
				l = list.New()
				l.PushBack(kv.Value)
				kvsMap[kv.Key] = l
			}
		}
	}
	var keys []string
	for k,_ := range kvsMap{
		keys = append(keys,k)
	}
	sort.Strings(keys)
	var kvs []KeyValue
	for _,k := range keys{
		l := kvsMap[k]
		var values []string
		for itr:=l.Front();itr != nil;itr = itr.Next(){
			values = append(values,itr.Value.(string))
		}
		v :=reduceF(k,values)
		kvs = append(kvs,KeyValue{k,v})
	}
	outf,_ := os.Create(outFile)
	defer outf.Close()
	enc := json.NewEncoder(outf)
	for _,kv := range kvs{
		enc.Encode(kv)
		fmt.Printf("reduce enc:%s - %s\n",kv.Key,kv.Value)
	}
	return
}
