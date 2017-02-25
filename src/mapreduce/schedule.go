package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
//func (mr *Master)schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
func (mr *Master)schedule(phase jobPhase){
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	mapFiles := mr.files
	nReduce :=mr.nReduce
	jobName :=mr.jobName
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	ch :=make(chan string)
	go mr.forwardRegistrations(ch)
	var wg  sync.WaitGroup
	for i:=0;i<ntasks;i=i+1{
		fmt.Printf("call worker for %s :%s - %d (%d)\n",phase,jobName,i,ntasks)

		wg.Add(1)

		go func(taskNum int,nios int,phase jobPhase) {
			taskfileName :=mapFiles[taskNum]
			srv:=<-ch
			fmt.Printf("files number :%d\n",len(mapFiles))

			fmt.Printf("task file name:%s\n",taskfileName)
			ret :=call(srv, "Worker.DoTask", DoTaskArgs{
				JobName: jobName, File: taskfileName, Phase: phase, TaskNumber: taskNum, NumOtherPhase: nios,
			}, nil)
			if ret {
				go func(){
					ch <- srv
					fmt.Printf("call completed: %s\n",srv)
				}()


			}

			defer wg.Done()
		}(i,n_other,phase)
	}
	wg.Wait()
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
