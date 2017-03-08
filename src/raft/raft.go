package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"fmt"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type logEntry struct {
	LogIndex int
	LogTerm   int
	LogComd interface{}
}

const STATE_FOLLOWER = 0
const STATE_CANDIDATE =1
const STATE_LEADER =2
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	mReqVote sync.Mutex
	lockAppendEntry sync.Mutex
	lockAppendEntryHandler sync.Mutex
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


	chanHb		chan bool
	chanGrantVote	chan bool
	chanLeader	chan bool
	chanApply	chan ApplyMsg
	chanCommit 	chan bool

	// persistent state on all servers
	currentTerm 	int
	votedFor	int //candidateId that received in current Term or null is none
	logs	[]logEntry   //log entries;each entry contains command for state machine,and Term
	                   //when entry was received by leader(first index is 1)

	//volitle state on all servers
	commitIndex 	 int //index of highest log entry known to commited(initialized to 0,increase monotonically)
	lastApplied      int //index of highest log entry applied to state machine (0,monotonically)

	//volatile state on leaders:reinitialized after election
	nextIndexs 	[]int //index of next log entry to send to that server for each server
	matchIndexs	[]int //index of highest log entry known to replicated on server for each server
	reset_count      bool
	state 		 int
	vote_counter	int
}
func (rf *Raft) getLastIndex() int{
	return rf.logs[len(rf.logs) -1].LogIndex;
}

func (rf *Raft) getLastTerm() int{
	return rf.logs[len(rf.logs) -1].LogTerm;
}
func (rf *Raft) IsLeader() bool {
	return rf.state == STATE_LEADER
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//DPrintf("%d getState:%t\n",rf.me,)
	term = rf.currentTerm
	isleader = rf.IsLeader()
	DPrintf("%d getState:%t\n",rf.me,isleader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	DPrintf("read persist:\n")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidatedId int
	Term         int
	LastLogIndex int //candidate`s last log entry
	LastLogTerm  int //candidate`s last log entry Term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int   	//currentTerm for candidate to update itself
	VoteGranted	bool	// voted

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mReqVote.Lock()
	defer rf.mReqVote.Unlock()
	DPrintf("%d(%d) vote for %d (%d)",rf.me,rf.currentTerm,args.CandidatedId,args.Term)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false;
		DPrintf("RequestVote %d : vote for %d as %t at term %d\n",rf.me,args.CandidatedId,reply.VoteGranted,rf.currentTerm)
		return
	}
	reply.VoteGranted = false
	if rf.currentTerm <args.Term{
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
	}

	term := rf.getLastTerm()
	index := rf.getLastIndex()
	// := moreUpToDate(rf.getLastIndex(), rf.getLastTerm(), args.LastLogIndex, args.LastLogTerm)
	uptoDate := false

	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index { // at least up to date
		uptoDate = true
	}
	if uptoDate && (rf.votedFor == -1 || rf.votedFor ==args.CandidatedId){//
		reply.VoteGranted = true
	}
	if reply.VoteGranted{
		rf.chanGrantVote<-true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidatedId

	}



	//DPrintf("RequestVote %d : vote for %d as %t \n",rf.me,args.CandidatedId,reply.VoteGranted)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("11-sendRequestVote to server(%d) %d:\n",rf.me,server)

	DPrintf("sendRequestVote: server(%d)  send vote req to %d at term %d\n",rf.me,server,args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.VoteGranted {
			rf.vote_counter++
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = STATE_FOLLOWER
		}
		DPrintf("server %d request vote :%t\n",rf.me, reply.VoteGranted)
		if rf.vote_counter > len(rf.peers)/2 {
			DPrintf("sever %d winner election\n", rf.me)
			//server.state = STATE_FOLLOWER
			rf.chanLeader<-true
		}
	}
	//DPrintf("size of majority:%d\n", len(server.peers)/2)

	return ok
}

func (server *Raft) broadcastRequestVotes(){
	server.currentTerm++
	server.vote_counter = 0
	server.votedFor = server.me
	var args RequestVoteArgs
	args.CandidatedId = server.me
	args.Term = server.currentTerm
	args.LastLogIndex = server.lastApplied
	args.LastLogTerm = server.commitIndex
	DPrintf("%d broadcast request votes at term %d\n",server.me,server.currentTerm)

	//sg := sync.WaitGroup{}
	for idx, _ := range server.peers {

		if idx == server.me {
			server.vote_counter++ //vote for itself
			continue
		}
		//sg.Add(1)
		go func(i int,reqArgs RequestVoteArgs) {
			var reply RequestVoteReply
			server.sendRequestVote(i, &reqArgs, &reply)

			//sg.Done()

		}(idx,args)
	}
	//sg.Wait()
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
type AppendEntryReq struct {
	Term 		int
	LeaderId 	int
	PreLogIndex	int
	Entries		[]logEntry
	PrevLogTerm	int
	LeaderCommit	int
}
type AppendEntryReply struct {
	Term		int
	Success		bool
	NextIndex	int
}

func (rf *Raft) AppendEntryHandler(args *AppendEntryReq,reply *AppendEntryReply){
	rf.lockAppendEntryHandler.Lock()
	defer rf.lockAppendEntryHandler.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = true
	if args ==nil{
		DPrintf("%d args is null\n",rf.me)
		panic(args)
	}
	if args.Term < rf.currentTerm{
		reply.Success = false
		DPrintf("%d leader term less than %d",args.LeaderId,rf.me)
		return
	}
	rf.chanHb<-true
	reply.Term = rf.currentTerm

	//发送后面剩下的，重新加入的node
	if args.PreLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		DPrintf("%d leader prelogindex%d < rf.getlastindex %d",rf.me,args.PreLogIndex,rf.getLastIndex())
		return
	}

	//has_prelog :=false;
	//if rf.getLastTerm() == args.PreLogIndex && rf.getLastTerm() == args.PrevLogTerm{
	//	has_prelog = true
	//}else{
	//	for _,ele :=range rf.logs{
	//		if ele.LogIndex == args.PreLogIndex && ele.LogTerm == args.PrevLogTerm{
	//			has_prelog = true
	//		}
	//	}
	//}

	//if has_prelog{
	//
	//	DPrintf("server %d has prelog of leader %d ,append log to",rf.me,args.PreLogIndex)
	//	for _,ele := range args.Entries{
	//		rf.logs =append(rf.logs,ele)
	//
	//	}
	//
	//}
	//DPrintf("%d logs length is %d %d\n",rf.me,len(args.Entries),args.Entries)
	DPrintf("%d RECV %v\n",rf.me,args)
	if args.PreLogIndex == rf.getLastIndex() && args.PrevLogTerm == rf.getLastTerm(){
		if len(args.Entries) >0{
			DPrintf("%d append new %d entry",rf.me,len(args.Entries))
			rf.logs = append(rf.logs,args.Entries...)
		}
	}
	//DPrintf("%d lastapply id is len %d -%d",rf.me,len(rf.logs),rf.getLastIndex())
	//DPrintf("append log  %d to %d",ele.LogIndex,rf.me)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit >rf.getLastIndex(){
			rf.commitIndex = rf.getLastIndex()
		}else{
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit<-true
	}
	reply.NextIndex = rf.getLastIndex()+1
	reply.Success =true
	//DPrintf("get hb and write to chanHb for serve %d",rf.me)

}
func (rf *Raft) sendAppendEntry(server int, args AppendEntryReq, reply *AppendEntryReply) bool {

	if rf.state != STATE_LEADER{
		DPrintf("%d is not leader state =%d",rf.me,rf.state)
		return false;
	}
	//DPrintf("LEADER:(%d) sendHB to %d at %d\n",rf.me,server,time.Now().Nanosecond())
	DPrintf("%d SEND TO %d :%v\n",rf.me,server,args)
	ok := rf.peers[server].Call("Raft.AppendEntryHandler", &args, reply)
	rf.lockAppendEntry.Lock()
	defer rf.lockAppendEntry.Unlock()
	if ok{
		if reply.Term >rf.currentTerm{
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			DPrintf("%d change state to follower\n",rf.me)
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				DPrintf("%d %d nextIndex %d",rf.me,server,reply.NextIndex)
				rf.nextIndexs[server] = reply.NextIndex
				//reply.NextIndex
				//rf.nextIndex[server] = reply.NextIndex
				rf.matchIndexs[server] = rf.nextIndexs[server] - 1
			}
		} else {
			rf.nextIndexs[server] = reply.NextIndex
			rf.matchIndexs[server] = rf.nextIndexs[server] - 1
		}
	}
	DPrintf("%d SEND to %d completed",rf.me,server)
	return ok
}



func (rf *Raft) broadAppendEntries(){

	newCommitIdx:=rf.commitIndex
	baseIdx :=rf.logs[0].LogIndex
	last :=rf.getLastIndex()
	for i:=rf.commitIndex;i<=last;i++{
		num:=1
		for j:= range rf.peers{
			if j!= rf.me && rf.matchIndexs[j] >=i && rf.logs[i -baseIdx].LogTerm == rf.currentTerm{
				num++
			}
		}
		if 2*num >len(rf.peers){
			newCommitIdx =i
		}
	}
	if newCommitIdx != rf.commitIndex{
		rf.commitIndex = newCommitIdx
		rf.chanCommit <-true
		DPrintf("%d docommit to %d\n",rf.me,rf.commitIndex)
	}
	for idx,_ :=range rf.peers{
		if idx == rf.me{
			continue
		}

		go func(peerIdx int) {
			var args AppendEntryReq
			args.Term = rf.currentTerm;
			args.LeaderCommit = rf.commitIndex
			args.PreLogIndex = rf.nextIndexs[peerIdx]-1
			args.PrevLogTerm = rf.logs[args.PreLogIndex].LogTerm
			args.LeaderId = rf.me
			//rf.getLastIndex()-args.PreLogIndex
			DPrintf("appendEntry:%d -%d nextId %d\n",rf.me,peerIdx,rf.nextIndexs[peerIdx])
			args.Entries = make([]logEntry,len(rf.logs[args.PreLogIndex + 1:]))
			copy(args.Entries,rf.logs[args.PreLogIndex+1:])
			//DPrintf("args :%v\n",args)
			var reply AppendEntryReply
			rf.sendAppendEntry(peerIdx,args,&reply)

		}(idx)
	}


}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.IsLeader()
	// Your code here (2B).



	if isLeader{
		var entry logEntry
		index = rf.getLastIndex()+1
		entry.LogComd=command
		entry.LogIndex=rf.getLastIndex()+1
		entry.LogTerm = rf.currentTerm
		rf.logs = append(rf.logs,entry)
		DPrintf("%d ADD %v\n",rf.me,entry)
	}


	term = rf.currentTerm
	fmt.Printf("%d ,index=%d ,term=%d,isLeader=%t\n",rf.me,index,term,isLeader)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("Kill\n")
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	DPrintf("Make:init raft for -%d \n",me)
	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = 0 //0: follower ;1 :candidated;2:leader
	rf.votedFor = -1
	rf.logs = append(rf.logs,logEntry{LogTerm:rf.currentTerm,LogIndex:rf.lastApplied,LogComd:100})
	rf.chanGrantVote = make(chan bool,100)
	rf.chanHb = make(chan bool,100)
	rf.chanLeader = make(chan bool,100)
	rf.chanCommit=make(chan bool,100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())



	go func(server *Raft){
		for{

			//selection timeout

			//randNum :=rand.Intn(150)
			////DPrintf("server %d :sleep for %d ms\n",server.me,randNum)
			//time.Sleep(time.Duration(150 +randNum) * time.Millisecond)
			//DPrintf("server %d start for main state =%d at %d ",server.me,server.state,
			//	time.Now().Nanosecond())
			//DPrintf("%d state is %d",rf.me,rf.state)
			switch server.state {
			case STATE_FOLLOWER:
				randNum :=550+rand.Int()%333
				//DPrintf("%d timeafter %d\n",server.me,randNum)
				select {
				case <-server.chanHb:
					DPrintf("XXXXXXXX:%d get hb at %d",server.me,time.Now().Nanosecond())
				case <-server.chanGrantVote:
				case <-time.After(time.Duration(randNum)* time.Millisecond):

					DPrintf("START ELECTION:server %d change state to %d at %d\n",
						server.me,STATE_CANDIDATE,time.Now().Nanosecond())
					server.state = STATE_CANDIDATE
					server.votedFor = -1
				}
			case STATE_CANDIDATE:

				go server.broadcastRequestVotes()
				randNum :=550+rand.Int()%333
				DPrintf("%d nexttime election after %d\n",server.me,randNum)
				select {
				case <-time.After(time.Duration(randNum) * time.Millisecond):
					DPrintf("%d timeout and re req vote at %d",server.me,time.Now().Nanosecond())
				case <-server.chanHb:
					server.state = STATE_FOLLOWER
				case <-server.chanLeader:
					server.state = STATE_LEADER
					rf.nextIndexs = make([]int,len(rf.peers))
					rf.matchIndexs = make([]int,len(rf.peers))
					for idx,_ := range server.peers{
						server.nextIndexs[idx] = server.getLastIndex()+1
						server.matchIndexs[idx] =0
					}
					DPrintf("server %d:to be leader\n",server.me)
				}
			case STATE_LEADER:
				//DPrintf("server %d: is leader,starting to send hb..\n",server.me)
				server.broadAppendEntries()
				time.Sleep(time.Duration(10 * time.Millisecond))
			}
			//DPrintf("%d againg ..........at %d",server.me,time.Now().Nanosecond())
		}
	}(rf)
	go func(){
		for{
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				baseIndex := rf.logs[0].LogIndex
				for i := rf.lastApplied+1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.logs[i-baseIndex].LogComd}
					applyCh <- msg
					//fmt.Printf("me:%d %v\n",rf.me,msg)
					rf.lastApplied = i
					DPrintf("APPLY %d apply msg %v",rf.me,msg)
				}
				rf.mu.Unlock()
			}
		}

	}()


	return rf
}
