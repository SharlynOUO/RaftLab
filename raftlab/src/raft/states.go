package raft

import (
	"sync/atomic"
)

const (
	Candidate int = 0
	Follower  int = 1
	Leader    int = 2
)

const (
	connected uint32 = 0
	unconnect uint32 = 1
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.identity == Leader

	return term, isleader
}

func (rf *Raft) TransformToCandidate() {
	rf.identity = Candidate
}

func (rf *Raft) TransformToFollower() {
	rf.identity = Follower
}

func (rf *Raft) TransformToLeader() {
	rf.identity = Leader
}

func (rf *Raft) LeaderServerInit() {

	servernum := len(rf.peers)
	logslen := len(rf.logs)

	rf.nextIndex = make([]int, servernum)
	rf.matchIndex = make([]int, servernum)

	for i := 0; i < servernum; i++ {
		rf.nextIndex[i] = logslen //rf.LastLogIndex+1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = logslen - 1 //否则某些log会失效导致test失败
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
func (rf *Raft) SendHeartBeatToOneServer(server int, heartargs *RequestLogAppendArgs, connectReport *uint32, term int) {
	heartreps := RequestLogAppendReply{}
	if rf.SendRequestLogAppend(server, heartargs, &heartreps) {
		//println(server, "connect")
		atomic.StoreUint32(connectReport, connected)
	} else {
		//println(server, "unconnect")
		atomic.StoreUint32(connectReport, unconnect)
	}

}*/
