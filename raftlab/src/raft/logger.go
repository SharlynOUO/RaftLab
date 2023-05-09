package raft

import (
	"time"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

func GetNewLog(init_cmd interface{}, init_term int) *LogEntry {
	return &LogEntry{
		Command: init_cmd,
		Term:    init_term}
}

// return lastlog index and lastlog term
// 需要加锁
func (rf *Raft) GetLastLogInfo() (int, int) {
	idx := len(rf.logs) - 1
	return idx, rf.logs[idx].Term
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	//append new log
	newlog := GetNewLog(command, term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logs = append(rf.logs, newlog)
	index, _ = rf.GetLastLogInfo()
	me := rf.me
	rf.matchIndex[me] += 1
	servernum := len(rf.peers)
	//fmt.Printf("%d start %v\n", me, command)
	//rf.printlog()
	//then replicate logs
	//var comfirm_counter uint64
	//atomic.StoreUint64(&comfirm_counter, 1) //server itself has been updated,set 1
	for i := 0; i < servernum; i++ {
		if i == me {
			continue
		}
		go rf.ReplicateLogs(i)
	}
	/*PollingTime := time.Duration(30 * time.Millisecond)
	for isLeader {
		//轮询直到crush或者log可以应用到state machine
		//If command received from client: append entry to local log,
		// respond after entry applied to state machine
		confirmed := int(atomic.LoadUint64(&comfirm_counter))
		if quorum == 0 {
			//all followers have been unconnected
			return index, term, isLeader
		}
		if confirmed > quorum {
			go rf.committer()
			return index, term, isLeader
		}
		time.Sleep(PollingTime)
		term, isLeader = rf.GetState()
	}
	index = -1
	*/
	return index, term, isLeader
}

func (rf *Raft) ReplicateLogs(server int) { //, comfirm *uint64) {

	//var is_leader bool
	//_, is_leader = rf.GetState()

	rf.mu.Lock()
	next_i := rf.nextIndex[server]
	request_i := rf.GetLogAppendArgs(false, next_i)
	lastidx, _ := rf.GetLastLogInfo()
	rf.mu.Unlock()
	//offline_report := false

	for next_i > 0 {

		rep_i := new(RequestLogAppendReply)

		if !rf.SendRequestLogAppend(server, request_i, rep_i) {
			//if !offline_report {
			/*
				rf.mu.Lock()
				println("I am ", rf.me, " can't connect ", server)
				rf.printlog()
				rf.mu.Unlock()
				//*/
			//rf.mu.Lock()
			//rf.nextIndex[server] = next_i
			//rf.mu.Unlock()
			//offline_report = true
			//}
			return
		}
		//rpc ok
		//if offline_report {
		/*
			rf.mu.Lock()
			println("I am ", rf.me, " alive ", server)
			rf.printlog()
			rf.mu.Unlock()
			//*/
		//offline_report = false
		//}
		rf.mu.Lock()
		if rep_i.Success {
			//If successful: update nextIndex and matchIndex for follower
			//atomic.AddUint64(comfirm, 1)
			rf.nextIndex[server] = lastidx + 1
			rf.matchIndex[server] = lastidx
			//println("server", server, "replicate till", lastidx)
			rf.mu.Unlock()
			return
		}

		//对于返回false的rpc reply
		//先检查leader是否过期
		if rep_i.Term > rf.currentTerm {
			rf.TransformToFollower()
			rf.currentTerm = rep_i.Term
			rf.mu.Unlock()
			return
		}
		//rf.mu.Unlock()
		//If AppendEntries fails because of log inconsistency:
		//decrement nextIndex and retry
		//_, is_leader = rf.GetState()
		//time.Sleep(10 * time.Millisecond)
		next_i -= 1
		request_i = rf.GetLogAppendArgs(false, next_i)
		rf.mu.Unlock()

	}

}

func (rf *Raft) LogClientCheck() {

	for !rf.killed() {
		term, isleader := rf.GetState()

		if isleader {
			rf.LeaderCommitter(term)
		}

		go rf.applier()
		time.Sleep(20 * time.Millisecond)

	}
}

// If there exists an N such that
// N > commitIndex,
// a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm:
// set commitIndex = N (§5.3, §5.4)
func (rf *Raft) LeaderCommitter(term int) {

	_, isleader := rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !isleader {
		return
	}

	oldcommitidx := rf.commitIndex
	lastlogidx, _ := rf.GetLastLogInfo()
	servernum := len(rf.peers)
	quorum := int(servernum / 2)

	for N := oldcommitidx + 1; N <= lastlogidx; N++ {
		count := 0
		if rf.logs[N].Term != term {
			continue
		}
		for s := 0; s < servernum; s++ {
			if rf.matchIndex[s] >= N {
				count += 1
			}
		}
		if count > quorum {
			rf.commitIndex = N
		}
	}

	//println(rf.me, oldcommitidx, lastlogidx, rf.commitIndex)
}

/*
func (rf *Raft) printlog() {
	l := len(rf.logs)
	for i := 0; i < l; i++ {
		fmt.Printf("%v ", rf.logs[i])
	}
	println(rf.lastApplied, rf.commitIndex)

}*/
