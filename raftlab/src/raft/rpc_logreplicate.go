package raft

import (
	"time"
)

/*
 * rpc for appending log entries
 */

// Request of LogAppend RPC arguments structure.
type RequestLogAppendArgs struct {
	Term         int         //leader’s term
	LeaderId     int         //so follower can redirect clients
	PrevLogIndex int         //index of log entry immediately preceding new ones
	PrevLogTerm  int         //term of prevLogIndex entry
	Entries      []*LogEntry //log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int         //leader’s commitIndex

}

// Reply of LogAppend RPC arguments structure.
type RequestLogAppendReply struct {
	Term    int
	Success bool
}

// leader acts
func (rf *Raft) GetLogAppendArgs(is_heartbeat bool, next int) *RequestLogAppendArgs {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	entry := make([]*LogEntry, 0)
	prevterm := 0
	lastlogidx, _ := rf.GetLastLogInfo()
	previdx := next - 1

	if !is_heartbeat {

		//if not heartbeat, replicate logs
		for i := next; i <= lastlogidx; i++ {
			entry = append(entry, rf.logs[i])
		}
		prevterm = rf.logs[previdx].Term
	}

	return &RequestLogAppendArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: previdx,
		PrevLogTerm:  prevterm,
		Entries:      entry,
		LeaderCommit: rf.commitIndex,
	}
}

// RequestLogAppend RPC handler.
func (rf *Raft) RequestLogAppend(args *RequestLogAppendArgs, reply *RequestLogAppendReply) {

	termin, is_leader := rf.GetState()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//case below : term >= currentTerm
	if args.LeaderId != rf.me {
		//leader发现了其它leader
		rf.TransformToFollower()
		rf.currentTerm = args.Term
		if termin == args.Term && is_leader {
			return
		}
	}

	if len(args.Entries) == 0 {
		//is heartbeat
		rf.heartBeatStamp = time.Now()

	}

	//println("i am", rf.me)
	//fmt.Printf("%v\n", reply)

	//replicate

	//2. Reply false if log doesn’t contain an entry
	//at prevLogIndex whose term matches prevLogTerm
	lastidx, _ := rf.GetLastLogInfo()

	if lastidx < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//3. If an existing entry conflicts with a new one
	//(same index but different terms),delete the
	//existing entry and all that follow it
	/*
		//这个方法在并发Start的时候可能会因为rpc的乱序把同一个term内新的log覆盖成旧的log
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	*/

	next := args.PrevLogIndex + 1
	entry := 0
	leng_entry := len(args.Entries)
	for ; entry < leng_entry; entry++ {
		mylogidx := next + entry
		if mylogidx > lastidx {
			break
		}
		if rf.logs[mylogidx].Term != args.Entries[entry].Term {
			rf.logs = rf.logs[:mylogidx]
			break
		}

	}

	//4. Append any new entries not already in the log
	if entry < leng_entry {
		append_part := args.Entries[entry:]
		rf.logs = append(rf.logs, append_part...)
	}

	//copy(rf.logs, templog)

	//5. If leaderCommit > commitIndex, set
	//commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastlogi, _ := rf.GetLastLogInfo()
		if lastlogi >= args.LeaderCommit && rf.logs[args.LeaderCommit].Term == rf.currentTerm {
			rf.commitIndex = min(args.LeaderCommit, lastlogi)
		}

	}

	reply.Term = rf.currentTerm
	reply.Success = true

}

// Reply Sending code of LogAppend
func (rf *Raft) SendRequestLogAppend(server int, args *RequestLogAppendArgs, reply *RequestLogAppendReply) bool {

	ok := rf.peers[server].Call("Raft.RequestLogAppend", args, reply)
	return ok
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
