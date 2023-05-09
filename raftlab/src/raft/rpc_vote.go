package raft

import (
	"time"
)

/*
 * rpc for vote
 */

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote

}

func (rf *Raft) GetVoteArgs() *RequestVoteArgs {
	lastlog_idx, lastlog_term := rf.GetLastLogInfo()

	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastlog_idx,
		LastLogTerm:  lastlog_term,
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//here is the Receiver implementation:

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// term == currentTerm
	if args.Term == rf.currentTerm {
		//vote信息的term和自己的term相同时
		//1）若是自己的信息，投一票
		if args.CandidateId == rf.me {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		} else {
			//2）若不是自己的信息,说明有同时发起的vote
			//返回false等待ticker再次超时
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	}

	//case: args.Term > curterm
	rf.currentTerm = args.Term
	rf.TransformToFollower()

	//2. vote restriction:If votedFor is null or candidateId
	//and candidate’s log is at least as up-to-date
	//as receiver’s log, grant vote(§5.2, §5.4)

	if rf.votedFor != -1 { //vote for is candidateId
		lastlogidx, lastlogterm := rf.GetLastLogInfo()

		if lastlogterm > args.LastLogTerm {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
		if lastlogterm == args.LastLogTerm && lastlogidx > args.LastLogIndex {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	}

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	rf.votedFor = args.CandidateId
	rf.heartBeatStamp = time.Now()

	//fmt.Printf("%v", reply)
	//fmt.Printf("%v", args)
	//println("i am", rf.me, "arg is sent by", args.CandidateId, "its term is", args.Term, "myterm is", rf.currentTerm, "my votefor ", rf.votedFor, "my identity is ", rf.identity)

}

// code to send a RequestVote RPC to a server.
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
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//ch <- ok
	return ok
}
