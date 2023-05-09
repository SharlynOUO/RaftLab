package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	applyCh chan ApplyMsg

	// state a Raft server must maintain.
	currentTerm int
	votedFor    int //candidateId that received vote in current term (or null if none)

	logs []*LogEntry

	//for all servers
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//records of management (for leader)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//extra:

	identity       int       //0:candidate;1:follower;2:leader
	heartBeatStamp time.Time //handle timeout issue
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh

	rf.dead = 0

	rf.currentTerm = 0
	rf.votedFor = -1       //-1 stand for null
	rf.identity = Follower //When servers start up, they begin as followers

	rf.logs = make([]*LogEntry, 0)
	rf.logs = append(rf.logs, GetNewLog(nil, 0)) //log index range:[1,n]

	rf.commitIndex = 0 //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	rf.lastApplied = 0 //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	rf.nextIndex = make([]int, 0)  //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	rf.matchIndex = make([]int, 0) //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.heartBeatStamp = time.Now()
	go rf.ticker()
	go rf.LogClientCheck()

	return rf

}
