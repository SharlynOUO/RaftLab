package raft

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) applier() {

	rf.mu.Lock()
	commit := rf.commitIndex
	lastapply := rf.lastApplied
	if commit > lastapply {
		rf.ApplyLogToStateMachine(commit, lastapply)
	}

	rf.mu.Unlock()
}

func (rf *Raft) ApplyLogToStateMachine(commit int, lastapply int) {
	//curterm, _ := rf.GetState()

	for i := lastapply + 1; i <= commit; i++ {

		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}

		rf.applyCh <- msg
		rf.lastApplied = i

		//fmt.Printf("%d apply %d: %v ,peers length: %d, identity: %d,logs: \n", rf.me, i, rf.logs[i], len(rf.peers), rf.identity)
		//rf.printlog()

	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}
