package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

func (rf *Raft) ticker() {
	//println(rf.me, "ticker!")

	HeartBeatTimeOut := time.Duration(400 * time.Millisecond)
	VoteTimeOut := time.Duration(30 * time.Millisecond)
	var votefor int

	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		votefor = rf.votedFor
		lasthearbeat := rf.heartBeatStamp
		rf.mu.Unlock()

		if votefor == -1 || time.Since(lasthearbeat) > HeartBeatTimeOut {

			rf.mu.Lock()
			//a follower increments its current term
			rf.currentTerm += 1
			//and transitions to candidate state.
			rf.TransformToCandidate()
			rf.mu.Unlock()

			rf.LaunchElection(VoteTimeOut)

		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

/* 3 cases:
 * (a) it wins the election - send hearbeat
 * (b) another server establishes itself as leader - goto follower state
 * (c) a period of time goes by with no winner - timeout and new election
 */
func (rf *Raft) LaunchElection(VoteTimeOut time.Duration) {
	rf.mu.Lock()
	serversNum := len(rf.peers)
	voteargs := rf.GetVoteArgs()
	//fmt.Println("launchele:", rf.me)
	rf.mu.Unlock()

	//var results sync.Map
	//tmp := make([](RequestVoteReply), serversNum)

	var voteforme uint64
	atomic.StoreUint64(&voteforme, 0)
	quorum := int(serversNum / 2)
	//connectState := make([]uint32, serversNum)
	//results := make([]RequestVoteReply, serversNum)
	//isret := make([]chan bool, serversNum)
	for i := 0; i < serversNum; i++ {
		//results[i] = new(RequestVoteReply)
		//results[i].VoteGranted = false
		//isret[i] = make(chan bool)
		//results[i].VoteGranted = false
		//atomic.StoreUint32(&connectState[i], connected)
		go rf.BallotBox(i, &voteforme, voteargs) //, &connectState[i])

	}
	time.Sleep(VoteTimeOut)
	ballots := int(atomic.LoadUint64(&voteforme))
	/*for i := 0; i < serversNum; i++ { //<-可以过但效果很差，大概率10s内选不出来
		<-isret[i]
		close(isret[i])
	}*/

	/*alive_server := 0
	for i := 0; i < serversNum; i++ {
		if atomic.LoadUint32(&connectState[i]) == connected {
			alive_server += 1
		}
	}*/
	/*
		//统计票数
		for i := 0; i < serversNum; i++ {

			//fmt.Printf("%v", results[i])
			if results[i].VoteGranted {
				voteforme++
			}
	}*/
	//quorum := uint64(alive_server / 2)
	//println(alive_server, quorum)

	if ballots > quorum {
		rf.mu.Lock()
		rf.LeaderServerInit()
		rf.TransformToLeader()
		//rf.connectServerNum = alive_server
		rf.mu.Unlock()

		go rf.StartLeaderHeartBeat(serversNum)
	}

}

func (rf *Raft) BallotBox(server int, counter *uint64, votearg *RequestVoteArgs) { //, Report *uint32) {
	result := RequestVoteReply{}
	rf.SendRequestVote(server, votearg, &result)
	if result.VoteGranted {
		atomic.AddUint64(counter, 1)
	}
	/*if rf.SendRequestVote(server, votearg, &result) {
		if result.VoteGranted {
			atomic.AddUint64(counter, 1)
		}
		atomic.StoreUint32(Report, connected)

		rf.mu.Lock()
		if result.Term > rf.currentTerm {
			rf.TransformToFollower()
		}
		rf.mu.Unlock()

	} else {
		atomic.StoreUint32(Report, uconnect)
	}*/

}

// 无需外部加锁
func (rf *Raft) StartLeaderHeartBeat(serverNUM int) {
	isleader := true
	BeatIntervalTime := time.Duration(100 * time.Millisecond) //10 times/s
	/*connectState := make([]uint32, serverNUM)
	for i := 0; i < serverNUM; i++ {
		atomic.StoreUint32(&connectState[i], connected)
	}*/

	for isleader {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		heart_args := rf.GetLogAppendArgs(true, 1)
		rf.mu.Unlock()
		heart_reps := make([]*RequestLogAppendReply, serverNUM)

		for i := 0; i < serverNUM; i++ {
			heart_reps[i] = new(RequestLogAppendReply)
			//go rf.SendHeartBeatToOneServer(i, heartargs, &connectState[i], term)
			//发送心跳不关心回复
			go rf.SendRequestLogAppend(i, heart_args, heart_reps[i])

		}

		time.Sleep(BeatIntervalTime)
		_, isleader = rf.GetState()

		/*//更新节点连接信息
		c := 0
		for i := 0; i < serverNUM; i++ {
			if atomic.LoadUint32(&connectState[i]) == connected {
				c += 1
			}
		}

			rf.mu.Lock()
			rf.connectServerNum = c
			rf.mu.Unlock()*/

	}

}
