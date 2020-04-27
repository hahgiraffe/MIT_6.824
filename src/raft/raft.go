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

import "sync"
import (
	"6.824/src/labrpc"
	"math/rand"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	//节点状态
	FOLLOWER  = 1
	CANDIDATE = 2
	LEADER    = 3

	HEART_FREQUENCE = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A Leader Election && HeartBeats RPC
	state         int           //当前状态，初始都为FOLLOWER
	currentTerm   int           //当前时期，初始为0
	voteFor       int           //投票给哪个节点，初始为-1
	electionTimer *time.Timer   //选举定时器
	votech        chan struct{} //投票成功通道，重置定时器
	appendch      chan struct{} //收到心跳包通道，重置定时器
	voteAcquired  int           //收到的票数

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = false
	if rf.state == LEADER {
		isleader = true
	}
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int //当前term
	CandidateId int //当前index
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int  //当前term
	VoteOrNot   bool //是否投票给该节点
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term == rf.currentTerm {
		if rf.voteFor == -1 {
			reply.VoteOrNot = true
			rf.voteFor = args.CandidateId
		} else {
			reply.VoteOrNot = false
		}
	} else if args.Term < rf.currentTerm {
		reply.VoteOrNot = false
		reply.CurrentTerm = rf.currentTerm
	} else {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.voteFor = -1

		if rf.voteFor == -1 {
			reply.VoteOrNot = true
			rf.voteFor = args.CandidateId
		} else {
			reply.VoteOrNot = false
		}
	}

	reply.CurrentTerm = rf.currentTerm
	rf.mu.Unlock()

	if reply.VoteOrNot == true {
		go func() {
			rf.votech <- struct{}{}
		}()
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
	//PrevLogIndex int
	//PrevLogTerm  int
}

type AppendEntryReply struct {
	Term        int
	AppendOrNot bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()

	var flag bool = true
	if args.Term < rf.currentTerm {
		reply.AppendOrNot = false
		reply.Term = rf.currentTerm
		flag = false
	} else if args.Term == rf.currentTerm {
		reply.AppendOrNot = true
		reply.Term = rf.currentTerm
	} else {
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = FOLLOWER

		reply.AppendOrNot = false
	}

	rf.mu.Unlock()
	if flag {
		go func() {
			rf.appendch <- struct{}{}
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.electionTimer = time.NewTimer(randEelectionInterval())
	rf.voteFor = -1
	rf.appendch = make(chan struct{})
	rf.votech = make(chan struct{})
	rf.voteAcquired = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startWork()

	return rf
}

func randEelectionInterval() time.Duration {
	//return time.Duration(time.Second)
	var Lower int32 = 150
	var Upper int32 = 300
	t := rand.New(rand.NewSource(time.Now().UnixNano()))
	interval := t.Int31n(int32(Upper-Lower)) + Lower
	return time.Duration(interval) * time.Millisecond
}

func (rf *Raft) startWork() {
	//根据自己节点的不同状态，做相应不同的动作
	for {
		state := rf.state
		switch state {
		case FOLLOWER:
			//FOLLOWER只用监听包，并且重置定时器
			select {
			case <-rf.electionTimer.C:
				//定时器超时，更改状态诶CANDIDATE，并重置定时器
				DPrintf("electionTimer called server: %v term: %v state: %v \n", rf.me, rf.currentTerm, rf.state)
				rf.mu.Lock()
				//rf.updateState(CANDIDATE)
				rf.state = CANDIDATE
				rf.mu.Unlock()
				rf.leaderElection()
			case <-rf.votech:
				//表示该节点投票成功，重置定时器
				DPrintf("votech called server: %v term: %v state: %v \n", rf.me, rf.currentTerm, rf.state)
				rf.electionTimer.Reset(randEelectionInterval())
			case <-rf.appendch:
				//表示日志已收到，重置定时器
				DPrintf("appendch called server: %v term: %v state: %v \n", rf.me, rf.currentTerm, rf.state)
				rf.electionTimer.Reset(randEelectionInterval())
			}
		case CANDIDATE:
			//调用RequestVote RPC,发起选举
			select {
			case <-rf.electionTimer.C:
				rf.leaderElection()
			case <-rf.appendch:
				//收到leader心跳表
				rf.mu.Lock()
				//rf.updateState(FOLLOWER)
				rf.state = FOLLOWER
				rf.mu.Unlock()
			default:
				rf.mu.Lock()
				if rf.voteAcquired > len(rf.peers)/2 {
					//获得票超过一半
					//rf.updateState(LEADER)
					rf.state = LEADER
				}
				rf.mu.Unlock()
			}
		case LEADER:
			//定期广播心跳包
			n := len(rf.peers)
			for i := 0; i < n; i++ {
				if i == rf.me {
					continue
				}
				if rf.state != LEADER {
					break
				}
				go rf.broadcastAppend(i)
			}

			if rf.state == LEADER {
				time.Sleep(time.Duration(HEART_FREQUENCE) * time.Millisecond)
			}
		}
	}
}

//CANDIDATE发起的选举过程
func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1

	DPrintf("term: %v server: %v state: %v", rf.currentTerm, rf.me, rf.state)
	rf.electionTimer.Reset(randEelectionInterval())

	//投自己
	rf.voteAcquired = 1
	rf.voteFor = rf.me

	n := len(rf.peers)

	//调用RequestVote发给其他所有节点
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	for i := 0; i < n; i++ {
		if rf.state != CANDIDATE {
			break
		}
		if i == rf.me {
			continue
		}
		go func(nodenum int) {
			var reply RequestVoteReply
			if rf.state == CANDIDATE && rf.sendRequestVote(nodenum, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.VoteOrNot {
					rf.voteAcquired += 1
				} else {
					if reply.CurrentTerm > rf.currentTerm {
						rf.state = FOLLOWER
						rf.voteFor = -1
						rf.voteAcquired = 0
						rf.currentTerm = reply.CurrentTerm

					}
				}
			}
		}(i)
	}

	rf.mu.Unlock()
}

//LEADER 定期广播心跳
func (rf *Raft) broadcastAppend(node int) {
	if rf.killed() {
		return
	}
	if rf.state != LEADER {
		return
	}

	rf.mu.Lock()
	var reply AppendEntryReply

	args := AppendEntryArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	rf.mu.Unlock()

	if rf.sendAppendEntries(node, &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.voteFor = -1
		}
		rf.mu.Unlock()
	}

	time.Sleep(10 * time.Millisecond)

}
