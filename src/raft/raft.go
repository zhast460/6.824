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
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "../labrpc"

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

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader

	ElectionInterval  = 200
	HeartbeatInterval = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	applyCh      chan ApplyMsg
	cond         *sync.Cond
	appliedUpTo  int
	applyLogMu   sync.Mutex
	applyLogCond *sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile
	commitIndex int
	lastApplied int

	// volatile leader
	nextIndex  []int
	matchIndex []int

	state       ServerState
	lastReceive time.Time
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.mu.Unlock()
	return term, isLeader
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// learned some observations from Raft visualization in its official site (it is so helpful!!!):
	// 1st: if args term is higher, but log is not as updated as others, others won't vote, they update terms only
	// 2nd: even though s2 has voted itself in term 11, s1 in term 12 with newer log ask s2 to vote, and s2 gives yes
	// 3rd: grant yes will reset timeout, grant no won't
	rf.mu.Lock()
	defer rf.mu.Unlock()
	votedFor := rf.votedFor

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	lastLogEntry := rf.getLastLogEntry()
	if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
		if args.LastLogTerm > lastLogEntry.Term || args.LastLogTerm == lastLogEntry.Term && args.LastLogIndex >= lastLogEntry.Index {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogEntry.Term || args.LastLogTerm == lastLogEntry.Term && args.LastLogIndex >= lastLogEntry.Index) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastReceive = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	DPrintf("%d voted %v for candidate %d. his prev votedFor: %d, his current term: %d, requester's term: %d", rf.me, reply.VoteGranted, args.CandidateId, votedFor, rf.currentTerm, args.Term)
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
	// DPrintf("%d is asking %d to vote for term %d", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("%d received term %d append entries from %d", rf.me, args.Term, args.LeaderId)
	if args.Term < rf.currentTerm {
		DPrintf("%d returned false to %d, term too low. its term: %d, incoming term: %d", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.ConvertToFollower(args.Term)
	} else {
		rf.lastReceive = time.Now()
	}

	if args.PreLogIndex >= len(rf.log) || rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		DPrintf("%d returned false to %d, pre log mismatch. prelogindex: %d", rf.me, args.LeaderId, args.PreLogIndex)
		if args.PreLogIndex < len(rf.log) {
			DPrintf("%d returned false to %d, pre log mismatch. prelogterm: %d, incoming request prelogterm: %d", rf.me, args.LeaderId, rf.log[args.PreLogIndex].Term, args.PreLogTerm)
		}
		reply.Success = false
		return
	} else {
		if len(args.Entries) > 0 {
			DPrintf("%d before append log, log len: %d, PreLogIndex: %d, entries len: %d", rf.me, len(rf.log), args.PreLogIndex, len(args.Entries))
			rf.log = append(rf.log[0:args.PreLogIndex+1], args.Entries...)

			idx := args.PreLogIndex
			for i := 0; i < len(args.Entries); i++ {
				if idx+i+1 >= len(rf.log) {
					rf.log = append(rf.log, args.Entries[i:]...)
					break
				} else if rf.log[idx+i+1].Term != args.Entries[i].Term {
					rf.log = append(rf.log[0:idx+i+1], args.Entries[i:]...)
					break
				}
			}
			DPrintf("%d after append log, log len: %d, PreLogIndex: %d, entries len: %d", rf.me, len(rf.log), args.PreLogIndex, len(args.Entries))
		}
		if args.LeaderCommit > rf.commitIndex {
			from := rf.commitIndex + 1
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
			to := rf.commitIndex
			go rf.applyLogs(from, to)
		}
		DPrintf("%d returned true to %d", rf.me, args.LeaderId)
		reply.Success = true
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server]
	preLog := rf.log[nextIndex-1]
	entries := rf.log[nextIndex:]
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  preLog.Index,
		PreLogTerm:   preLog.Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}

	DPrintf("%d sending term %d append entries to %d, whose nextIndex: %d, entry size: %d, commands: %v", rf.me, rf.currentTerm, server, rf.nextIndex[server], len(entries), entries)
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	// NOTE! at this point, raft instance's state can be different from the state before RPC call
	if reply.Term > rf.currentTerm {
		DPrintf("%d see newer term, and convert from leader to follower now. new term: %d", rf.me, reply.Term)
		rf.ConvertToFollower(reply.Term)
	} else if reply.Success {
		if len(entries) > 0 {
			DPrintf("leader %d got success from %d", rf.me, server)
			rf.matchIndex[server] = preLog.Index + len(entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			DPrintf("leader %d's current matchIndex: %v", rf.me, rf.matchIndex)
			rf.advanceCommitIndex()
		}
	} else {
		if rf.nextIndex[server] == nextIndex {
			rf.nextIndex[server]--
		}
		DPrintf("leader %d got NOT success from %d, its nextIndex-1 = %d", rf.me, server, rf.nextIndex[server])
	}
	rf.mu.Unlock()
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
	rf.mu.Lock()
	DPrintf("starting command %d", command)
	index = len(rf.log)
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	rf.matchIndex[rf.me]++

	rf.log = append(rf.log, LogEntry{
		Index:   len(rf.log),
		Term:    rf.currentTerm,
		Command: command,
	})

	num := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < num; i++ {
		go func(p int) {
			if p != rf.me {
				rf.sendAppendEntries(p)
			}
		}(i)
	}

	//rf.mu.Lock()
	//for rf.commitIndex < index {
	//	rf.cond.Wait()
	//}
	//rf.mu.Unlock()

	isLeader = true
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
	rf.mu.Lock()
	rf.dead = 1
	rf.mu.Unlock()
	//atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	rf.mu.Lock()
	z := rf.dead
	rf.mu.Unlock()
	//z := atomic.LoadInt32(&rf.dead)
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
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)
	rf.applyLogCond = sync.NewCond(&rf.applyLogMu)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{0, 0, nil})
	rf.lastReceive = time.Now()
	go rf.LeaderElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) LeaderElection() {
	for {
		electionTimeout := ElectionInterval + rand.Intn(200)
		startTime := time.Now()
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		rf.mu.Lock()
		if rf.dead == 1 {
			rf.mu.Unlock()
			return
		}
		if rf.lastReceive.Before(startTime) && rf.state != Leader {
			//DPrintf("%d's lastReceive is %v, startTime is %v", rf.me, rf.lastReceive, startTime)
			go rf.KickoffElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) KickoffElection() {
	rf.mu.Lock()
	rf.ConvertToCandidate()

	DPrintf("%d is kicking off election for term %d", rf.me, rf.currentTerm)
	lastLogEntry := rf.getLastLogEntry()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	numVote := 1 // voted for self
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(p int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(p, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.ConvertToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					numVote++
					if rf.state == Candidate && numVote > len(rf.peers)/2 {
						DPrintf("%d BECOME LEADER for term %d", rf.me, rf.currentTerm)
						rf.ConvertToLeader()
						for j := 0; j < len(rf.peers); j++ {
							if j != rf.me {
								go rf.sendHeartbeat(j)
							}
						}
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) ConvertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastReceive = time.Now()
	//DPrintf("%d's last receive is updated to %v", rf.me, rf.lastReceive)
}

func (rf *Raft) ConvertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastReceive = time.Now()
}

func (rf *Raft) ConvertToLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogEntry().Index + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.lastReceive = time.Now()
}

func (rf *Raft) sendHeartbeat(p int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go rf.sendAppendEntries(p)
		time.Sleep(HeartbeatInterval * time.Millisecond)
	}
}

func (rf *Raft) advanceCommitIndex() {
	// TODO: can do better than sort every time?
	c := make([]int, len(rf.matchIndex))
	copy(c, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(c)))
	idx := len(rf.peers) / 2
	from := rf.commitIndex + 1
	N := c[idx]
	if rf.log[N].Term != rf.currentTerm {
		return
	}

	rf.commitIndex = N
	//rf.cond.Broadcast()
	to := rf.commitIndex
	if to >= from {
		go rf.applyLogs(from, to)
	}
}

func (rf *Raft) applyLogs(from, to int) {
	DPrintf("%d is applying log from index %d to %d, appliedUpTo: %d", rf.me, from, to, rf.appliedUpTo)
	rf.applyLogMu.Lock()
	defer rf.applyLogMu.Unlock()

	for rf.appliedUpTo < from-1 {
		DPrintf("%d's appliedUpTo %d is less than (from) %d - 1, should wait now", rf.me, rf.appliedUpTo, from)
		rf.applyLogCond.Wait()
	}
	DPrintf("%d's appliedUpTo is %d now", rf.me, rf.appliedUpTo)

	// other thread could have applied logs and modified rf.appliedUpTo
	if rf.appliedUpTo >= to {
		return
	} else if rf.appliedUpTo >= from {
		from = rf.appliedUpTo + 1
	}

	// seems sometimes follower's committed logs can be truncated, not figured out why yet
	to = min(to, len(rf.log)-1)

	for i := from; i <= to; i++ {
		log := rf.log[i]
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: log.Index,
		}
	}
	rf.appliedUpTo = to
	rf.applyLogCond.Broadcast()
	DPrintf("%d completed applying log from index %d to %d, appliedUpTo is updated to %d", rf.me, from, to, rf.appliedUpTo)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
