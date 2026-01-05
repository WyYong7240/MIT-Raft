package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const TIMTOUTDURATION = 1500 // 选举超时基础时间

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 所有服务器上的持久性状态
	CurrentTerm int        // 服务器当前任期，首次启动初始化为0，单调递增
	VoteFor     int        // 投票的服务器ID，没有投票给任何候选人为空
	Log         []LogEntry // 该服务器存储的日志体，初始索引为1

	// 所有服务器上的易失性状态
	CommitIndex int // 已知已经提交的最高日志条目的索引，初始值为0，单调递增
	LastApplied int // 已经被应用到状态机 的最高日志条目索引，初始值为0，单调递增

	// leader上的易失性状态，选举后需要重新初始化
	NextIndex  []int // 对于每台服务器，发送到该服务器的下一个日志条目索引，初始值为领导人最后的日志条的索引+1
	MatchIndex []int // 对于每台服务器，已知的已经复制到该服务器的最高日志条目索引

	State       int32 // 当前服务器的角色状态，0是follower、1是candidate、2是leader
	TimeOutChan chan int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.CurrentTerm
	state := atomic.LoadInt32(&rf.State)
	isleader = state == 2
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// 由候选人负责调用，来征集选票
	Term         int // 候选人任期号
	CandidateID  int // 候选人ID
	LastLogIndex int // 候选人最后日志条目的索引
	LastLogTerm  int // 候选人最后日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得此选票时为真
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.CurrentTerm {
		// 如果拉票请求的任务term大于自己的term，直接同意，并将自己转为follower
		// 拉票请求回复
		reply.Term = args.Term
		reply.VoteGranted = true
		reply.Term = args.Term

		// 将自己转为Follower
		rf.CurrentTerm = args.Term // 更新自己的任期
		rf.State = 0               // 将自己转换为follower
		rf.VoteFor = -1            // 清空voteFor
	} else if args.Term == rf.CurrentTerm && (rf.VoteFor == -1 || rf.VoteFor == args.CandidateID) {
		// 如果两者的任期term是一样的，比较该server是否已经投过票，如果投过是否是该拉票请求的发起者
		// 如果没投过，或者之前给该请求发起者透过票，再次进行比较
		myLastLogIndex := len(rf.Log) - 1
		if args.LastLogTerm > rf.Log[myLastLogIndex].Term {
			// 如果最后日志term大于自己的最后日志任期term，允许成为leader
			reply.VoteGranted = true

			rf.CurrentTerm = args.Term
			rf.State = 0
			rf.VoteFor = args.CandidateID
		} else if args.LastLogTerm == rf.Log[myLastLogIndex].Term && args.LastLogIndex >= myLastLogIndex {
			// 如果最后日志任务term等于自己最后日志任期term，并且最后日志索引大于自己的最后日志索引，允许成为leader
			reply.VoteGranted = true

			rf.CurrentTerm = args.Term
			rf.State = 0
			rf.VoteFor = args.CandidateID
		} else {
			// 否则不允许成为leader
			reply.VoteGranted = false
		}
		reply.Term = rf.CurrentTerm
	} else {
		// 或者如果任期小于自己的任期，拒绝拉票请求
		// 如果投过票，且不是该请求的发起者，那么拒绝该请求的拉票请求
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term         int        // 领导人任期
	LeaderID     int        // 领导人ID，据此follower可以对客户端进行重定向
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引,其实就是follower和leader在追加新日志之前，相匹配的那条日志的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期,其实就是follower和leader在追加新日志之前，相匹配的那条日志的任期值
	Entries      []LogEntry // 需要被保存的日志条目，当做心跳时，该内容为空
	LeaderCommit int        // 领导人的已知的已提交的最高日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，对于领导人而言，其会更新自己的任期
	Success bool // 如果follower所含有的条目和prevLogIndex和prevLogTerm匹配上了，则为true
}

// AppendEntries 实现
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果log参数为nil，说明这是leader的心跳信息，给自己的timeChan管道发送一个信息，重置倒计时
	if args.Entries == nil {
		rf.TimeOutChan <- int(rf.State)
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// me := rf.me
		curState := atomic.LoadInt32(&rf.State)
		// log.Printf("%d Ticker, curState: %d\n", me, curState)
		if curState == 0 {
			// 如果为follower状态,等待接收leader的心跳
			// 设定本轮的超时时间，设定为1.5秒的1-2倍随机，因为要求在5秒内选出leader
			curDuration := time.Duration(TIMTOUTDURATION * (rand.Float32() + 1) * float32(time.Millisecond))
			select {
			case <-rf.TimeOutChan:
				// 收到leader的心跳，重置倒计时，即进入下一轮倒计时
				continue
			case <-time.After(curDuration):
				// 如果超时没有收到leader的心跳，将自己转换身份为candidate，并将自己的term+1
				rf.CurrentTerm += 1
				atomic.StoreInt32(&rf.State, 1)
			}

		} else if curState == 1 {
			// 如果当前server身份转变为candidate，则循环向每个Server发送拉票请求
			guaranteedNum := 1              // 初始化为1，是因为自己给自己投一票
			rf.VoteFor = rf.me              // 给自己投票，因此需要将VoteFor也设置
			effectiveNum := 0               // 计算过半门槛
			lastLogIndex := len(rf.Log) - 1 // 自己的最后日志索引

			// 设定过半有效门槛票数
			if len(rf.peers)%2 == 0 {
				effectiveNum = len(rf.peers) / 2
			} else {
				effectiveNum = len(rf.peers)/2 + 1
			}

			// 对每个server发送拉票请求
			for i := 0; i < len(rf.peers); i++ {
				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateID:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  rf.Log[lastLogIndex].Term,
				}
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(i, &args, &reply); ok && reply.VoteGranted {
					// 如果发送的拉票请求收到了回复，计算是否得票
					guaranteedNum++
				}
				if guaranteedNum >= effectiveNum {
					// 如果同意票数大于过半人数，将自己转换为leader身份，并初始化leader相关结构
					atomic.StoreInt32(&rf.State, 2)
					rf.NextIndex = make([]int, len(rf.peers))
					rf.MatchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.NextIndex[i] = lastLogIndex + 1
						rf.MatchIndex[i] = 0
					}
				}
			}
		} else {
			// 如果为leader状态，需要向每个server发送心跳，心跳使用AppendEntries RPC代替
			lastLogIndex := len(rf.Log) - 1 // 作为leader自己的最后日志索引
			for i := 0; i < len(rf.peers); i++ {
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: lastLogIndex,              // 作为leader，默认认为leader的最后一个日志索引就是其他follower相匹配的最后日志索引，不一样再前推
					PrevLogTerm:  rf.Log[lastLogIndex].Term, // 与prevLogIndex一样
					Entries:      nil,
					LeaderCommit: rf.CommitIndex,
				}
				reply := AppendEntriesReply{}
				rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
			}
			// 由于实验要求leader每秒钟发送心跳不能超过10次，即睡眠随机睡眠时长至少为100ms，而下面的随机睡眠时长范围是50-350ms，因此在这里睡眠50ms
			time.Sleep(50 * time.Millisecond)
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		// 如此计算，leader每次发送心跳的时间间隔大概为100ms-400ms，而本设计的follower超时选举时间在1.5-3s，应该不会出问题
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.Log[0] = LogEntry{0, nil}
	// rf.Log = append(rf.Log, LogEntry{0, nil}) // 该条用于占位，有效日志索引从1开始
	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.State = 0                       // 服务器状态初始化为follower
	rf.TimeOutChan = make(chan int, 1) // 初始化一个通道，防止发送方阻塞

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	log.Printf("%d initialized success, run ticker", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
