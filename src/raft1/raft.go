package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const TIMTOUTDURATION = 1500 // 选举超时基础时间，单位毫秒
const SERVER_TIMEOUT = 500
const isRandom = false
const enableSleep = true

type ServerState int

const (
	FOLLOWER  ServerState = 0
	CANDIDATE ServerState = 1
	LEADER    ServerState = 2
)

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

	State       ServerState // 当前服务器的角色状态，0是follower、1是candidate、2是leader
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
	rf.mu.Lock()
	term = rf.CurrentTerm
	state := rf.State
	rf.mu.Unlock()
	isleader = state == LEADER
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

		// 将自己转为Follower
		rf.CurrentTerm = args.Term    // 更新自己的任期
		rf.State = FOLLOWER           // 将自己转换为follower
		rf.VoteFor = args.CandidateID // 将投票记录修改

		// 自己已经投票，重置自己的timeOut计时器
		// 使用非阻塞的重置信号发送方式，保证最近有一次倒计时重置即可
		select {
		case rf.TimeOutChan <- 1:
		default:
		}

		Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, args.CandidateID, rf.CurrentTerm)
	} else if args.Term == rf.CurrentTerm && (rf.VoteFor == -1 || rf.VoteFor == args.CandidateID) {
		// 如果两者的任期term是一样的，比较该server是否已经投过票，如果投过是否是该拉票请求的发起者
		// 如果没投过，或者之前给该请求发起者透过票，再次进行比较
		myLastLogIndex := len(rf.Log) - 1
		if args.LastLogTerm > rf.Log[myLastLogIndex].Term {
			// 如果最后日志term大于自己的最后日志任期term，允许成为leader
			reply.VoteGranted = true

			rf.State = FOLLOWER
			rf.VoteFor = args.CandidateID

			// 自己已经投票，重置自己的timeOut计时器
			// 使用非阻塞的重置信号发送方式，保证最近有一次倒计时重置即可
			select {
			case rf.TimeOutChan <- 1:
			default:
			}
			Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, args.CandidateID, rf.CurrentTerm)
		} else if args.LastLogTerm == rf.Log[myLastLogIndex].Term && args.LastLogIndex >= myLastLogIndex {
			// 如果最后日志任务term等于自己最后日志任期term，并且最后日志索引大于自己的最后日志索引，允许成为leader
			reply.VoteGranted = true

			rf.State = FOLLOWER
			rf.VoteFor = args.CandidateID

			// 自己已经投票，重置自己的timeOut计时器，防止自己刚投完票后就成为大一个Term的Candidate，最终出现一个小一个Term的leader和自己这个大一轮的Candidate
			// 使用非阻塞的重置信号发送方式，保证最近有一次倒计时重置即可
			select {
			case rf.TimeOutChan <- 1:
			default:
			}
			Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, args.CandidateID, rf.CurrentTerm)
		} else {
			// 否则不允许成为leader
			reply.VoteGranted = false
			Debug(dVote, "S%d Refuse Vote to S%d at T%d", rf.me, args.CandidateID, rf.CurrentTerm)
		}
		reply.Term = rf.CurrentTerm
	} else {
		// 或者如果任期小于自己的任期，拒绝拉票请求
		// 如果投过票，且不是该请求的发起者，那么拒绝该请求的拉票请求
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d Refuse Vote to S%d at T%d", rf.me, args.CandidateID, rf.CurrentTerm)
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

	// 如果log参数为nil，说明这是leader的心跳信息，
	if args.Entries == nil {
		Debug(dTrace, "S%d At T%d Recive Heart Beat From Leader S%d In T%d", rf.me, rf.CurrentTerm, args.LeaderID, args.Term)

		// 如果leader的Term大于自己的Term，更新自己的Term，并将身份转换为follower，重置投票项
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VoteFor = -1
			rf.State = 0
		} else if args.Term == rf.CurrentTerm && rf.State == 1 {
			// 如果leader的Term和自己的一样，说明自己和leader是同时期的candidate，确认自己的身份是candidate，服从先自己一步成为leader的server
			rf.State = 0
			Debug(dTrace, "S%d At T%d Recive Heart Beat From Leader S%d In T%d, Convert From Candidate to Follower", rf.me, rf.CurrentTerm, args.LeaderID, args.Term)
		}
		reply.Success = true
		reply.Term = rf.CurrentTerm
		// 给自己的timeChan管道发送一个信息，重置倒计时,不论自己是follower还是candidate，都需要重置倒计时
		select {
		case rf.TimeOutChan <- 1:
		default:
		}
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

func (rf *Raft) FollowerCase(me int) {

	// 如果为follower状态,等待接收leader的心跳
	// 设定本轮的超时时间，设定为1.5秒的1-2倍随机，因为要求在5秒内选出leader
	// curDuration := time.Duration(TIMTOUTDURATION * (float32(rf.me) + 1) / 2 * float32(time.Millisecond))

	var curDuration time.Duration
	if isRandom {
		curDuration = time.Duration(TIMTOUTDURATION * (rand.Float32() + 1) * float32(time.Millisecond))
	} else {
		curDuration = time.Duration(SERVER_TIMEOUT * float32(me+1) * float32(time.Millisecond))
	}

	select {
	case <-rf.TimeOutChan:
		// 收到leader的心跳，重置倒计时，即进入下一轮倒计时
		return
	case <-time.After(curDuration):
		// 如果超时没有收到leader的心跳，将自己转换身份为candidate，并将自己的term+1
		// 由于Term+1，自己变成candidate，将投票投给自己，如果重置，可能会在发送拉票选举之前，投票给其他server
		rf.mu.Lock()
		rf.CurrentTerm += 1
		rf.VoteFor = rf.me
		rf.State = CANDIDATE
		Debug(dTimer, "S%d TimeOut Convert State From Follower to Candidate At T%d", rf.me, rf.CurrentTerm)
		rf.mu.Unlock()
	}
}

func (rf *Raft) CandidateSendVoteRequestParallel(guaranteedNum, effectiveNum, serverNum int) {
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	curTerm := rf.CurrentTerm
	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[lastLogIndex].Term
	rf.mu.Unlock()

	// 创建上下文，用于在身份已经跳转后，同志其他还在运行的拉票请求停下来
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保函数退出前，总是调用cancel释放资源

	// 创建缓冲通道，用于接收并行发送的拉票请求的reply，容量须够大，能够处理当身份已经跳转后，未执行完成的goroutine也能非阻塞写入结果
	requestVoteReplyChan := make(chan RequestVoteReply, len(peers)-1)

	// 并行对每个server发送拉票请求
	for i := 0; i < serverNum; i++ {
		if i == me { // 不给自己发送拉票请求
			continue
		}

		go func(peer int, ctx context.Context) {
			args := RequestVoteArgs{
				Term:         curTerm,
				CandidateID:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}

			// 发送拉票请求，如果请求有回复，或者说请求成功，就将结果发送到reply处理管道
			// 同时监听是否外部拉票程序已经有结果了，如果有结果，就直接退出该程序
			select {
			case <-ctx.Done():
				return
			default:
				if ok := rf.sendRequestVote(i, &args, &reply); ok {
					requestVoteReplyChan <- reply
				}
			}
		}(i, ctx)
	}

	// 拉票请求结果处理部分
	for i := 0; i < serverNum-1; i++ {
		reply := <-requestVoteReplyChan

		if reply.VoteGranted {
			guaranteedNum++
			if guaranteedNum >= effectiveNum {
				// 同时，该线程不再等待未完成的线程
				cancel()
				// 拉票环节结束，同意票数大于过半人数，将自己转换为leader身份，并初始化leader相关结构，开始发送心跳进入下一周期
				rf.mu.Lock()
				rf.State = LEADER
				rf.NextIndex = make([]int, len(rf.peers))
				rf.MatchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.NextIndex[i] = len(rf.Log)
					rf.MatchIndex[i] = 0
				}
				Debug(dTrace, "S%d Candidate SendRequestVote Done At T%d, Success Come Leader", rf.me, rf.CurrentTerm)
				rf.mu.Unlock()

				select {
				case rf.TimeOutChan <- 1:
				default:
				}
				return
			} else if reply.Term > curTerm {
				// 也是拉票请求有了结果，因此通知未完成的拉票程序停止
				cancel()
				// 由于收到了更高的Term，将自己的身份转换为follower，并且更新term，重置倒计时进入下一ticker循环
				rf.mu.Lock()
				rf.State = FOLLOWER
				rf.CurrentTerm = reply.Term
				rf.VoteFor = -1
				Debug(dTrace, "S%d Candidate SendRequestVote Receive Higher Term At T%d, Convert to Follower", me, rf.CurrentTerm)
				rf.mu.Unlock()

				select {
				case rf.TimeOutChan <- 1:
				default:
				}
				return
			}
		}
	}
}

func (rf *Raft) CandidateCase(me int) {
	// 如果当前server身份转变为candidate，则循环向每个Server发送拉票请求，每次成为candidate只用发送一轮拉票请求
	rf.mu.Lock()
	serverNum := len(rf.peers)
	rf.mu.Unlock()

	guaranteedNum := 1 // 初始化为1，是因为自己给自己投一票
	effectiveNum := 0  // 计算过半门槛
	// 设定过半有效门槛票数
	if serverNum%2 == 0 {
		effectiveNum = serverNum / 2
	} else {
		effectiveNum = serverNum/2 + 1
	}

	// 将发送拉票请求包装为一个函数
	rf.CandidateSendVoteRequestParallel(guaranteedNum, effectiveNum, serverNum)

	var curDuration time.Duration
	if isRandom {
		curDuration = time.Duration(TIMTOUTDURATION * (rand.Float32() + 1) * float32(time.Millisecond))
	} else {
		curDuration = time.Duration(SERVER_TIMEOUT * float32(me+1) * float32(time.Millisecond))
	}

	select {
	case <-rf.TimeOutChan:
		return
	case <-time.After(curDuration):
		// 如果拉票环节超时，将自己的term+1，进入下一轮的拉票环节
		rf.mu.Lock()
		rf.CurrentTerm += 1
		Debug(dTrace, "S%d Candidate TimeOut Increate Term From T%d to T%d", rf.me, rf.CurrentTerm-1, rf.CurrentTerm)
		rf.mu.Unlock()
	}
}

func (rf *Raft) LeaderCase() {
	// 如果为leader状态，需要向每个server发送心跳，心跳使用AppendEntries RPC代替
	rf.mu.Lock()
	me := rf.me
	serverNum := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < serverNum; i++ {
		if i == me { // 不向自己发送心跳
			continue
		}

		rf.mu.Lock()
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     me,
			PrevLogIndex: len(rf.Log) - 1,            // 作为leader，默认认为leader的最后一个日志索引就是其他follower相匹配的最后日志索引，不一样再前推
			PrevLogTerm:  rf.Log[len(rf.Log)-1].Term, // 与prevLogIndex一样
			Entries:      nil,
			LeaderCommit: rf.CommitIndex,
		}
		reply := AppendEntriesReply{}
		Debug(dLeader, "S%d Sending Heart Beat to S%d At T%d", me, i, rf.CurrentTerm)
		rf.mu.Unlock()
		go func() {
			rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
		}()
	}
	// 由于实验要求leader每秒钟发送心跳不能超过10次，即睡眠随机睡眠时长至少为100ms，而下面的随机睡眠时长范围是50-350ms，因此在这里睡眠50ms
	// time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		Debug(dTrace, "S%d is Status %d, At T%d Debug", rf.me, rf.State, rf.CurrentTerm)

		rf.mu.Lock()
		curState := rf.State
		me := rf.me
		rf.mu.Unlock()

		switch curState {
		case FOLLOWER:
			rf.FollowerCase(me)
		case CANDIDATE:
			rf.CandidateCase(me)
		case LEADER:
			rf.LeaderCase()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		if enableSleep {
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
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

	Debug(dInfo, "S%d Server initialized success, run ticker", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
