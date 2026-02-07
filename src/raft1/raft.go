package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"context"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// 由于设定最久200ms一个心跳，因此，Follower超时时间设定为250ms~350ms
const TIMTOUTDURATION_INTERVAL = 100 // 选举超时基础时间，单位毫秒
const BASE_TIMEOUT_DURATION = 300

// 每个服务器基础的TIMEOUT时间，适用于<4个服务器情况
// S0:1500ms, S1:2000ms, S2:2500ms
const SERVER_TIMEOUT = 500
const SERVER_BASE_TIMEOUT = 1000
const isRandom = true
const LeaderElectionDebug = true
const LogAppendDebug = true
const EnableDebug = true

var ME int

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

	State           ServerState // 当前服务器的角色状态，0是follower、1是candidate、2是leader
	TimeOutChan     chan int
	NewLogChan      []chan int            // 当Leader收到一个新日志，并添加到自己的Log中时，需要触发此channel，跳过Leader心跳发送完毕的睡眠时间，直接进行下一轮的新日志发送,每个Follower对应一个发送协程，也即一个通道
	ApplyChan       chan raftapi.ApplyMsg // 用于将已经被多数server复制的logs提交到状态机的管道，不通过该管道发送已提交日志，无法通过3B测
	ApplierSyncCond sync.Cond             // 用于每个Server当发现自己的CommitIndex被更新后，通知Applier协程，将已经被提交的log，应用到服务器
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
	rf.mu.Lock()
	// Debug 所用信息
	me := rf.me

	// 函数逻辑所用信息
	curTerm := rf.CurrentTerm
	myLastLogIndex := len(rf.Log) - 1
	myLastLogTerm := rf.Log[myLastLogIndex].Term
	voteFor := rf.VoteFor

	rf.mu.Unlock()

	// 首先排除对方Term小于自己Term的情况
	if args.Term < curTerm {
		reply.Term = curTerm
		reply.VoteGranted = false
	}

	// 其次，再检查对方日志是否比自己的日志更新
	var isHeLogNewer bool = false
	if args.LastLogTerm > myLastLogTerm {
		isHeLogNewer = true
	} else if myLastLogTerm == args.LastLogTerm && args.LastLogIndex >= myLastLogIndex {
		isHeLogNewer = true
	}

	// 最后，处理对方Term比自己大、相等的情况
	if args.Term > curTerm {

		if isHeLogNewer {
			// 如果对方日志更新，我投票，并更新自己的Term，转为Follower
			reply.Term = args.Term
			reply.VoteGranted = true

			rf.mu.Lock()
			rf.VoteFor = args.CandidateID
			rf.CurrentTerm = args.Term
			rf.State = FOLLOWER
			rf.mu.Unlock()

			// 我投票，因此重置自己的计时器
			select {
			case rf.TimeOutChan <- 1:
			default:
			}

			if LeaderElectionDebug && EnableDebug {
				Debug(dVote, "S%d Granting Vote to S%d at T%d", me, args.CandidateID, args.Term)
			}
		} else {
			// 如果我的日志更新，我拒绝投票，但是我要更新我的Term，并转为Follower
			reply.Term = args.Term
			reply.VoteGranted = false

			rf.mu.Lock()
			rf.CurrentTerm = args.Term
			rf.State = FOLLOWER
			rf.mu.Unlock()

			if LeaderElectionDebug && EnableDebug {
				Debug(dVote, "S%d Refuse to Vote for S%d at T%d, but Refresh Term to T%d", me, args.CandidateID, args.Term, args.Term)
			}
		}
	} else if voteFor == -1 || voteFor == args.CandidateID {
		// 如果Term相同，并且没有投过票，或者给对方投过票

		if isHeLogNewer {
			// 如果对方日志更新，我投票，转为Follower
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.VoteFor = args.CandidateID
			rf.State = FOLLOWER
			rf.mu.Unlock()

			// 我投票，因此重置自己的计时器
			select {
			case rf.TimeOutChan <- 1:
			default:
			}

			if LeaderElectionDebug && EnableDebug {
				Debug(dVote, "S%d Granting Vote to S%d at T%d", me, args.CandidateID, args.Term)
			}
		} else {
			// 如果我的日志更新，我拒绝投票
			reply.Term = args.Term
			reply.VoteGranted = false

			if LeaderElectionDebug && EnableDebug {
				Debug(dVote, "S%d Refuse to Vote for S%d at T%d", me, args.CandidateID, args.Term)
			}
		}
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

	ConfilictTerm  int // 用于实现快速回退机制的冲突任期标识
	ConfilictIndex int // 用于实现快速回退机制的当前冲突任期在Follower的第一个Index
}

// AppendEntries 实现
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// Debug信息
	curTerm := rf.CurrentTerm
	me := rf.me

	// 代码逻辑所用信息
	state := rf.State
	rf.mu.Unlock()

	if EnableDebug {
		Debug(dTrace, "S%d At T%d Recive AppendEntries RPC From Leader S%d In T%d, Entries Len %d", me, curTerm, args.LeaderID, args.Term, len(args.Entries))
	}

	// 首先确认RPC的任期是否合法
	if curTerm > args.Term {
		// 如果Leader的任期小于自己的任期，不合法，返回false
		reply.Success = false
		reply.Term = curTerm
		reply.ConfilictIndex = -1
		reply.ConfilictTerm = -1
		if EnableDebug {
			Debug(dTrace, "S%d At T%d Refuse AppendEntries RPC From Leader S%d In T%d, Entries Len %d", me, curTerm, args.LeaderID, args.Term, len(args.Entries))
		}
		return
	}
	// 其他情况下，Leader的任期都是合法的
	// 首先接收心跳，重置计时器
	// 给自己的timeChan管道发送一个信息，重置倒计时,不论自己是follower还是candidate，都需要重置倒计时
	select {
	case rf.TimeOutChan <- 1:
	default:
	}

	if curTerm == args.Term {
		// 如果leader的Term和自己的一样，说明自己和leader是同时期的candidate，确认自己的身份是candidate，服从先自己一步成为leader的server
		if state == CANDIDATE {
			rf.mu.Lock()
			rf.State = FOLLOWER

			// 更新之前的信息
			state = rf.State
			rf.mu.Unlock()

			if LeaderElectionDebug && EnableDebug {
				Debug(dTrace, "S%d At T%d Recive AppendEntries From Leader S%d In T%d, Convert From Candidate to Follower", me, curTerm, args.LeaderID, args.Term)
			}
		}
	} else {
		// 如果leader的Term大于自己的Term，更新自己的Term，并将身份转换为follower，重置投票项
		rf.mu.Lock()
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
		rf.State = FOLLOWER

		// Refresh ReadInfo
		curTerm = rf.CurrentTerm
		rf.mu.Unlock()

		if LeaderElectionDebug && EnableDebug {
			Debug(dTrace, "S%d At T%d Recive AppendEntries From Leader S%d In T%d, Convert to Follower", me, curTerm, args.LeaderID, args.Term)
		}
	}

	// 不论任期是大于还是等于自己的任期，都需要处理发送来的新增日志条目，不同的是，针对选举的处理
	var myPrevLogTerm int = 0
	rf.mu.Lock()
	logLen := len(rf.Log)
	if args.PrevLogIndex <= logLen-1 {
		myPrevLogTerm = rf.Log[args.PrevLogIndex].Term
	}
	rf.mu.Unlock()

	// 确认自己存在prevLogIndex、prevLogTerm日志
	// 如果prevLogIndex小于等于当前Server的Log长度，那么可以说明prevLogIndex在这个server上存在;否则不存在
	if args.PrevLogIndex <= logLen-1 {
		if myPrevLogTerm == args.PrevLogTerm {
			// 该Server的Log与prevLogIndex、prevLogTerm适配，可以开始处理args中的entries新条目
			if LogAppendDebug && EnableDebug {
				Debug(dLog, "S%d At T%d Satisfied prevLogIndex:%d, prevLogTerm:%d, Start AppendEntries(Len%d)", me, curTerm, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
			}
			for i := 0; i < len(args.Entries); i++ {
				// 计算下一个新日志条目需要插入的位置
				toInsertIndex := args.PrevLogIndex + 1 + i

				var toInsertIndexLogTerm int = 0
				if toInsertIndex <= logLen-1 {
					rf.mu.Lock()
					toInsertIndexLogTerm = rf.Log[toInsertIndex].Term
					rf.mu.Unlock()
				}

				// 如果下一个新日志条目要插入的位置，超过了目前log的容量，也就是该插入位置后面都是空的，那么将后续新条目直接append到log后面，不用再循环了
				if toInsertIndex > logLen-1 {
					toAppend := args.Entries[i:]
					rf.mu.Lock()
					rf.Log = append(rf.Log, toAppend...)
					rf.mu.Unlock()

					if LogAppendDebug && EnableDebug {
						Debug(dLog2, "S%d At T%d, NewEntries:%d Exceed, toInsertIndex:%d, appendLength:%d", me, curTerm, i, toInsertIndex, len(toAppend))
					}
					break
				} else if toInsertIndexLogTerm != args.Entries[i].Term {
					// 在处理后续新追加条目时，出现了索引相同，任期不同的情况，需要将该冲突条目及其后面的条目都删除，并重新追加新的条目
					toAppend := args.Entries[i:] // 获取后续新条目
					rf.mu.Lock()
					rf.Log = rf.Log[:toInsertIndex]      // 将冲突条目及其后面的条目都删除
					rf.Log = append(rf.Log, toAppend...) // 追加后续新条目
					rf.mu.Unlock()

					if LogAppendDebug && EnableDebug {
						Debug(dLog2, "S%d At T%d, NewEntries:%d TermConfilict, toInsertIndex:%d, appendLength:%d", me, curTerm, i, toInsertIndex, len(toAppend))
					}
					break // 所有新条目追加完毕，跳出循环
				} else {
					// 需要追加的条目，在该Server上是已有条目，不用处理
					break
				}
			}
			reply.Success = true
			reply.Term = curTerm

			// 检查Follower和Leader的CommitIndex
			CommitAppendLogIndex := args.PrevLogIndex + len(args.Entries)
			targetCommitIndex := int(math.Min(float64(args.LeaderCommit), float64(CommitAppendLogIndex)))

			rf.mu.Lock()
			if args.LeaderCommit > rf.CommitIndex {
				rf.CommitIndex = targetCommitIndex

				// 通知本Server的applier 应用已经提交的日志
				rf.ApplierSyncCond.Signal()
			}
			// Debug Info
			curCommitIndex := rf.CommitIndex
			rf.mu.Unlock()

			if LogAppendDebug && EnableDebug {
				Debug(dCommit, "S%d At T%d, CommitIndex=%d, args.LeaderCommit=%d, appendLogIndex=%d", me, curTerm, curCommitIndex, args.LeaderCommit, CommitAppendLogIndex)
			}
		} else {
			// 如果上述if条件都不满足，说明是发生了任期冲突，告诉Leader将nextIndex设置为当前任期的第一个日志的Index
			// 也就是下次尝试从上一任期的日志的最后开始
			rf.mu.Lock()
			reply.ConfilictTerm = rf.Log[args.PrevLogIndex].Term

			index := args.PrevLogIndex
			for index > 0 && rf.Log[index].Term == reply.ConfilictTerm {
				index--
			}
			rf.mu.Unlock()
			reply.ConfilictIndex = index + 1
		}
	} else {
		// 如果是prevLogIndex大于自己的日志长度，让Leader下次设置nextIndex为logLen，也就是下次从日志末尾尝试
		reply.Success = false
		reply.Term = curTerm
		reply.ConfilictIndex = logLen
		reply.ConfilictTerm = -1
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
	rf.mu.Lock()
	index := len(rf.Log)           // 第一个表示将要添加的日志条目在该Server Log中的位置,由于Log从1开始，并且Log默认有一个占位的条目，因此不需要-1
	term := rf.CurrentTerm         // 表示当前任期
	isLeader := rf.State == LEADER // 表示自己是否是Leader

	me := rf.me
	serverNum := len(rf.peers)
	rf.mu.Unlock()

	// Your code here (3B).

	// 如果不是Leader，直接返回
	if !isLeader {
		return index, term, isLeader
	}

	// 将新条目添加到自己的日志列表中
	newLog := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.mu.Lock()
	rf.Log = append(rf.Log, newLog)
	// 更新Leader的MatchIndex、nextIndex，虽然自己不是follower，但是commitIndex检查中会用到
	rf.MatchIndex[rf.me] = len(rf.Log) - 1
	rf.NextIndex[rf.me] += 1
	rf.mu.Unlock()

	if LogAppendDebug && EnableDebug {
		Debug(dLog, "Leader S%d At T%d, Receive Log", me, term)
	}

	// Leader并行向所有Follower发送新条目的AppendEntries RPC
	// 但是其实不应该在这里触发AppendEntriesRPC发送，而是在Leader的Ticker周期触发中，与心跳发送机制融合
	// Leader每轮检查是否存在新日志，如果没有新日志，就发送心跳，否则发送新日志，同时也作为发送心跳
	// go rf.LeaderAppendEntriesParallelToFollower()

	// 向Leader的每个Follower replicator协程发送新日志到达请求
	for i := 0; i < serverNum; i++ {
		if i == me {
			continue
		}
		select {
		case rf.NewLogChan[i] <- 1:
		default:
		}
	}

	return index, term, isLeader
}

func (rf *Raft) replicator(peer int) {
	if EnableDebug {
		Debug(dTrace, "Leader Start Replicator For Follower S%d", peer)
	}

	// 刚启动时，需要立即触发一次，用于发送Leader心跳
	rf.NewLogChan[peer] <- 1

	for !rf.killed() {
		ms := 100 + (rand.Int63() % 100)
		duration := time.Duration(ms) * time.Millisecond

		select {
		// 收到新日志，开始发送
		case <-rf.NewLogChan[peer]:
			// 超时了，需要发送心跳信息
		case <-time.After(duration):
		}

		// 如果发现自己不是Leader了，那么就退出该协程
		rf.mu.Lock()
		if rf.State != LEADER {
			rf.mu.Unlock()
			return
		}

		curTerm := rf.CurrentTerm
		me := rf.me

		logFrom := rf.NextIndex[peer] // 包含该下标日志
		prevLogIndex := rf.NextIndex[peer] - 1
		prevLogTerm := rf.Log[prevLogIndex].Term

		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.Log[logFrom:],
			LeaderCommit: rf.CommitIndex,
		}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}

		if EnableDebug {
			Debug(dLeader, "S%d Sending AppendEntries RPC to S%d At T%d, Log Length:%d, prevLogIndex:%d", me, peer, args.Term, len(args.Entries), args.PrevLogIndex)
		}
		if ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply); ok {
			// 如果发送AppendEntries RPC返回了失败，那么说明prevLogIndex或者prevLogTerm匹配失败，将Leader针对该Server纪律的nextIndex向前调整一个
			// 失败还有一种情况，就是自己是过期的Leader，对方Follower的Term比自己的高
			if !reply.Success {
				// 加上这个当前任期>=reply任期是因为，如果作为Leader掉线，再发送AppendEntries，收到reply为false的原因是任期不合法，而不是nextIndex不匹配,在这种情况下，不更新nextIndex
				// 把这个条件判断放到里面，是因为，可能会出现reply=false，但是任期小于Follower任期的情况，被判定为AppendEntries RPC成功
				if curTerm >= reply.Term {
					// 首先判断是不是preLogIndex大于其日志长度，如果是，则将nextIndex设置为其日志长度
					var nextIndex int = 0
					if reply.ConfilictTerm == -1 {
						rf.mu.Lock()
						rf.NextIndex[peer] -= 1

						// DebugInfo
						nextIndex = rf.NextIndex[peer]
						rf.mu.Unlock()
					} else {
						// 否则就是发生了任期冲突，首先检查Leader自己是否有该冲突任期的日志
						confilictTermIndex := -1

						rf.mu.Lock()
						for i := len(rf.Log) - 1; i > 0; i-- {
							if rf.Log[i].Term == reply.ConfilictTerm {
								confilictTermIndex = i
								break
							}
						}

						// 如果Leader存在该冲突任期的日志，那么就是该Follower掉线后，Leader日志更新并提交了，
						if confilictTermIndex != -1 {
							rf.NextIndex[peer] = confilictTermIndex + 1
						} else {
							rf.NextIndex[peer] = reply.ConfilictIndex
						}

						// nextIndex安全检查
						if rf.NextIndex[peer] < 1 {
							rf.NextIndex[peer] = 1
						}
						nextIndex = rf.NextIndex[peer]
						rf.mu.Unlock()
					}

					if EnableDebug {
						Debug(dLeader, "S%d Sending AppendEntries RPC to S%d At T%d, Log Length:%d, Failed, new NextIndex:%d", me, peer, args.Term, len(args.Entries), nextIndex)
					}
				} else {
					// 处理自己是过期Leader的情况,原本这种情况，都是由其他几个Server选出更高Term的Leader，由新Leader向自己发送高Term心跳，在AppendEntries中进行身份转换的
					// 但是，在这里处理的情况就是，当剩余Server不足以选出新Leader发出高Term心跳的情况（其实就算只剩下一个Server，只要那个Server超时，发出高Term的拉票请求，应该也能身份转换）
					rf.mu.Lock()
					rf.State = FOLLOWER
					rf.CurrentTerm = reply.Term
					if EnableDebug {
						Debug(dLeader, "S%d Convert to Follower At T%d", rf.me, rf.CurrentTerm)
					}
					rf.mu.Unlock()
					return
				}
			} else {
				// 如果Follower成功追加，需要Leader更新对应的matchIndex、nextIndex，甚至commitIndex
				rf.mu.Lock()
				rf.MatchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.NextIndex[peer] = rf.MatchIndex[peer] + 1

				// DebugInfo
				nextIndex := rf.NextIndex[peer]
				rf.mu.Unlock()

				if EnableDebug {
					Debug(dLeader, "S%d Sending AppendEntries RPC to S%d At T%d, Log Length:%d,Success, new NextIndex:%d", me, peer, args.Term, len(args.Entries), nextIndex)
				}

				// 调用Leader的commitIndex更新函数
				rf.LeaderRefreshCommitIndex()

				// 发送完成本轮日志后，如果发现又增加了新日志，那么跳过睡眠，直接发送下一轮新日志
				rf.mu.Lock()
				if rf.NextIndex[peer] < len(rf.Log) {
					select {
					case rf.NewLogChan[peer] <- 1:
					default:
					}
				}
				rf.mu.Unlock()
			}
		} else {
			// 如果 RPC发送失败，即没有回复，那么跳出该次发送循环，等待下一次Leader心跳周期再发送日志
			continue
		}
	}
}

func (rf *Raft) LeaderRefreshCommitIndex() {
	rf.mu.Lock()
	// Debug Info
	me := rf.me

	// Code Logic Info
	targetCommitIndex := rf.CommitIndex + 1
	logLen := len(rf.Log)
	serverNum := len(rf.peers)
	rf.mu.Unlock()

	var effectiveNum int = int(math.Ceil(float64(serverNum) / 2))

	for ; targetCommitIndex < logLen; targetCommitIndex++ {
		count := 0
		// 检查每个Server已经复制的Log数量，如果满足大多数都复制的情况，就可以进行下一个索引位置的检查
		rf.mu.Lock()
		matchIndex := rf.MatchIndex
		rf.mu.Unlock()
		for i := 0; i < serverNum; i++ {
			// 不能将任期检查放在这里，否则无法处理 T1的0~49， T13的400都被大多数服务器复制，但是检查却检查不到T13的400，而无法一次性提交前面所有的日志的情况
			if matchIndex[i] >= targetCommitIndex {
				count++
			}
			if LogAppendDebug && EnableDebug {
				Debug(dCommit, "Leader S%d Check CommitIndex, targetCommitIndex=%d, MatchIndex[%d]=%d, curCount=%d, effctiveNum=%d", me, targetCommitIndex, i, matchIndex[i], count, effectiveNum)
			}
		}

		// 如果该索引位置的日志大多数服务器有复制，就再次检查该index的任期是否与当前任期相同，相同则更新
		// 不同则继续检查，万一现在检查的都是过去任期的日志，但是都被复制，但是没有提交，而在最后又来一个当前任期的日志都被复制，就都要一次性提交了
		if count >= effectiveNum {
			rf.mu.Lock()
			targetLogTerm := rf.Log[targetCommitIndex].Term
			curTerm := rf.CurrentTerm
			rf.mu.Unlock()

			if targetLogTerm == curTerm {
				rf.mu.Lock()
				rf.CommitIndex = targetCommitIndex
				rf.mu.Unlock()

				// 通知Leader的applier应用已经提交的日志
				rf.ApplierSyncCond.Signal()

				if LogAppendDebug && EnableDebug {
					Debug(dCommit, "Leader S%d At T%d, Refresh CommitIndex:%d", me, curTerm, targetCommitIndex)
				}
			}
		} else {
			// 如果索引位置的日志，大多数服务器都没有复制，那targetCommitIndex的检查到此结束
			break
		}
	}
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

func (rf *Raft) applier() {
	// 如果更新了commitIndex，就要将新提交的command应用到状态机
	if !rf.killed() {
		// 启动时先获取锁，检查状态
		rf.mu.Lock()

		// 使用for循环，避免条件变量虚假唤醒
		for rf.LastApplied >= rf.CommitIndex {
			rf.ApplierSyncCond.Wait()

			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		// 将需要提交的日志信息全部复制一份
		lastApplied := rf.LastApplied
		commitIndex := rf.CommitIndex

		toApplyEntries := make([]LogEntry, commitIndex-lastApplied)
		copy(toApplyEntries, rf.Log[lastApplied+1:commitIndex+1]) // 左闭右开

		// 更新LastApplied的状态
		rf.LastApplied = commitIndex
		rf.mu.Unlock()

		for i, entry := range toApplyEntries {
			applyMsg := raftapi.ApplyMsg{
				CommandValid:  true,
				Command:       entry.Command,
				CommandIndex:  lastApplied + 1 + i,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			if LogAppendDebug && EnableDebug {
				Debug(dLog2, "S%d At T%d, Apply Index:%d, LogTerm:%d, LogCommand:%v", rf.me, rf.CurrentTerm, lastApplied+1+i, entry.Term, applyMsg.Command)
			}
			rf.ApplyChan <- applyMsg
		}
	}
}

func (rf *Raft) FollowerCase(me int) {

	// 如果为follower状态,等待接收leader的心跳
	// 设定本轮的超时时间，设定为1.5秒的1-2倍随机，因为要求在5秒内选出leader
	// curDuration := time.Duration(TIMTOUTDURATION * (float32(rf.me) + 1) / 2 * float32(time.Millisecond))

	var curDuration time.Duration
	if isRandom {
		// 设定超时时间为250~350ms
		curDuration = time.Duration((TIMTOUTDURATION_INTERVAL*rand.Float32() + BASE_TIMEOUT_DURATION) * float32(time.Millisecond))
	} else {
		curDuration = time.Duration((SERVER_TIMEOUT*float32(me+1) + SERVER_BASE_TIMEOUT) * float32(time.Millisecond))
	}

	select {
	case <-rf.TimeOutChan:
		// 收到leader的心跳，重置倒计时，即进入下一轮倒计时
		if LeaderElectionDebug && EnableDebug {
			Debug(dTrace, "S%d Receive HeatBeat", me)
		}
		return
	case <-time.After(curDuration):
		// 如果超时没有收到leader的心跳，将自己转换身份为candidate，并将自己的term+1
		// 由于Term+1，自己变成candidate，将投票投给自己，如果重置，可能会在发送拉票选举之前，投票给其他server
		rf.mu.Lock()
		rf.CurrentTerm += 1
		rf.VoteFor = rf.me
		rf.State = CANDIDATE

		// DebugInfo
		curTerm := rf.CurrentTerm
		rf.mu.Unlock()

		if LeaderElectionDebug && EnableDebug {
			Debug(dTimer, "S%d TimeOut Convert State From Follower to Candidate At T%d", me, curTerm)
		}
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
				if LeaderElectionDebug && EnableDebug {
					Debug(dTrace, "S%d Candidate SendRequestVote To S%d At T%d", args.CandidateID, i, args.Term)
				}
				if ok := rf.sendRequestVote(i, &args, &reply); ok {
					requestVoteReplyChan <- reply
					if LeaderElectionDebug && EnableDebug {
						Debug(dTrace, "S%d Candidate Recevie RequestVoteReply From S%d At T%d", args.CandidateID, i, args.Term)
					}
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
				rf.NewLogChan = make([]chan int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.NextIndex[i] = len(rf.Log)
					rf.MatchIndex[i] = 0
					rf.NewLogChan[i] = make(chan int, 1) // 为每个向Follower发送协程初始化日志到达管道

					// 启动Leader针对该follower的日志/心跳发送协程
					if i != me {
						go rf.replicator(i)
					}
				}
				rf.mu.Unlock()

				if LeaderElectionDebug && EnableDebug {
					Debug(dTrace, "S%d Candidate SendRequestVote Done At T%d, Success Come Leader", me, curTerm)
				}

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
				rf.mu.Unlock()

				if LeaderElectionDebug && EnableDebug {
					Debug(dTrace, "S%d Candidate SendRequestVote Receive Higher Term At T%d, Convert to Follower", me, reply.Term)
				}

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

	// 将发送拉票请求包装为一个函数，并使用goroutine方式运行，
	go rf.CandidateSendVoteRequestParallel(guaranteedNum, effectiveNum, serverNum)

	var curDuration time.Duration
	if isRandom {
		curDuration = time.Duration((TIMTOUTDURATION_INTERVAL*rand.Float32() + BASE_TIMEOUT_DURATION) * float32(time.Millisecond))
	} else {
		curDuration = time.Duration((SERVER_TIMEOUT*float32(me+1) + SERVER_BASE_TIMEOUT) * float32(time.Millisecond))
	}

	select {
	case <-rf.TimeOutChan:
		return
	case <-time.After(curDuration):
		// 如果拉票环节超时，将自己的term+1，进入下一轮的拉票环节
		rf.mu.Lock()
		rf.CurrentTerm += 1

		//DebugInfo
		curTerm := rf.CurrentTerm
		rf.mu.Unlock()

		if LeaderElectionDebug && EnableDebug {
			Debug(dTrace, "S%d Candidate TimeOut Increate Term From T%d to T%d", me, curTerm-1, curTerm)
		}
	}
}

func (rf *Raft) LeaderCase() {
	time.Sleep(50 * time.Millisecond)
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
			// rf.LeaderCase()	// 在3A测试中，没有实现日志相关操作时，使用LeaderCase
			// 在3B部分，由于实现了日志相关操作，心跳发送其实包含在日志操作中，使用Replicator， Leader需要做的所有事情都在replicator中了
			rf.LeaderCase()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		// 该睡眠时间需要默认开启，否则一方面导致ticker CPU占用拉满，CPU无空余处理其他RPC请求，另一方面导致领导人选举出现逻辑错误
		// if enableSleep {
		// 	ms := 50 + (rand.Int63() % 300)
		// 	time.Sleep(time.Duration(ms) * time.Millisecond)
		// }
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
	ME = me

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.Log[0] = LogEntry{0, nil} // 该条用于占位，有效日志索引从1开始
	rf.CommitIndex = 0
	rf.LastApplied = 0

	rf.State = 0                       // 服务器状态初始化为follower
	rf.TimeOutChan = make(chan int, 1) // 初始化一个通道，防止发送方阻塞
	rf.ApplyChan = applyCh             // 初始化状态机提交管道
	rf.ApplierSyncCond = *sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Debug(dInfo, "S%d Server initialized success, run ticker", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier() // 应用已经被提交的日志

	return rf
}
