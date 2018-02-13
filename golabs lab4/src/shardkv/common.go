package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	NotReady = "NotReady"
)

type Err string

type Args interface{}
type OpResult interface{}
type Reply interface{}

type ReConfigStartArgs struct {}
type ReConfigEndArgs struct {}
type NoopArgs struct {}

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client_id int       // client_id
 	Request_id int      // request_id unique per client

}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client_id int      // client_id
	Request_id int     // request_id unique per client
}

type GetReply struct {
	Err   Err
	Value string
}


// ReceiveShardArgs and ReceiveShardReply
///////////////////////////////////////////////////////////////////////////////

type ReceiveShardArgs struct {
  Kvpairs []KVPair     // slice of Key/Value pairs
  Trans_to int         // config number the sender is transitioning to
  Shard_index int      // index of shard being sent
}

type ReceiveShardReply struct {
  Err Err
}

type SentShardArgs struct {
  Trans_to int        // config number the sender is transitioning to
  Shard_index int     // index of shard that was successfully sent
}

