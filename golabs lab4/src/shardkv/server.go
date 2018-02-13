package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
  Get = "Get"
  Put = "Put"
  Append = "Append"
  ReConfigStart = "ReConfigStart"
  SentShard = "SentShard"
  ReceiveShard = "ReceiveShard"
  ReConfigEnd = "ReConfigEnd"
  Noop = "Noop"
)

type Op struct {
	// Your definitions here.
	Id string          // uuid, identifies the operation itself
  //Request_id string  // combined, stringified client_id:request_id, identifies the client requested operation
	Name string        // Operation name: Get, Put, ConfigChange, Transfer, ConfigDone
	Args Args          // GetArgs, PutAppendArgs, etc.
}

func generate_uuid() string {
  return strconv.Itoa(rand.Int())
}

func makeOp(name string, args Args) (Op) {
	return Op{Id: generate_uuid(),
			Name: name,
			Args: args,
			}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	config_now shardmaster.Config    // latest Config of replica groups
	config_prior shardmaster.Config  // previous Config of replica groups
	shards []bool                    // whether or not ith shard is present
	transition_to int                // Num of new Config transitioning to, -1 if not transitioning
	// Key/Value State
	operation_number int             // agreement number of latest applied operation
	storage map[string]string        // key/value data storage
	cache map[string]Reply           // "client_id:request_id" -> reply cache  
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Don't accept if no initial config yet.
	if kv.config_now.Num == 0 {
		return nil
	}

	operation := makeOp(Get, *args)                      // requested Op
	agreement_number := kv.paxos_agree(operation)      // sync call returns after agreement reached

	kv.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
	op_result := kv.perform_operation(agreement_number, operation)  // perform requested Op
	get_result := op_result.(GetReply)                   // type assertion
	reply.Value = get_result.Value
	reply.Err = get_result.Err
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Don't accept if no initial config yet.
	if kv.config_now.Num == 0 {
		return nil
	}

	operation := makeOp(args.Op, *args)              // requested Op
	agreement_number := kv.paxos_agree(operation)      // sync call returns after agreement reached

	kv.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
	op_result := kv.perform_operation(agreement_number, operation)  // perform requested Op
	putAppend_result := op_result.(PutAppendReply)                   // type assertion
	reply.Err = putAppend_result.Err
	return nil
}

/*
Accepts a ReceiveShard, starts and awaits Paxos agreement for the op, performs all
operations up to and then including the requested operation.
Does not respond until the requested operation has been applied.
*/
func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  // Don't accept if no initial config yet.
  if kv.config_now.Num == 0 {
    return nil
  }

  operation := makeOp(ReceiveShard, *args)             // requested Op
  agreement_number := kv.paxos_agree(operation)      // sync call returns after agreement reached

  kv.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
  op_result := kv.perform_operation(agreement_number, operation)  // perform requested Op
  receive_result := op_result.(ReceiveShardReply)      // type assertion
  reply.Err = receive_result.Err
  return nil
}


//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.ensure_updated()

  if kv.transition_to == -1 {         // Not currently changing Configs
    // Special initial case
    if kv.config_now.Num == 0 {
      config := kv.sm.Query(1)
      if config.Num == 1 {
        kv.config_prior = kv.config_now
        kv.config_now = config
        // No shard transfers needed. Automatically have shards of first valid Config.
        kv.shards = shard_state(kv.config_now.Shards, kv.gid)
        return
      }
      // No Join has been performed yet. ShardMaster still has initial Config
      return
    }

    // are there new Configs we this replica group should be conforming to?
    config := kv.sm.Query(-1)     // type ShardMaster.Config
    if config.Num > kv.config_now.Num {      // ShardMaster reporting a new Config
      operation := makeOp(ReConfigStart, ReConfigStartArgs{})  // requested Op
      agreement_number := kv.paxos_agree(operation)      // sync call returns after agreement reached

      kv.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
      kv.perform_operation(agreement_number, operation)  // perform requested Op
    } else {
      // Otherwise, no new Config and no action needed
    }
  } else {                           // Currently changing Configs
    kv.broadcast_shards()
    
    if kv.done_sending_shards() && kv.done_receiving_shards() {
      operation := makeOp(ReConfigEnd, ReConfigEndArgs{})  // requested Op
      agreement_number := kv.paxos_agree(operation)      // sync call returns after agreement reached

      kv.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
      kv.perform_operation(agreement_number, operation)  // perform requested Op
    }
  }
  return
}

func (kv *ShardKV) paxos_agree(operation Op) (int) {
  var agreement_number int
  var decided_operation = Op{}

  for decided_operation.Id != operation.Id {
    agreement_number = kv.available_agreement_number()
    kv.px.Start(agreement_number, operation)
    decided_operation = kv.await_paxos_decision(agreement_number).(Op)  // type assertion
  }
  return agreement_number
}

func (kv *ShardKV) await_paxos_decision(agreement_number int) (decided_val interface{}) {
  sleep_max := 10 * time.Second
  sleep_time := 10 * time.Millisecond
  for {
    has_decided, decided_val := kv.px.Status(agreement_number)
    if has_decided == paxos.Decided{
      return decided_val
    }
    time.Sleep(sleep_time)
    if sleep_time < sleep_max {
      sleep_time *= 2
    }
  }
  panic("unreachable")
}

func (kv *ShardKV) available_agreement_number() int {
  return kv.px.Max() + 1
}


/*
Wrapper around the server's paxos instance px.Status call which converts the (bool,
interface{}) value returned by Paxos into a (bool, Op) pair. 
Accepts the agreement number which should be passed to the paxos Status call and 
panics if the paxos value is not an Op.
*/
func (kv *ShardKV) px_status_op(agreement_number int) (bool, Op){
  has_decided, value := kv.px.Status(agreement_number)
  if has_decided == paxos.Decided {
    operation, ok := value.(Op)    // type assertion, Op expected
    if ok {
        return true, operation
    }
    panic("expected Paxos agreement instance values of type Op at runtime. Type assertion failed.")
  }
  return false, Op{}
}


/*
Attempts to use the given operation struct to drive agreement among Shardmaster paxos 
peers using the given agreement number. Discovers the operation that was decided on
for the specified agreement number.
*/
func (kv *ShardKV) drive_discovery(operation Op, agreement_number int) Op {
  kv.px.Start(agreement_number, operation)
  decided_operation := kv.await_paxos_decision(agreement_number).(Op)  // type assertion
  return decided_operation
}

/*
Performs Paxos agreement to submit a Noop operation and performs all operations up to
and including the agreed upon Noop
*/
func (kv *ShardKV) ensure_updated() {
  noop := makeOp(Noop, NoopArgs{})                     // requested Op
  agreement_number := kv.paxos_agree(noop)           // sync call returns after agreement reached

  kv.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
  kv.perform_operation(agreement_number, noop)       // perform requested Op
}


// Methods for Performing ShardKV Operations
///////////////////////////////////////////////////////////////////////////////

/*
Synchronously performs all operations up to but NOT including the 'limit' op_number.
The set of operations to be performed may not all yet be known to the local paxos
instance so it will propose No_Ops to discover missing operations.
*/
func (kv *ShardKV) perform_operations_prior_to(limit int) {
  op_number := kv.operation_number + 1     // op number currently being performed
  has_decided, operation := kv.px_status_op(op_number)

  for op_number < limit {       // continue looping until op_number == limit - 1 has been performed   
    if has_decided {
      kv.perform_operation(op_number, operation)   // perform_operation mutates kv.operation_number
      op_number = kv.operation_number + 1
      has_decided, operation = kv.px_status_op(op_number)
    } else {
      noop := makeOp(Noop, NoopArgs{})          // Force Paxos instance to discover next operation or agree on a Noop
      kv.drive_discovery(noop, op_number)     // synchronously proposes Noop and discovered decided operation
      has_decided, operation = kv.px_status_op(op_number)
      kv.perform_operation(op_number, operation)
      op_number = kv.operation_number + 1
      has_decided, operation = kv.px_status_op(op_number)
    }
  }
}

/*
Accepts an Op operation which should be performed locally, reads the name of the
operation and calls the appropriate handler by passing the operation arguments.
Returns OpResult from performing the operation and increments (mutates) the ShardKV
operation_number field to the latest pperation (performed in increasing order).
*/
func (kv *ShardKV) perform_operation(op_number int, operation Op) OpResult {
  var result OpResult

  switch operation.Name {
    case "Get":
      var get_args = (operation.Args).(GetArgs)     // type assertion, Args is a GetArgs
      result = kv.get(&get_args)
    case "Put":
      var put_args = (operation.Args).(PutAppendArgs)     // type assertion, Args is a PutAppendArgs
      result = kv.put(&put_args)
    case "Append":
      var append_args = (operation.Args).(PutAppendArgs)     // type assertion, Args is a PutAppendArgs
      result = kv.append(&append_args)
    case "ReceiveShard":
      var receive_shard_args = (operation.Args).(ReceiveShardArgs)     // type assertion
      result = kv.receive_shard(&receive_shard_args)
    case "ReConfigStart":
      var re_config_start_args = (operation.Args).(ReConfigStartArgs)  // type assertion
      result = kv.re_config_start(&re_config_start_args)
    case "SentShard":
      var sent_shard_args = (operation.Args).(SentShardArgs)           // type assertion
      result = kv.sent_shard(&sent_shard_args)
    case "ReConfigEnd":
      var re_config_end_args = (operation.Args).(ReConfigEndArgs)     // type assertion
      result = kv.re_config_end(&re_config_end_args)
    case "Noop":
      // zero-valued result of type interface{} is nil
    default:
      panic(fmt.Sprintf("unexpected Op name '%s' cannot be performed", operation.Name))
  }
  kv.operation_number = op_number     // latest operation that has been applied
  kv.px.Done(op_number)               // local Paxos no longer needs to remember Op
  return result
}

// Methods for Managing Shard State
///////////////////////////////////////////////////////////////////////////////

/*
Checks against kv.config_now.Shards to see if the shardkv server is/(will be once
ongoing xfer complete) responsible for the given key. Returns true if so and false
otherwise.
*/
func (kv *ShardKV) owns_shard(key string) bool {
  shard_index := key2shard(key)
  return kv.config_now.Shards[shard_index] == kv.gid
}


/*
Returns whether or not the shardkv server currently has the shard corresponding
to the given stirng key. Consults the kv.shards []bool state slice which 
represents which shards are present. kv.shards is kept up to date during shard 
transfers that occur during config transitions.
*/
func (kv *ShardKV) has_shard(key string) bool {
  shard_index := key2shard(key)
  return kv.shards[shard_index]
}

/*
Converts a shards array of int64 gids (such as Config.Shards) into a slice of
booleans of the same length where an entry is true if the gid of the given 
shards array equals my_gid and false otherwise.
*/
func shard_state(shards [shardmaster.NShards]int64, my_gid int64) []bool {
  shard_state := make([]bool, len(shards))
  for shard_index, gid := range shards {
    if gid == my_gid {
      shard_state[shard_index] = true
    } else {
      shard_state[shard_index] = false
    }
  }
  return shard_state
}

func (kv *ShardKV) done_sending_shards() bool {
  goal_shards := shard_state(kv.config_now.Shards, kv.gid)
  for shard_index, _ := range kv.shards {
    if kv.shards[shard_index] == true && goal_shards[shard_index] == false {
      // still at least one send has not been acked
      return false
    }
  } 
  return true
}

func (kv *ShardKV) done_receiving_shards() bool {
  goal_shards := shard_state(kv.config_now.Shards, kv.gid)
  for shard_index, _ := range kv.shards {
    if kv.shards[shard_index] == false && goal_shards[shard_index] == true {
      // still at least one send has not been received
      return false
    }
  } 
  return true
}

func (kv *ShardKV) broadcast_shards() {
  goal_shards := shard_state(kv.config_now.Shards, kv.gid)
  for shard_index, _ := range kv.shards {
    if kv.shards[shard_index] == true && goal_shards[shard_index] == false {
      // shard_index should be transferred to gid in new config
      new_replica_group_gid := kv.config_now.Shards[shard_index]
      kv.send_shard(shard_index, new_replica_group_gid)
    }
  } 
  return
}

type KVPair struct {
  Key string
  Value string
}

func (kv *ShardKV) send_shard(shard_index int, gid int64) {
  // collect the key/value pairs that are part of the shard to be transferred
  var kvpairs []KVPair
  for key,value := range kv.storage {
    if key2shard(key) == shard_index {
      kvpairs = append(kvpairs, KVPair{Key: key, Value: value})
    }
  }

  servers := kv.config_now.Groups[gid]
  // next_rg_server := servers[rand.Intn(len(servers))]
  // args := &ReceiveShardArgs{}    // declare and init struct with zero-valued fields
  //   args.Kvpairs = kvpairs
  //   args.Trans_to = kv.transition_to
  //   args.Shard_index = shard_index
  //   var reply ReceiveShardReply
  //   // Attempt to send shard to random server in replica group now owning the shard
  //   ok := call(next_rg_server, "ShardKV.ReceiveShard", args, &reply)
  //   if ok && reply.Err == OK {
  //     sent_shard_args := SentShardArgs{Shard_index: shard_index, Trans_to: kv.transition_to}
  //     operation := makeOp(SentShard, sent_shard_args)      // requested Op
  //     agreement_number := kv.paxos_agree(operation)      // sync call returns after agreement reached
  //     kv.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
  //     kv.perform_operation(agreement_number, operation)  // perform requested Op
  //   }

  for _, srv := range servers {
    args := &ReceiveShardArgs{}    // declare and init struct with zero-valued fields
    args.Kvpairs = kvpairs
    args.Trans_to = kv.transition_to
    args.Shard_index = shard_index
    var reply ReceiveShardReply
    // Attempt to send shard to random server in replica group now owning the shard
    ok := call(srv, "ShardKV.ReceiveShard", args, &reply)
    if ok && reply.Err == OK {
      sent_shard_args := SentShardArgs{Shard_index: shard_index, Trans_to: kv.transition_to}
      operation := makeOp(SentShard, sent_shard_args)      // requested Op
      agreement_number := kv.paxos_agree(operation)      // sync call returns after agreement reached
      kv.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
      kv.perform_operation(agreement_number, operation)  // perform requested Op
      return
    }
  }
}

/*
Used when memory of a shard that was live in the past can be deleted which removes all
key/value pairs in storage corresponding to the shard and marks the kv.shards entry
for the shard as false since the shard has now been sent and should be the same as the
goal shards state.
*/ 
func (kv *ShardKV) remove_shard(shard_index int) {
  for key, _ := range kv.storage {
    if key2shard(key) == shard_index {
      delete(kv.storage, key)
    }
  }
  kv.shards[shard_index] = false    // shard is no longer maintained on shardkv server
}

// ShardKV RPC operations (internal, performed after paxos agreement)
///////////////////////////////////////////////////////////////////////////////

/*
If the key to get is in a shard that is owned by the shardkv's replica group and 
present then a standard get is performed and the reply cached and returned. If the 
the key's shard is owned by the replica group, but not yet present, a reply with
an error is returned, but the reply is not cached since the client is expected to
retry at a later point. Finally, if the key is in a shard that is not owned by the 
replica group, an ErrWrongGroup reply is cached and returned.
Caller responsible for attaining lock on shardkv properties.
*/
func (kv *ShardKV) get(args *GetArgs) OpResult {
  client_request := request_identifier(args.Client_id, args.Request_id) // string

  reply, present := kv.cache[client_request]
  if present {
    return reply       // client requested get has already been performed
  }

  // client requested get has not been performed
  get_reply := GetReply{}

  if kv.owns_shard(args.Key) {
    // currently or soon to be responsible for the key
    if kv.has_shard(args.Key) {
      // currently has the needed shard (note: config transition may still be in progress)
      value, present := kv.storage[args.Key]
      if present {
        get_reply.Value = value
        get_reply.Err = OK
      } else {
        get_reply.Value = ""
        get_reply.Err = ErrNoKey
      }
      // cache get reply so duplicate client requests not performed
      kv.cache[client_request] = get_reply    
      return get_reply
    }
    // waiting to receive the shard
    get_reply.Err = NotReady
    // do not cache, expecting client to retry after transition progress
    return get_reply
  }
  // otherwise, the replica group does not own the needed shard
  get_reply.Err = ErrWrongGroup
  // client may know a new config that shardkv doesn't. Don't cache rejection.
  return get_reply   
}

/*
*/
func (kv *ShardKV) put(args *PutAppendArgs) OpResult {
  client_request := request_identifier(args.Client_id, args.Request_id) // string

  reply, present := kv.cache[client_request]
  if present {
    return reply                   // client requested put has already been performed
  }

  // client requested put has not been performed
  put_reply := PutAppendReply{}

  if kv.owns_shard(args.Key) {
    // currently or soon to be responsible for the key
    if kv.has_shard(args.Key) {
      // currently has the needed shard (note: config transition may still be in progress)
      kv.storage[args.Key] = args.Value
      put_reply := PutAppendReply{Err: OK}             // reply for successful Put request

      // cache put reply so duplicate client requests not performed
      kv.cache[client_request] = put_reply    
      return put_reply
    }
    // waiting to receive shard
    put_reply.Err = NotReady
    // do not cache, expecting client to retry after transition progress
    return put_reply
  }

  // otherwise, the replica group does not own the needed shard
  put_reply.Err = ErrWrongGroup
  // client may know a new config that shardkv doesn't. Don't cache rejection.
  return put_reply
}



func (kv *ShardKV) append(args *PutAppendArgs) OpResult {
  client_request := request_identifier(args.Client_id, args.Request_id) // string

  reply, present := kv.cache[client_request]
  if present {
    return reply                   // client requested put has already been performed
  }

  // client requested put has not been performed
  append_reply := PutAppendReply{}

  if kv.owns_shard(args.Key) {
    // currently or soon to be responsible for the key
    if kv.has_shard(args.Key) {
      // currently has the needed shard (note: config transition may still be in progress)
      kv.storage[args.Key] = kv.storage[args.Key] + args.Value
      append_reply := PutAppendReply{Err: OK}             // reply for successful Append request

      // cache append reply so duplicate client requests not performed
      kv.cache[client_request] = append_reply    
      return append_reply
    }
    // waiting to receive shard
    append_reply.Err = NotReady
    // do not cache, expecting client to retry after transition progress
    return append_reply
  }

  // otherwise, the replica group does not own the needed shard
  append_reply.Err = ErrWrongGroup
  // client may know a new config that shardkv doesn't. Don't cache rejection.
  return append_reply
}


/*
Local or fellow ShardKV peer has detected that ShardMaster has a more recent Config.
All peers have paxos agreed that this operation marks the transition to the next 
higher Config. Fetch the numerically next Config from the ShardMaster and fire first
set of shard transfer broadcasts. Also set shard_transition_period to true. The
shard_transition_period will be over once a peers paxos agree on a ReConfigEnd 
operation.
*/
func (kv *ShardKV) re_config_start(args *ReConfigStartArgs) OpResult {

  if kv.transition_to == kv.config_now.Num {  // If currently transitioning
    // Multiple peers committed ReConfigStart operations, only need to start once
    fmt.Printf("Already transitioning to %d\n", kv.transition_to)
    return nil
  }
  next_config := kv.sm.Query(kv.config_now.Num + 1)    // next Config
  kv.config_prior = kv.config_now
  kv.config_now = next_config
  kv.transition_to = kv.config_now.Num

  kv.shards = shard_state(kv.config_prior.Shards, kv.gid)
  // goal_shards := shard_state(kv.config_now.Shards, kv.gid)

  return nil
}

func (kv *ShardKV) receive_shard(args *ReceiveShardArgs) OpResult {
  client_request := internal_request_identifier(args.Trans_to, args.Shard_index) // string

  reply, present := kv.cache[client_request]
  if present {
    return reply       // client requested ReceiveShard has already been performed
  }

  // client requested get has not been performed
  receive_shard_reply := ReceiveShardReply{}

  // not yet transitioning or working on an earlier transition
  if kv.transition_to < args.Trans_to {
    // do not cache the reply, expect sender to resend after we've caught up
    receive_shard_reply.Err = NotReady
    return receive_shard_reply

  } else if kv.transition_to == args.Trans_to {
    // working on same transition
    for _, pair := range args.Kvpairs {
      kv.storage[pair.Key] = pair.Value
    }
    kv.shards[args.Shard_index] = true   // key/value pairs for the shard have been received 
    receive_shard_reply.Err = OK
    kv.cache[client_request] = receive_shard_reply
    return receive_shard_reply
  } 
  // kv.transition_to > args.trans_to, already received all shards needed.
  receive_shard_reply.Err = OK
  kv.cache[client_request] = receive_shard_reply
  return receive_shard_reply
}


func (kv *ShardKV) sent_shard(args *SentShardArgs) OpResult {
  if kv.transition_to == args.Trans_to {
    kv.remove_shard(args.Shard_index)
  }
  return nil
}


/*
Local or fellow ShardKV peer has successfully recieved all needed shards for the
new Config (thus other peers can determine from the paxos log) and has received
acks from replica groups that were to receive shards from this replica group during
the transition tofrom all replica groups that 
*/
func (kv *ShardKV) re_config_end(args *ReConfigEndArgs) OpResult {
  if kv.transition_to == -1 {
    return nil
  }
  kv.transition_to = -1            // no longer in transition to a new config
  return nil
}


// Helpers
///////////////////////////////////////////////////////////////////////////////

func request_identifier(client_id int, request_id int) string {
  return strconv.Itoa(client_id) + ":" + strconv.Itoa(request_id)
}

func internal_request_identifier(client_id int, request_id int) string {
  return "i" + request_identifier(client_id, request_id)
}


// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(ReConfigStartArgs{})
	gob.Register(ReConfigEndArgs{})
	gob.Register(ReceiveShardArgs{})
	gob.Register(SentShardArgs{})
	gob.Register(NoopArgs{})
	gob.Register(KVPair{})
	kv.config_prior = shardmaster.Config{}  // initial prior Config
	kv.config_prior.Groups = map[int64][]string{}  // initialize map
	kv.config_now = shardmaster.Config{}  // initial prior Config
	kv.config_now.Groups = map[int64][]string{}  // initialize map
	kv.shards = make([]bool, shardmaster.NShards)
	kv.transition_to = -1
	kv.storage = map[string]string{}        // key/value data storage
	kv.cache =  map[string]Reply{}          // "client_id:request_id" -> reply cache
	kv.operation_number = -1                // first agreement number will be 0
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)


	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
