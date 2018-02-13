package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"
// import "crypto/rand"
// import "math/big"
import "math/rand"

type Clerk struct {
	mu     sync.Mutex // one RPC at a time
	sm     *shardmaster.Clerk
	config shardmaster.Config
	// You'll have to modify Clerk.
	id int                      // unique id serves as a client identifier
	get_request_id func() int   // returns unique request ids (among requests by this client)
}


func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	// You'll have to modify MakeClerk.
	ck.id = rand.Int()
 	ck.get_request_id = make_int_generator()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// You'll have to modify Get().
	client_id := ck.id
 	request_id := ck.get_request_id()

	for {
		shard := key2shard(key)

		gid := ck.config.Shards[shard]

		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				args := &GetArgs{}
				args.Client_id = client_id
				args.Request_id = request_id
				args.Key = key
				var reply GetReply
				ok := call(srv, "ShardKV.Get", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for a new configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// send a Put or Append request.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// You'll have to modify PutAppend().
	client_id := ck.id
    request_id := ck.get_request_id()

	for {
		shard := key2shard(key)

		gid := ck.config.Shards[shard]

		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				args := &PutAppendArgs{}
				args.Client_id = client_id
				args.Request_id = request_id
				args.Key = key
				args.Value = value
				args.Op = op
				var reply PutAppendReply
				ok := call(srv, "ShardKV.PutAppend", args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for a new configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

/*
Returns a function which is a generator of a deterministic (read: not unique across 
instances) sequence of unique, incrementing ints and provides such an int each time
it is called.
*/
func make_int_generator() (func() int) {
  base_id := -1
  return func() int {
    base_id += 1
    return base_id
  }
}

