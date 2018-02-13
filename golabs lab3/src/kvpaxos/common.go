package kvpaxos

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrPut = "Undefined Put Error"
  ErrGet = "Undefined Get Error"
  Nobody = "Nobody"
  Put = "Put"
  Append = "Append"
  Get = "Get"
)
type Err string


// Put or Append
type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
  Op  string // "Put" or "Append"
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  From string
  SeqNum int
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  From string
  SeqNum int
}

type GetReply struct {
  Err Err
  Value string
}

