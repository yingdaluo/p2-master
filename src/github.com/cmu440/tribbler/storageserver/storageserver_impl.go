package storageserver

import (
	"container/list"
	"github.com/cmu440/tribbler/common"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"hash/fnv"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

type storageServer struct {
	serverID             uint32
	numNodes             int
	port                 int
	masterServerHostPort string
	serverList           *list.List
	serverListLock       *sync.Mutex

	elementMap     map[string]string
	elementMapLock *sync.Mutex
	listMap        map[string]*list.List
	listMapLock    *sync.Mutex
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	srv := &storageServer{
		serverID:             nodeID,
		port:                 port,
		numNodes:             numNodes,
		masterServerHostPort: masterServerHostPort,
		serverList:           list.New(),
		serverListLock:       new(sync.Mutex),
		listMap:              make(map[string]*list.List),
		listMapLock:          new(sync.Mutex),
		elementMap:           make(map[string]string),
		elementMapLock:       new(sync.Mutex),
	}
	if srv.masterServerHostPort == "" {
		// run RPC server
		srv.masterServerHostPort = "localhost:" + strconv.Itoa(port)
		rpc.RegisterName("storageServer", srv)
		rpc.HandleHTTP()
		l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			return nil, err
		}
		go http.Serve(l, nil)
	}

	//register to master
	for {
		client, err := rpc.DialHTTP("tcp", srv.masterServerHostPort)
		if err != nil {
			return nil, err
		}
		args := &storagerpc.RegisterArgs{*&storagerpc.Node{"localhost:" + strconv.Itoa(srv.port), srv.serverID}}
		var reply *storagerpc.RegisterReply
		err = client.Call("storageServer.RegisterServer", args, &reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			srv.serverList = list.New()
			for _, node := range reply.Servers {
				srv.serverList.PushBack(node)
			}
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
	return srv, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	node := args.ServerInfo
	found := false
	ss.serverListLock.Lock()
	for e := ss.serverList.Front(); e != nil; e = e.Next() {
		if e.Value.(storagerpc.Node) == node {
			found = true
			break
		}
	}
	if !found {
		ss.serverList.PushBack(node)
	}
	if ss.serverList.Len() == ss.numNodes {
		slice := make([]storagerpc.Node, ss.numNodes)
		i := 0
		for e := ss.serverList.Front(); e != nil; e = e.Next() {
			slice[i] = e.Value.(storagerpc.Node)
			i++
		}
		ss.serverListLock.Unlock()
		reply.Status = storagerpc.OK
		reply.Servers = slice
		return nil
	} else {
		ss.serverListLock.Unlock()
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
		return nil
	}
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.serverListLock.Lock()
	if ss.serverList.Len() < ss.numNodes {
		ss.serverListLock.Unlock()
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
		return nil
	} else {
		slice := make([]storagerpc.Node, ss.numNodes)
		i := 0
		for e := ss.serverList.Front(); e != nil; e = e.Next() {
			slice[i] = e.Value.(storagerpc.Node)
			i++
		}
		ss.serverListLock.Unlock()
		reply.Status = storagerpc.OK
		reply.Servers = slice
		return nil
	}
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	ss.serverListLock.Lock()
	if ss.serverList.Len() < ss.numNodes {
		ss.serverListLock.Unlock()
		reply.Status = storagerpc.NotReady
		reply.Value = ""
		return nil
	}
	ss.serverListLock.Unlock()

	key := args.Key
	if !ss.validServer(StoreHash(key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.elementMapLock.Lock()
	value, ok := ss.elementMap[key]
	if !ok {
		ss.elementMapLock.Unlock()
		reply.Status = storagerpc.ItemNotFound
		return nil
	}

	ss.elementMapLock.Unlock()
	reply.Status = storagerpc.OK
	reply.Value = value

	if args.WantLease == true {
		//TODO: grant a lease
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	ss.serverListLock.Lock()
	if ss.serverList.Len() < ss.numNodes {
		ss.serverListLock.Unlock()
		reply.Status = storagerpc.NotReady
		reply.Value = nil
		return nil
	}
	ss.serverListLock.Unlock()
	reply.Status = storagerpc.OK

	key := args.Key
	if !ss.validServer(StoreHash(key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.listMapLock.Lock()
	list, ok := ss.listMap[key]
	if !ok {
		ss.listMapLock.Unlock()
		reply.Status = storagerpc.ItemNotFound
		return nil
	}

	slice := make([]string, list.Len())
	i := 0
	for e := list.Front(); e != nil; e = e.Next() {
		value := e.Value.(string)
		slice[i] = value
		i++
	}

	ss.listMapLock.Unlock()
	reply.Value = slice
	if args.WantLease == true {
		//TODO: grant a lease
	}

	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	if !ss.validServer(StoreHash(key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	value := args.Value
	ss.elementMapLock.Lock()
	if _, ok := ss.elementMap[key]; ok {
		ss.elementMapLock.Unlock()
		reply.Status = storagerpc.ItemExists
		return nil
	} else {
		ss.elementMap[key] = value
		ss.elementMapLock.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	if !ss.validServer(StoreHash(key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	suffix := strings.Split(key, ":")[1]
	if suffix == "tribbleList" {
		ss.listMapLock.Lock()
		if l, ok := ss.listMap[key]; ok { // There is already a list for that key
			var value string
			for e := l.Front(); e != nil; e = e.Next() {
				value = e.Value.(string) // <User_id>:<Posted>
				if value == args.Value {
					ss.listMapLock.Unlock()
					reply.Status = storagerpc.ItemExists
					return nil
				}
				if after(args.Value, value) { // A newer tribble
					ss.listMap[key].InsertBefore(args.Value, e)
					ss.listMapLock.Unlock()
					reply.Status = storagerpc.OK
					return nil
				}
			}
			//Oldest tribble should be put last of list
			ss.listMap[key].PushBack(args.Value)
			ss.listMapLock.Unlock()
			reply.Status = storagerpc.OK
			return nil
		} else { // No key list exist before
			ss.listMap[key] = list.New()
			ss.listMap[key].PushFront(args.Value)
			ss.listMapLock.Unlock()
			reply.Status = storagerpc.OK
			return nil
		}
	} else { // It's a request for subscriptions
		ss.listMapLock.Lock()
		if l, ok := ss.listMap[key]; ok { // There is already a list for that key
			var value string
			for e := l.Front(); e != nil; e = e.Next() {
				value = e.Value.(string)
				if value == args.Value {
					ss.listMapLock.Unlock()
					reply.Status = storagerpc.ItemExists
					return nil
				}
			}
			ss.listMap[key].PushFront(args.Value)
			ss.listMapLock.Unlock()
			reply.Status = storagerpc.OK
			return nil
		} else { // No key list exist before
			ss.listMap[key] = list.New()
			ss.listMap[key].PushFront(args.Value)
			ss.listMapLock.Unlock()
			reply.Status = storagerpc.OK
			return nil
		}
	}

}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	if !ss.validServer(StoreHash(key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.listMapLock.Lock()
	if l, ok := ss.listMap[key]; ok { // There is already a list for that key
		var value string
		for e := l.Front(); e != nil; e = e.Next() {
			value = e.Value.(string)
			if value == args.Value {
				l.Remove(e)
				ss.listMapLock.Unlock()
				reply.Status = storagerpc.OK
				return nil
			}
		}
		ss.listMapLock.Unlock()
		reply.Status = storagerpc.ItemNotFound
		return nil
	} else { // No key list exist
		ss.listMapLock.Unlock()
		reply.Status = storagerpc.ItemNotFound
		return nil
	}
}

func StoreHash(key string) uint32 {

	strs := strings.Split(key, ":")
	userID := strs[0]
	hasher := fnv.New32()
	hasher.Write([]byte(userID))
	return hasher.Sum32()
}

func (ss *storageServer) validServer(keyHash uint32) bool {
	var serverID uint32
	serverID = 1<<32 - 1
	ss.serverListLock.Lock()
	for e := ss.serverList.Front(); e != nil; e = e.Next() {
		node := e.Value.(storagerpc.Node)
		if node.NodeID >= keyHash && node.NodeID < serverID {
			serverID = node.NodeID
		}
	}
	if serverID == 1<<32-1 {
		serverID = ss.serverList.Front().Value.(storagerpc.Node).NodeID
	}
	if serverID == ss.serverID {
		ss.serverListLock.Unlock()
		return true
	} else {
		ss.serverListLock.Unlock()
		return false
	}

}

func after(tribbleIDOne, tribbleIDTwo string) bool {
	strOne := strings.Split(tribbleIDOne, ":")
	strTwo := strings.Split(tribbleIDOne, ":")
	timeOne, _ := time.Parse(common.Layout, strOne[1])
	timeTwo, _ := time.Parse(common.Layout, strTwo[1])
	if timeOne.After(timeTwo) {
		return true
	} else {
		return false
	}
}
