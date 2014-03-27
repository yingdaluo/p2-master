package storageserver

import (
	"container/list"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"hash/fnv"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
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
			fmt.Println(err)
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
		args := &storagerpc.RegisterArgs{*&storagerpc.Node{"localhost:" + string(srv.port), srv.serverID}}
		var reply *storagerpc.RegisterReply
		err = client.Call("storageServer.RegisterServer", args, &reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			for _, node := range reply.Servers {
				srv.serverList.PushBack(node)
			}
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
	if ss.serverID < StoreHash(key) {
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
	if ss.serverID < StoreHash(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.listMapLock.Lock()
	list, ok := ss.listMap[key]
	if !ok {
		ss.elementMapLock.Unlock()
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
	if ss.serverID < StoreHash(key) {
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
	if ss.serverID < StoreHash(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.elementMapLock.Lock()
	if l, ok := ss.listMap[key]; ok { // There is already a list for that key
		var value string
		for e := l.Front(); e != nil; e = e.Next() {
			value = e.Value.(string)
			if value == args.Value {
				ss.elementMapLock.Unlock()
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
		ss.listMap[key].PushFront(args.Value)
		ss.elementMapLock.Unlock()
		reply.Status = storagerpc.OK
		return nil
	} else { // No key list exist before
		ss.listMap[key] = list.New()
		ss.listMap[key].PushFront(args.Value)
		ss.elementMapLock.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	if ss.serverID < StoreHash(key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.elementMapLock.Lock()
	if l, ok := ss.listMap[key]; ok { // There is already a list for that key
		var value string
		for e := l.Front(); e != nil; e = e.Next() {
			value = e.Value.(string)
			if value == args.Value {
				l.Remove(e)
				ss.elementMapLock.Unlock()
				reply.Status = storagerpc.OK
				return nil
			}
		}
		ss.elementMapLock.Unlock()
		reply.Status = storagerpc.ItemNotFound
		return nil
	} else { // No key list exist
		ss.elementMapLock.Unlock()
		reply.Status = storagerpc.ItemNotFound
		return nil
	}
}

func StoreHash(key string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return hasher.Sum32()
}
