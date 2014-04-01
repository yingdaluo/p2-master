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

	elementMap     map[string]string
	elementMapLock *sync.Mutex
	listMap        map[string]*list.List
	listMapLock    *sync.Mutex

	EleaseMap     map[string]*list.List
	EleaseMapLock *sync.Mutex

	LleaseMap     map[string]*list.List
	LleaseMapLock *sync.Mutex
}

type leaseInfo struct {
	HostPort  string
	GrantTime time.Time
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
		listMap:              make(map[string]*list.List),
		listMapLock:          new(sync.Mutex),
		elementMap:           make(map[string]string),
		elementMapLock:       new(sync.Mutex),
		EleaseMap:            make(map[string]*list.List),
		EleaseMapLock:        new(sync.Mutex),
		LleaseMap:            make(map[string]*list.List),
		LleaseMapLock:        new(sync.Mutex),
	}
	isMaster := false
	if srv.masterServerHostPort == "" {
		isMaster = true
		// run RPC server
		srv.masterServerHostPort = "localhost:" + strconv.Itoa(port)
		rpc.RegisterName("StorageServer", srv)
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
		err = client.Call("StorageServer.RegisterServer", args, &reply)
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

	if !isMaster {
		// run RPC server
		srv.masterServerHostPort = "localhost:" + strconv.Itoa(port)
		rpc.RegisterName("StorageServer", srv)
		rpc.HandleHTTP()
		l, err := net.Listen("tcp", ":"+strconv.Itoa(srv.port))
		if err != nil {
			return nil, err
		}
		go http.Serve(l, nil)
	}
	return srv, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	node := args.ServerInfo
	found := false
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
		reply.Status = storagerpc.OK
		reply.Servers = slice
		return nil
	} else {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
		return nil
	}
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if ss.serverList.Len() < ss.numNodes {
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
		reply.Status = storagerpc.OK
		reply.Servers = slice
		return nil
	}
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if ss.serverList.Len() < ss.numNodes {
		reply.Status = storagerpc.NotReady
		reply.Value = ""
		return nil
	}

	key := args.Key
	if !ss.validServer(StoreHash(key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.elementMapLock.Lock()
	value, ok := ss.elementMap[key]
	if !ok {
		ss.elementMapLock.Unlock()
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
	ss.elementMapLock.Unlock()
	reply.Status = storagerpc.OK
	reply.Value = value

	needGrant := false
	if args.WantLease == true {
		ss.EleaseMapLock.Lock()
		_, ok := ss.EleaseMap[key]
		ss.EleaseMapLock.Unlock()

		if !ok {
			leaseinfo := new(leaseInfo)
			leaseinfo.GrantTime = time.Now()
			leaseinfo.HostPort = args.HostPort
			ss.EleaseMapLock.Lock()
			ss.EleaseMap[key] = list.New()
			ss.EleaseMap[key].PushFront(leaseinfo)
			ss.EleaseMapLock.Unlock()
			needGrant = true

		} else {
			ss.EleaseMapLock.Lock()
			for e := ss.EleaseMap[key].Front(); e != nil; e = e.Next() {
				leaseinfo := e.Value.(*leaseInfo)
				if leaseinfo.HostPort == args.HostPort && time.Since(leaseinfo.GrantTime).Seconds() > float64(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) {
					needGrant = true
					ss.EleaseMap[key].Remove(e)
					ss.EleaseMapLock.Unlock()
					break
				}
			}
			ss.EleaseMapLock.Unlock()
		}
		if needGrant {
			ss.EleaseMapLock.Lock()
			ss.EleaseMap[key].PushBack(&leaseInfo{args.HostPort, time.Now()})
			ss.EleaseMapLock.Unlock()
			lease := new(storagerpc.Lease)
			lease.Granted = true
			lease.ValidSeconds = storagerpc.LeaseSeconds
			reply.Lease = *lease
		} else {
			lease := new(storagerpc.Lease)
			lease.Granted = false
			lease.ValidSeconds = storagerpc.LeaseSeconds
			reply.Lease = *lease
		}
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if ss.serverList.Len() < ss.numNodes {
		reply.Status = storagerpc.NotReady
		reply.Value = nil
		return nil
	}
	reply.Status = storagerpc.OK

	key := args.Key
	if !ss.validServer(StoreHash(key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.listMapLock.Lock()
	l, ok := ss.listMap[key]
	ss.listMapLock.Unlock()
	if !ok {
		reply.Status = storagerpc.ItemNotFound
		return nil
	}

	slice := make([]string, l.Len())
	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		value := e.Value.(string)
		slice[i] = value
		i++
	}

	reply.Value = slice
	needGrant := false
	if args.WantLease == true {
		ss.LleaseMapLock.Lock()
		_, ok := ss.LleaseMap[key]
		ss.LleaseMapLock.Unlock()

		if !ok {
			leaseinfo := new(leaseInfo)
			leaseinfo.GrantTime = time.Now()
			leaseinfo.HostPort = args.HostPort
			ss.LleaseMapLock.Lock()
			ss.LleaseMap[key] = list.New()
			ss.LleaseMap[key].PushFront(leaseinfo)
			ss.LleaseMapLock.Unlock()
			needGrant = true

		} else {
			ss.LleaseMapLock.Lock()
			for e := ss.LleaseMap[key].Front(); e != nil; e = e.Next() {
				leaseinfo := e.Value.(*leaseInfo)
				if leaseinfo.HostPort == args.HostPort && time.Since(leaseinfo.GrantTime).Seconds() > float64(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) {
					needGrant = true
					ss.LleaseMap[key].Remove(e)
					ss.LleaseMapLock.Unlock()
					break
				}
			}
			ss.LleaseMapLock.Unlock()
		}
		if needGrant {
			ss.LleaseMapLock.Lock()
			ss.LleaseMap[key].PushBack(&leaseInfo{args.HostPort, time.Now()})
			ss.LleaseMapLock.Unlock()
			lease := new(storagerpc.Lease)
			lease.Granted = true
			lease.ValidSeconds = storagerpc.LeaseSeconds
			reply.Lease = *lease
		} else {
			lease := new(storagerpc.Lease)
			lease.Granted = false
			lease.ValidSeconds = storagerpc.LeaseSeconds
			reply.Lease = *lease
		}
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
        	reply.Status = storagerpc.ItemExists
        	ss.elementMapLock.Unlock()
        	return nil
    	}
    	ss.elementMapLock.Unlock()

	//Revoke all leases for this key
	ss.EleaseMapLock.Lock()
	l, ok := ss.EleaseMap[key]
	ss.EleaseMapLock.Unlock()
	if ok {
		for e := l.Front(); e != nil; e = e.Next() {
			leaseinfo := e.Value.(*leaseInfo)
			if time.Since(leaseinfo.GrantTime).Seconds() > float64(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) {
				continue
			}
			//Send revoke message and wait for reply.
			client, err := rpc.DialHTTP("tcp", leaseinfo.HostPort)
			if err != nil {
				continue
			}
			args := &storagerpc.RevokeLeaseArgs{key}
			var reply *storagerpc.RevokeLeaseReply
			if time.Since(leaseinfo.GrantTime).Seconds() <= float64(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) {
				timeoutChan := time.After(time.Duration(float64(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)) * time.Second)
				replyChan := make(chan int, 1)
				go func(replyChan chan int) {
					client.Call("LeaseCallbacks.RevokeLease", args, &reply)
					replyChan <- 1
				}(replyChan)
				select {
				case <-replyChan:
				case <-timeoutChan:
				}
			}
		}
		ss.EleaseMapLock.Lock()
		delete(ss.EleaseMap, key)
		ss.EleaseMapLock.Unlock()
	}

	ss.elementMapLock.Lock()
	ss.elementMap[key] = value
	ss.elementMapLock.Unlock()
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	if !ss.validServer(StoreHash(key)) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	//Revoke all leases for this key
	ss.revokeKey(key)

	suffix := strings.Split(key, ":")[1]
	if suffix == "tribbleList" {
		ss.listMapLock.Lock()
		l, ok := ss.listMap[key]
		ss.listMapLock.Unlock()
		if ok { // There is already a list for that key
			var value string
			for e := l.Front(); e != nil; e = e.Next() {
				value = e.Value.(string) // <User_id>:<Posted>
				if value == args.Value {
					reply.Status = storagerpc.ItemExists
					return nil
				}
				if after(args.Value, value) { // A newer tribble
					ss.listMapLock.Lock()
					ss.listMap[key].InsertBefore(args.Value, e)
					ss.listMapLock.Unlock()
					reply.Status = storagerpc.OK
					return nil
				}
			}
			//Oldest tribble should be put last of list
			ss.listMapLock.Lock()
			ss.listMap[key].PushBack(args.Value)
			ss.listMapLock.Unlock()
			reply.Status = storagerpc.OK
			return nil
		} else { // No key list exist before
			ss.listMapLock.Lock()
			ss.listMap[key] = list.New()
			ss.listMap[key].PushFront(args.Value)
			ss.listMapLock.Unlock()
			reply.Status = storagerpc.OK
			return nil
		}
	} else { // It's a request for subscriptions
		ss.listMapLock.Lock()
		l, ok := ss.listMap[key]
		ss.listMapLock.Unlock()
		if ok { // There is already a list for that key
			var value string
			for e := l.Front(); e != nil; e = e.Next() {
				value = e.Value.(string)
				if value == args.Value {
					reply.Status = storagerpc.ItemExists
					return nil
				}
			}
			ss.listMapLock.Lock()
			ss.listMap[key].PushFront(args.Value)
			ss.listMapLock.Unlock()
			reply.Status = storagerpc.OK
			return nil
		} else { // No key list exist before
			ss.listMapLock.Lock()
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
	l, ok := ss.listMap[key]
	ss.listMapLock.Unlock()
	if ok { // There is already a list for that key
		var value string
		for e := l.Front(); e != nil; e = e.Next() {
			value = e.Value.(string)
			if value == args.Value {
				ss.revokeKey(key)
				ss.listMapLock.Lock()
				l.Remove(e)
				ss.listMapLock.Unlock()
				reply.Status = storagerpc.OK
				return nil
			}
		}
		reply.Status = storagerpc.ItemNotFound
		return nil
	} else { // No key list exist
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
	var serverID, minID uint32
	serverID = 1<<32 - 1
	minID = 1<<32 - 1
	for e := ss.serverList.Front(); e != nil; e = e.Next() {
		node := e.Value.(storagerpc.Node)
		if node.NodeID >= keyHash && node.NodeID < serverID {

			serverID = node.NodeID
		}
		if minID > node.NodeID {
			minID = node.NodeID
		}
	}
	if serverID == 1<<32-1 {
		serverID = minID
	}
	if serverID == ss.serverID {
		return true
	} else {
		return false
	}

}

func (ss *storageServer) revokeKey(key string) {
	ss.LleaseMapLock.Lock()
	l, ok := ss.LleaseMap[key]
	ss.LleaseMapLock.Unlock()
	if ok {
		for e := l.Front(); e != nil; e = e.Next() {
			leaseinfo := e.Value.(*leaseInfo)
			if time.Since(leaseinfo.GrantTime).Seconds() > float64(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) {
				continue
			}
			//Send revoke message and wait for reply.
			client, err := rpc.DialHTTP("tcp", leaseinfo.HostPort)
			if err != nil {
				continue
			}
			args := &storagerpc.RevokeLeaseArgs{key}
			var reply *storagerpc.RevokeLeaseReply
			if time.Since(leaseinfo.GrantTime).Seconds() <= float64(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) {
				timeoutChan := time.After(time.Duration(float64(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds)) * time.Second)
				replyChan := make(chan int, 1)
				go func(replyChan chan int) {
					client.Call("LeaseCallbacks.RevokeLease", args, &reply)
					replyChan <- 1
				}(replyChan)
				select {
				case <-replyChan:
				case <-timeoutChan:
				}
			}
		}
		ss.LleaseMapLock.Lock()
		delete(ss.LleaseMap, key)
		ss.LleaseMapLock.Unlock()
	}

}

func after(tribbleIDOne, tribbleIDTwo string) bool {
	strOne := strings.Split(tribbleIDOne, ":")
	strTwo := strings.Split(tribbleIDTwo, ":")
	timeOne, _ := time.Parse(common.Layout, strOne[1])
	timeTwo, _ := time.Parse(common.Layout, strTwo[1])
	if timeOne.After(timeTwo) {
		return true
	} else {
		return false
	}
}
