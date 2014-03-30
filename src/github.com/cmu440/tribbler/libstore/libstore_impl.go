package libstore

import (
	"container/list"
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type libstore struct {
	mode                 LeaseMode
	masterServerHostPort string
	myHostPort           string
	serverList           *list.List
	serverListLock       *sync.Mutex

	connMap         map[string]*rpc.Client
	connMapLock     *sync.Mutex
	requestsMap     map[string]map[time.Time]int
	requestsMapLock *sync.Mutex

	listRequestsMap     map[string]map[time.Time]int
	listRequestsMapLock *sync.Mutex
	ECacheMap           map[string]*ECache
	ECacheMapLock       *sync.Mutex
	LCacheMap           map[string]*LCache
	LCacheMapLock       *sync.Mutex
}

type ECache struct {
	value       string
	timeGranted time.Time
	validSecond int
}

type LCache struct {
	value       []string
	timeGranted time.Time
	validSecond int
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	ls := &libstore{
		mode:                 mode,
		masterServerHostPort: masterServerHostPort,
		myHostPort:           myHostPort,
		serverList:           list.New(),
		serverListLock:       new(sync.Mutex),
		connMap:              make(map[string]*rpc.Client),
		connMapLock:          new(sync.Mutex),

		requestsMap:         make(map[string]map[time.Time]int),
		requestsMapLock:     new(sync.Mutex),
		listRequestsMap:     make(map[string]map[time.Time]int),
		listRequestsMapLock: new(sync.Mutex),
		ECacheMap:           make(map[string]*ECache),
		ECacheMapLock:       new(sync.Mutex),
		LCacheMap:           make(map[string]*LCache),
		LCacheMapLock:       new(sync.Mutex),
	}

	//register to master
	i := 0
	for ; i < 5; i++ {
		client, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}
		args := &storagerpc.GetServersArgs{}
		var reply *storagerpc.GetServersReply
		err = client.Call("StorageServer.GetServers", args, &reply)
		if err != nil {
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			ls.serverListLock.Lock()
			for _, node := range reply.Servers {
				ls.serverList.PushBack(node)
			}
			ls.serverListLock.Unlock()
			ls.connMapLock.Lock()
			ls.connMap[masterServerHostPort] = client
			ls.connMapLock.Unlock()
			break
		} else {
			time.Sleep(time.Duration(1) * time.Second)
		}
	}

	if i >= 5 {
		return nil, errors.New("Fail to connect storage server")
	}
	if mode != Never {
		rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
		go ls.cleaningECache()
		go ls.cleaningLCache()
	}
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	client, err := ls.getRPCServer(key)
	if err != nil {
		return "", err
	}

	// Check if something in the cache
	ls.ECacheMapLock.Lock()
	cacheResult, ok := ls.ECacheMap[key]
	ls.ECacheMapLock.Unlock()
	if ok {
		if time.Since(cacheResult.timeGranted).Seconds() > float64(cacheResult.validSecond) {
			ls.ECacheMapLock.Lock()
			delete(ls.ECacheMap, key)
			ls.ECacheMapLock.Unlock()
		} else {
			return cacheResult.value, nil
		}

	}

	wantlease := false
	if ls.mode == Never {
		wantlease = false
	} else if ls.mode == Always {
		wantlease = true
	} else {
		// Put this request into requestsMap
		ls.requestsMapLock.Lock()
		_, ok := ls.requestsMap[key]
		if !ok { //No such request before
			ls.requestsMap[key] = make(map[time.Time]int)
		}
		ls.requestsMap[key][time.Now()] = 1
		// Check if threshold has been reached.
		i := 0
		for grantTime, _ := range ls.requestsMap[key] {
			if time.Since(grantTime).Seconds() > float64(storagerpc.QueryCacheSeconds) {
				delete(ls.requestsMap[key], grantTime)
			} else {
				i++
				if i > storagerpc.QueryCacheThresh {
					wantlease = true
					break
				}
			}
		}
		ls.requestsMapLock.Unlock()
	}

	args := &storagerpc.GetArgs{key, wantlease, ls.myHostPort}
	var reply *storagerpc.GetReply
	err = client.Call("StorageServer.Get", args, &reply)
	if err != nil {
		return "", err
	}
	if reply.Status == storagerpc.OK {
		result := reply.Value
		if reply.Lease.Granted == true {
			ls.ECacheMapLock.Lock()
			ls.ECacheMap[key] = new(ECache)
			ls.ECacheMap[key].value = reply.Value
			ls.ECacheMap[key].timeGranted = time.Now()
			ls.ECacheMap[key].validSecond = reply.Lease.ValidSeconds
			ls.ECacheMapLock.Unlock()
		}
		return result, nil
	} else if reply.Status == storagerpc.WrongServer {
		return "", errors.New("Wrong server")
	} else if reply.Status == storagerpc.ItemNotFound {
		return "", errors.New("Item Not Found")
	} else {
		return "", errors.New("Fail to get " + key + "from storageserver")
	}
}

func (ls *libstore) Put(key, value string) error {
	client, err := ls.getRPCServer(key)
	if err != nil {
		return err
	}
	args := &storagerpc.PutArgs{key, value}
	var reply *storagerpc.PutReply
	err = client.Call("StorageServer.Put", args, &reply)
	if err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else if reply.Status == storagerpc.WrongServer {
		return errors.New("Wrong server")
	} else if reply.Status == storagerpc.ItemExists {
		return errors.New("Item already exists")
	} else {
		return errors.New("Fail to put " + key + "into storageserver")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	client, err := ls.getRPCServer(key)
	if err != nil {
		return nil, err
	}

	// Check if something in the cache
	ls.LCacheMapLock.Lock()
	cacheResult, ok := ls.LCacheMap[key]
	ls.LCacheMapLock.Unlock()
	if ok {
		if time.Since(cacheResult.timeGranted).Seconds() > float64(cacheResult.validSecond) {
			ls.LCacheMapLock.Lock()
			delete(ls.LCacheMap, key)
			ls.LCacheMapLock.Unlock()
		} else {
			return cacheResult.value, nil
		}
	}

	wantlease := false
	if ls.mode == Never {
		wantlease = false
	} else if ls.mode == Always {
		wantlease = true
	} else {
		// Put this request into listRequestsMap
		ls.listRequestsMapLock.Lock()
		_, ok := ls.listRequestsMap[key]
		if !ok { //No such request before
			ls.listRequestsMap[key] = make(map[time.Time]int)
		}
		ls.listRequestsMap[key][time.Now()] = 1
		// Check if threshold has been reached.
		i := 0
		for grantTime, _ := range ls.listRequestsMap[key] {
			if time.Since(grantTime).Seconds() > float64(storagerpc.QueryCacheSeconds) {
				delete(ls.listRequestsMap[key], grantTime)
			} else {
				i++
				if i > storagerpc.QueryCacheThresh {
					wantlease = true
					break
				}
			}
		}
		ls.listRequestsMapLock.Unlock()
	}

	args := &storagerpc.GetArgs{key, wantlease, ls.myHostPort}
	var reply *storagerpc.GetListReply
	err = client.Call("StorageServer.GetList", args, &reply)
	if err != nil {
		return nil, err
	}
	if reply.Status == storagerpc.OK {
		result := reply.Value
		if reply.Lease.Granted == true {
			ls.LCacheMapLock.Lock()
			ls.LCacheMap[key] = new(LCache)
			ls.LCacheMap[key].value = reply.Value
			ls.LCacheMap[key].timeGranted = time.Now()
			ls.LCacheMap[key].validSecond = reply.Lease.ValidSeconds
			ls.LCacheMapLock.Unlock()
		}
		return result, nil
	} else if reply.Status == storagerpc.WrongServer {
		return nil, errors.New("Wrong server")
	} else if reply.Status == storagerpc.ItemNotFound {
		return nil, errors.New("Item Not Found")
	} else {
		return nil, errors.New("Fail to get " + key + " list from storageserver")
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	client, err := ls.getRPCServer(key)
	if err != nil {
		return err
	}
	args := &storagerpc.PutArgs{key, removeItem}
	var reply *storagerpc.PutReply
	err = client.Call("StorageServer.RemoveFromList", args, &reply)
	if err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else if reply.Status == storagerpc.WrongServer {
		return errors.New("Wrong server")
	} else if reply.Status == storagerpc.ItemNotFound {
		return errors.New("Item Not Found")
	} else {
		return errors.New("Fail to remove " + key + "from storageserver")
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	client, err := ls.getRPCServer(key)
	if err != nil {
		return err
	}
	args := &storagerpc.PutArgs{key, newItem}
	var reply *storagerpc.PutReply
	err = client.Call("StorageServer.AppendToList", args, &reply)
	if err != nil {
		return err
	}
	if reply.Status == storagerpc.OK {
		return nil
	} else if reply.Status == storagerpc.WrongServer {
		return errors.New("Wrong server")
	} else if reply.Status == storagerpc.ItemExists {
		return errors.New("Item already exists")
	} else {
		return errors.New("Fail to append " + key + ":" + newItem + "into storageserver")
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	for {
		ls.LCacheMapLock.Lock()
		if _, ok := ls.LCacheMap[args.Key]; ok {
			delete(ls.LCacheMap, args.Key)
			reply.Status = storagerpc.OK
			ls.LCacheMapLock.Unlock()
			return nil
		}
		ls.LCacheMapLock.Unlock()
		ls.ECacheMapLock.Lock()
		if _, ok := ls.ECacheMap[args.Key]; ok {
			delete(ls.ECacheMap, args.Key)
			reply.Status = storagerpc.OK
			ls.ECacheMapLock.Unlock()
			return nil
		}
		ls.ECacheMapLock.Unlock()
		reply.Status = storagerpc.OK
		return nil
	}
}

func (ls *libstore) cleaningLCache() {
	for {
		ls.LCacheMapLock.Lock()
		cacheMap := ls.LCacheMap
		ls.LCacheMapLock.Unlock()

		for key, cache := range cacheMap {
			if time.Since(cache.timeGranted).Seconds() > float64(storagerpc.QueryCacheSeconds) {
				ls.LCacheMapLock.Lock()
				delete(ls.LCacheMap, key)
				ls.LCacheMapLock.Unlock()
			}
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
}

func (ls *libstore) cleaningECache() {
	for {
		ls.ECacheMapLock.Lock()
		cacheMap := ls.ECacheMap
		ls.ECacheMapLock.Unlock()

		for key, cache := range cacheMap {
			if time.Since(cache.timeGranted).Seconds() > float64(storagerpc.QueryCacheSeconds) {
				ls.ECacheMapLock.Lock()
				delete(ls.ECacheMap, key)
				ls.ECacheMapLock.Unlock()
			}
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
}

func (ls *libstore) getRPCServer(key string) (*rpc.Client, error) {
	strs := strings.Split(key, ":")
	userID := strs[0]
	keyHash := StoreHash(userID)
	serverHostPort := ""
	var serverID, minID uint32
	serverID = 1<<32 - 1
	minID = 1<<32 - 1
	minHostPort := ""
	ls.serverListLock.Lock()
	for e := ls.serverList.Front(); e != nil; e = e.Next() {
		node := e.Value.(storagerpc.Node)
		if node.NodeID >= keyHash && node.NodeID < serverID {
			serverID = node.NodeID
			serverHostPort = node.HostPort
		}
		if minID > node.NodeID {
			minID = node.NodeID
			minHostPort = node.HostPort
		}
	}
	if serverID == 1<<32-1 {
		serverHostPort = minHostPort
	}
	ls.serverListLock.Unlock()

	ls.connMapLock.Lock()
	if client, ok := ls.connMap[serverHostPort]; ok {
		ls.connMapLock.Unlock()
		return client, nil
	} else {
		client, err := rpc.DialHTTP("tcp", serverHostPort)
		if err != nil {
			ls.connMapLock.Unlock()
			return nil, err
		}
		ls.connMap[serverHostPort] = client
		return client, nil
	}

}
