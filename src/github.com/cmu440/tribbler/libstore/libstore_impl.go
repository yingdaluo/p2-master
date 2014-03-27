package libstore

import (
	"container/list"
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sync"
	"time"
)
// written by wayman tan
type libstore struct {
	mode                 LeaseMode
	masterServerHostPort string
	myHostPort           string
	serverList           *list.List
	serverListLock       *sync.Mutex

	connMap     map[string]*rpc.Client
	connMapLock *sync.Mutex
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
		connMap:              make(map[string]*rpc.Client),
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
		err = client.Call("storageServer.GetServers", args, &reply)
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
	}

	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	client, err := ls.getRPCServer(key)
	if err != nil {
		return "", err
	}
	wantlease := false
	if ls.mode == Never {
		wantlease = false
	} else if ls.mode == Always {
		wantlease = true
	} else {
		//TODO: Judge whether wantlease is true or false
	}

	args := &storagerpc.GetArgs{key, wantlease, ls.myHostPort}
	var reply *storagerpc.GetReply
	err = client.Call("storageServer.Get", args, &reply)
	if err != nil {
		return "", err
	}
	if reply.Status == storagerpc.OK {
		result := reply.Value
		if wantlease == true {
			//TODO: Add reply.Lease into leaseMap?
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
	err = client.Call("storageServer.Put", args, &reply)
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
}

func (ls *libstore) GetList(key string) ([]string, error) {
	client, err := ls.getRPCServer(key)
	if err != nil {
		return nil, err
	}
	wantlease := false
	if ls.mode == Never {
		wantlease = false
	} else if ls.mode == Always {
		wantlease = true
	} else {
		//TODO: Judge whether wantlease is true or false
	}

	args := &storagerpc.GetArgs{key, wantlease, ls.myHostPort}
	var reply *storagerpc.GetListReply
	err = client.Call("storageServer.GetList", args, &reply)
	if err != nil {
		return nil, err
	}
	if reply.Status == storagerpc.OK {
		result := reply.Value
		if wantlease == true {
			//TODO: Add reply.Lease into leaseMap?
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
	err = client.Call("storageServer.RemoveFromList", args, &reply)
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
	err = client.Call("storageServer.AppendToList", args, &reply)
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
	return errors.New("not implemented")
}

func (ls *libstore) getRPCServer(key string) (*rpc.Client, error) {
	keyHash := StoreHash(key)

	var serverHostPort string
	var serverID uint32
	serverID = 1<<32 - 1
	ls.serverListLock.Lock()
	for e := ls.serverList.Front(); e != nil; e = e.Next() {
		node := e.Value.(storagerpc.Node)
		if node.NodeID >= keyHash && node.NodeID < serverID {
			serverID = node.NodeID
			serverHostPort = node.HostPort
		}
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
