package tribserver

import (
	"encoding/json"
	// "errors"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/common"
	"net"
	"net/http"
	"net/rpc"
	"time"
	"strings"
)

const (
	TRIBBLE_LIMIT = 100
)

type tribServer struct {
	myHostPort string
	libstore   libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, err
	}
	ts := &tribServer{
		myHostPort: myHostPort,
		libstore:   libstore,
	}

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	if err != nil {
		return nil, err
	}
	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return ts, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	err := ts.libstore.Put(args.UserID, args.UserID)
	if err != nil {
		if err.Error() == libstore.ItemExists {
			reply.Status = tribrpc.Exists
			return nil
		} 
		return err	
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.libstore.Get(args.UserID)
	if err != nil {
		if err.Error() == libstore.ItemNotFound {
			reply.Status = tribrpc.NoSuchUser
			return nil
		} 
		return err	
	}

	_, err = ts.libstore.Get(args.TargetUserID)
	if err != nil {
		if err.Error() == libstore.ItemNotFound {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		} 
		return err
	}

	err = ts.libstore.AppendToList(toSubscribeList(args.UserID), args.TargetUserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.libstore.Get(args.UserID)
	if err != nil {
		if err.Error() == libstore.ItemNotFound {
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
		return err	
	}

	_, err = ts.libstore.Get(args.TargetUserID)
	if err != nil {
		if err.Error() == libstore.ItemNotFound {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		}
		return err		
	}

	err = ts.libstore.RemoveFromList(toSubscribeList(args.UserID), args.TargetUserID)
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	_, err := ts.libstore.Get(args.UserID) 
	if err != nil {
		if err.Error() == libstore.ItemNotFound {
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
		return err		
	}
	subscriptionList, err := ts.libstore.GetList(toSubscribeList(args.UserID))
	if err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	reply.UserIDs = subscriptionList
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	_, err := ts.libstore.Get(args.UserID)
	if err != nil {
		if err.Error() == libstore.ItemNotFound {
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
		return err		
	}
	time := time.Now()
	tribble := new(tribrpc.Tribble)

	tribble.UserID = args.UserID
	tribble.Contents = args.Contents
	tribble.Posted = time

	value, _ := json.Marshal(tribble)
	newItem := string(value)

	tribbleID := args.UserID + ":" + time.Format(common.Layout)

	// put the new tribble first
	err = ts.libstore.Put(tribbleID, newItem)
	if err != nil {
		return err
	}

	// update the user's tribble list
	err = ts.libstore.AppendToList(toTribbleList(args.UserID), tribbleID)
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_, err := ts.libstore.Get(args.UserID)
	if err != nil {
		if err.Error() == libstore.ItemNotFound {
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
		return err
	}

	// Get the tribble list of the user
	tribbleList, err := ts.libstore.GetList(toTribbleList(args.UserID))
	if err != nil {
		return err
	}
	
	tribbles := make([]tribrpc.Tribble, TRIBBLE_LIMIT)

	counter := 0
	for i := 0; i < TRIBBLE_LIMIT && i < len(tribbleList); i++ {
		tribbleByteStr, err := ts.libstore.Get(tribbleList[i])
		if err != nil {
			return err
		}
		tribble := new(tribrpc.Tribble)
		json.Unmarshal([]byte(tribbleByteStr), tribble)
		tribbles[i] = *tribble
		counter++
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles[:counter]
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_, err := ts.libstore.Get(args.UserID)
	if err != nil {
		if err.Error() == libstore.ItemNotFound {
			reply.Status = tribrpc.NoSuchUser
			return nil
		}
		return err
	}

	// Get the subscription list of the user
	subList, err := ts.libstore.GetList(toSubscribeList(args.UserID))
	if err != nil {
		return err
	}

	subLen := len(subList)
	subTribbleList := make([][]string, subLen)

	// Get tribble list from all users
	var tribbleList []string
	for i := 0; i < subLen; i++ {
		tribbleList, err = ts.libstore.GetList(toTribbleList(subList[i]))
		if err != nil {
			// continue if there is no tribble list found for the subscribed user
			if err.Error() == libstore.ItemNotFound {
				continue
			}
			return err
		}		
		subTribbleList[i] = tribbleList		
	}

	// Get the latest 100 tribbles
	tribbles := make([]tribrpc.Tribble, TRIBBLE_LIMIT)

	var latestTribbleID string
	var idx int
	counter := 0
	for i := 0; i < TRIBBLE_LIMIT; i++ {
		latestTribbleID = ""
		idx = 0
		for j := 0; j < subLen; j++ {
			if subTribbleList[j] == nil {
				continue
			}

			if len(subTribbleList[j]) == 0 {
				continue
			}

			if latestTribbleID == "" {
				latestTribbleID = subTribbleList[j][0]
				idx = j
			} else {
				if after(subTribbleList[j][0], latestTribbleID) {
					latestTribbleID = subTribbleList[j][0]
					idx = j
				}
			}
		}

		// there is no more tribble id in the subscription
		if latestTribbleID == "" {
			break
		}

		// get the latest tribble
		tribbleByteStr, err := ts.libstore.Get(latestTribbleID)
		if err != nil {
			return err
		}
		tribble := new(tribrpc.Tribble)
		json.Unmarshal([]byte(tribbleByteStr), tribble)
		tribbles[i] = *tribble

		// take the latest tribble id (the first element) out from the corresponding sub tribble list
		subTribbleList[idx] = subTribbleList[idx][1:]

		counter++
	}

	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles[:counter]
	return nil
}

func toSubscribeList(userID string) string {
	return userID + ":subList"
}

func toTribbleList(userID string) string {
	return userID + ":tribbleList"
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
