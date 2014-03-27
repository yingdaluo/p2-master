package tribserver

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"net"
	"net/http"
	"net/rpc"
	"time"
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
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.libstore.Get(args.UserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	_, err = ts.libstore.Get(args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	ts.libstore.AppendToList(toSubscribeList(args.UserID), args.TargetUserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.libstore.Get(args.UserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	_, err = ts.libstore.Get(args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	ts.libstore.RemoveFromList(toSubscribeList(args.UserID), args.TargetUserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	if _, err := ts.libstore.Get(args.UserID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	subscriptionList, _ := ts.libstore.GetList(args.UserID)
	reply.Status = tribrpc.OK
	reply.UserIDs = subscriptionList
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	if _, err := ts.libstore.Get(args.UserID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	time := time.Now()
	tribble := new(tribrpc.Tribble)

	tribble.UserID = args.UserID
	tribble.Contents = args.Contents
	tribble.Posted = time

	value, _ := json.Marshal(tribble)
	newItem := string(value)
	ts.libstore.AppendToList(args.UserID, newItem)

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	if _, err := ts.libstore.Get(args.UserID); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	tribbleList, _ := ts.libstore.GetList(args.UserID)
	tribbles := make([]tribrpc.Tribble, TRIBBLE_LIMIT)

	i := 0
	for _, str := range tribbleList {
		tribble := new(tribrpc.Tribble)
		json.Unmarshal([]byte(str), tribble)
		tribbles[i] = *tribble
		i++
		if i > TRIBBLE_LIMIT {
			break
		}
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func toSubscribeList(userID string) string {
	return userID + ":subList"
}

func toTribbleList(userID string) string {
	return userID + ":tribbleList"
}
