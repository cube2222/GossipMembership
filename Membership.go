package GossipMembership

import (
	"errors"
	"fmt"
	"github.com/cube2222/GossipMembership/gossip"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	"github.com/gorilla/mux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"
)

type cluster struct {
	listenAddress           string
	httpListenAddress       string
	clusterAddress          string
	pingFrequency           time.Duration
	nodesToPing             int
	nodeTimeout             time.Duration
	membersToSend           int
	members                 map[string]*gossip.Member
	memberListMutex         sync.RWMutex
	membersLastUpdated      gossip.MemberList
	membersLastUpdatedMutex sync.RWMutex
}

// Creates a new cluster with configuration.
// ListenAddress is required. It describes on what address gossiping will take place.
func NewCluster(ListenAddress string) cluster {
	newCluster := cluster{}.WithListenAddress(ListenAddress)
	newCluster.members = make(map[string]*gossip.Member)
	return newCluster
}

// You can change the listen address before starting the cluster.
// For whatever reason you'd like to do that.
func (c cluster) WithListenAddress(listenAddress string) cluster {
	c.listenAddress = listenAddress
	return c
}

// This is the address of the cluster we want to join.
// If not provided, we'll start a new cluster.
func (c cluster) WithClusterAddress(clusterAddress string) cluster {
	c.clusterAddress = clusterAddress
	return c
}

// The frequency at which we ping other nodes with our member list.
// This is also our heartbeat frequency.
func (c cluster) WithPingFrequency(freq time.Duration) cluster {
	c.pingFrequency = freq
	return c
}

// How many nodes to ping whenever we do a ping.
func (c cluster) WithNodesToPing(amount int) cluster {
	c.nodesToPing = amount
	return c
}

// If we haven't heard from a node for <timeout>. Mark it dead.
func (c cluster) WithNodeTimeout(timeout time.Duration) cluster {
	c.nodeTimeout = timeout
	return c
}

// The amount of members from our member list to send on each ping.
func (c cluster) WithMembersToSend(amount int) cluster {
	c.membersToSend = amount
	return c
}

// The listen address we can get debugging info from.
// Like the current list of nodes.
// Current list of nodes available at: <addr>/memberList
func (c cluster) WithHttpListenAddress(addr string) cluster {
	c.httpListenAddress = addr
	return c
}

// Start the cluster.
// This time we return a pointer to the actual running cluster.
func (c cluster) Start() (*cluster, error) {
	if c.membersToSend < 1 {
		c.membersToSend = 4
	}
	if c.nodesToPing == 0 {
		c.nodesToPing = 3
	}
	if c.pingFrequency == time.Second*0 {
		c.pingFrequency = time.Second * 1
	}
	if c.nodeTimeout == time.Second*0 {
		c.nodeTimeout = time.Second * 20
	}
	if c.listenAddress == "" {
		return &c, errors.New("Listen address must be provided.")
	}

	c.membersLastUpdated.List = make([]*gossip.Member, 0, c.membersToSend)

	c.addSelfToLocalMemberList()

	c.membersLastUpdated.List = append(c.membersLastUpdated.List, c.members[c.listenAddress])

	if c.clusterAddress == "" {
		return &c, c.startCluster()
	} else {
		return &c, c.connectToCluster()
	}
}

// Pretty self-descriptive, eh?
func (c *cluster) addSelfToLocalMemberList() {
	me := gossip.Member{
		Address:   c.listenAddress,
		Heartbeat: 1,
		Timestamp: time.Now().UnixNano(),
		Alive:     true,
	}

	c.members[me.Address] = &me
}

func (c *cluster) putIntoMembersLastUpdated(member *gossip.Member) {
	c.removeFromMembersLastUpdated(member.Address)
	c.membersLastUpdatedMutex.Lock()
	if len(c.membersLastUpdated.List) < c.membersToSend {
		c.membersLastUpdated.List = append(append(c.membersLastUpdated.List[0:1], member), c.membersLastUpdated.List[1:len(c.membersLastUpdated.List)]...)
	} else {
		c.membersLastUpdated.List = append(append(c.membersLastUpdated.List[0:1], member), c.membersLastUpdated.List[1:len(c.membersLastUpdated.List)-1]...)
	}
	c.membersLastUpdatedMutex.Unlock()
}

func (c *cluster) removeFromMembersLastUpdated(address string) {
	c.membersLastUpdatedMutex.Lock()
	defer c.membersLastUpdatedMutex.Unlock()
	for index, item := range c.membersLastUpdated.List {
		if item.Address == address {
			c.membersLastUpdated.List = append(c.membersLastUpdated.List[0:index], c.membersLastUpdated.List[index+1:len(c.membersLastUpdated.List)]...)
			return
		}
	}
}

// Connect to a remote cluster at the cluster address provided.
// We send them info about ourselves, and get back the current
// member list.
func (c *cluster) connectToCluster() error {
	log.Printf("Connecting to cluster at %s \n", c.clusterAddress+"/join")

	c.memberListMutex.RLock()
	conn, err := grpc.Dial(c.clusterAddress, grpc.WithInsecure(), grpc.WithTimeout(4*time.Second))
	if err != nil {
		return err
	}
	client := gossip.NewNodeClient(conn)
	clusterMemberList, err := client.Join(context.Background(), c.members[c.listenAddress])
	if err != nil {
		conn.Close()
		return err
	}
	conn.Close()
	c.memberListMutex.RUnlock()

	c.memberListMutex.Lock()
	for _, item := range clusterMemberList.List {
		if item.Address != c.listenAddress {
			c.members[item.Address] = item
			c.members[item.Address].Timestamp = time.Now().UnixNano()
			log.Printf("New node added when joining: %s \n", item.Address)
			c.putIntoMembersLastUpdated(item)
		}
	}
	c.memberListMutex.Unlock()

	return c.startNode()
}

func (c *cluster) startCluster() error {
	log.Printf("Starting cluster at %s \n", c.listenAddress)
	return c.startNode()
}

func (c *cluster) startNode() error {
	if c.httpListenAddress != "" {
		if err := c.setupHttpHandler(); err != nil {
			return err
		}
	}

	if err := c.setupListener(); err != nil {
		return err
	}

	c.setupPinger()

	return nil
}

func (c *cluster) setupHttpHandler() error {
	m := mux.NewRouter()
	m.HandleFunc("/listMembers", func(w http.ResponseWriter, r *http.Request) {
		c.memberListMutex.RLock()
		for _, item := range c.members {
			fmt.Fprintf(w, "Address: %s Heartbeat: %v Timestamp: %v Alive: %t\n", item.Address, item.Heartbeat, item.Timestamp, item.Alive)
		}
		c.memberListMutex.RUnlock()
	})

	errorChannel := make(chan error)
	errorTimeout := time.After(time.Second)

	go func() {
		errorChannel <- http.ListenAndServe(c.httpListenAddress, m)
	}()

	select {
	case err := <-errorChannel:
		return err
	case <-errorTimeout:
	}
	return nil
}

func (c *cluster) setupListener() error {
	lis, err := net.Listen("tcp", c.listenAddress)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	gossip.RegisterNodeServer(s, c)

	errorChannel := make(chan error)
	errorTimeout := time.After(time.Second)

	go func() {
		errorChannel <- s.Serve(lis)
	}()

	select {
	case err := <-errorChannel:
		return err
	case <-errorTimeout:
	}
	return nil
}

func (c *cluster) setupPinger() {
	go func(clusterToTick *cluster) {
		tick := time.Tick(c.pingFrequency)
		for range tick {
			clusterToTick.handleDead()
			clusterToTick.heartbeat()
			clusterToTick.ping()
		}
	}(c)
}

func (c *cluster) handleDead() {
	c.memberListMutex.Lock()
	nodeToDelete := make([]string, 0)
	for _, item := range c.members {
		if time.Now().Sub(time.Unix(0, item.Timestamp)) > c.nodeTimeout {
			if item.Alive == true {
				item.Alive = false
				item.Timestamp = time.Now().UnixNano()
				log.Printf("Node marked dead by timeout: %s \n", item.Address)
				c.putIntoMembersLastUpdated(item)
			} else {
				nodeToDelete = append(nodeToDelete, item.Address) // Better not delete in place while for ranging.
			}
		}
	}
	for _, addr := range nodeToDelete {
		delete(c.members, addr) // Delete from members to send list.
		c.removeFromMembersLastUpdated(addr)
	}
	c.memberListMutex.Unlock()
}

func (c *cluster) heartbeat() {
	// Update this node.
	c.memberListMutex.Lock()
	c.members[c.listenAddress].Heartbeat += 1
	c.members[c.listenAddress].Timestamp = time.Now().UnixNano()
	c.memberListMutex.Unlock()
}

func (c *cluster) ping() {

	// Get a list of pingable candidates.
	c.memberListMutex.RLock()
	pingCandidates := make([]string, 0)
	for _, item := range c.members {
		if item.Address != c.listenAddress && item.Alive == true {
			pingCandidates = append(pingCandidates, item.Address)
		}
	}
	c.memberListMutex.RUnlock()

	// Get node amount to ping.
	var numToPing int

	if c.nodesToPing > len(pingCandidates) {
		numToPing = len(pingCandidates)
	} else {
		numToPing = c.nodesToPing
	}

	// Get the actual nodes to ping
	addressesToPing := make([]string, numToPing, numToPing)
	randomNum := rand.Int()
	for i := 0; i < len(addressesToPing); i++ {
		addressesToPing[i] = pingCandidates[(randomNum+i)%len(pingCandidates)]
	}

	// Send pings in parallel
	wg := sync.WaitGroup{}
	c.membersLastUpdatedMutex.RLock()
	for _, str := range addressesToPing {
		go func(addr string) {
			wg.Add(1)
			conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(time.Second*1))
			if err != nil {
				return
			}
			client := gossip.NewNodeClient(conn)
			_, err = client.Ping(context.Background(), &c.membersLastUpdated)
			conn.Close()
			wg.Done()
		}(str)
	}
	wg.Wait()
	c.membersLastUpdatedMutex.RUnlock()
}

// Get all the cluster members as a list of addresses as strings. Including us.
func (c *cluster) GetClusterMembers() []string {
	curMembers := make([]string, 0, 10)
	c.memberListMutex.RLock()
	for _, item := range c.members {
		if item.Alive == true {
			curMembers = append(curMembers, item.Address)
		}
	}
	c.memberListMutex.RUnlock()
	return curMembers
}

// Called by other nodes each time they ping this node.
func (c *cluster) Ping(ctx context.Context, remoteList *gossip.MemberList) (*google_protobuf.Empty, error) {
	c.memberListMutex.Lock()
	for _, item := range remoteList.List {
		localItem, ok := c.members[item.Address] // Keep in mind, localItem is a pointer.
		if ok == true {
			if item.Heartbeat > c.members[item.Address].Heartbeat {
				// If remote heartbeat is higher, it's surely more up to date than ours.
				localItem.Alive = item.Alive
				localItem.Heartbeat = item.Heartbeat
				localItem.Timestamp = time.Now().UnixNano()
				if localItem.Alive == false {
					log.Printf("Node marked dead by gossip: %s \n", item.Address)
				}
				c.putIntoMembersLastUpdated(localItem)

			} else if item.Heartbeat == c.members[item.Address].Heartbeat && localItem.Alive == true && item.Alive == false {
				// If remote heartbeat is the same, and remotely this node is dead, it means it's dead but we haven't noticed it yet. Kill it.
				localItem.Alive = false
				localItem.Timestamp = time.Now().UnixNano()
				log.Printf("Node marked dead by gossip: %s \n", item.Address)
				c.putIntoMembersLastUpdated(localItem)
			}
		} else {
			// If we do not have a node saved, put it into our list, provided it is alive.
			if item.Alive == true {
				c.members[item.Address] = item
				c.members[item.Address].Timestamp = time.Now().UnixNano()
				log.Printf("New node added by gossip: %s \n", item.Address)
				c.putIntoMembersLastUpdated(c.members[item.Address])
			}
		}
	}
	c.memberListMutex.Unlock()

	return &google_protobuf.Empty{}, nil
}

// Called by new nodes to join the cluster on this nodes behalf.
func (c *cluster) Join(ctx context.Context, nodeToJoin *gossip.Member) (*gossip.MemberList, error) {
	c.memberListMutex.Lock()
	c.members[nodeToJoin.Address] = nodeToJoin
	c.members[nodeToJoin.Address].Timestamp = time.Now().UnixNano()
	c.memberListMutex.Unlock()
	log.Printf("New node added by join: %s \n", nodeToJoin.Address)

	c.memberListMutex.RLock()
	response := gossip.MemberList{}
	response.List = make([]*gossip.Member, 0, len(c.members))
	for _, item := range c.members {
		response.List = append(response.List, &gossip.Member{item.Address, item.Heartbeat, item.Timestamp, item.Alive})
	}
	c.memberListMutex.RUnlock()

	return &response, nil
}

// Called by other nodes to notify about leaving the cluster.
func (c *cluster) NotifyLeave(ctx context.Context, nodeToRemove *gossip.Member) (*google_protobuf.Empty, error) {
	c.memberListMutex.Lock()
	c.members[nodeToRemove.Address] = nodeToRemove // He's no more marked as alive, so that'l do it.
	c.members[nodeToRemove.Address].Timestamp = time.Now().UnixNano()
	c.memberListMutex.Unlock()
	c.removeFromMembersLastUpdated(nodeToRemove.Address)
	log.Printf("Node marked dead by leave: %s \n", nodeToRemove.Address)

	return &google_protobuf.Empty{}, nil
}
