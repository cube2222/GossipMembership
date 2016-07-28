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
	listenAddress     string
	httpListenAddress string
	clusterAddress    string
	pingFrequency     time.Duration
	nodesToPing       int
	nodeTimeout       time.Duration
	members           gossip.MemberList
	memberListMutex   sync.RWMutex
}

func NewCluster(ListenAddress string) cluster {
	newCluster := cluster{}.WithListenAddress(ListenAddress)
	newCluster.members.List = make(map[string]*gossip.Member)
	return newCluster
}

func (c cluster) WithListenAddress(listenAddress string) cluster {
	c.listenAddress = listenAddress
	return c
}

func (c cluster) WithClusterAddress(clusterAddress string) cluster {
	c.clusterAddress = clusterAddress
	return c
}

func (c cluster) WithPingFrequency(freq time.Duration) cluster {
	c.pingFrequency = freq
	return c
}

func (c cluster) WithNodesToPing(amount int) cluster {
	c.nodesToPing = amount
	return c
}

func (c cluster) WithNodeTimeout(timeout time.Duration) cluster {
	c.nodeTimeout = timeout
	return c
}

func (c cluster) WithHttpListenAddress(addr string) cluster {
	c.httpListenAddress = addr
	return c
}

func (c cluster) Start() (*cluster, error) {
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

	c.addSelfToCluster()

	if c.clusterAddress == "" {
		return &c, c.startCluster()
	} else {
		return &c, c.connectToCluster()
	}
}

func (c *cluster) addSelfToCluster() {
	me := gossip.Member{
		Address:   c.listenAddress,
		Heartbeat: 1,
		Timestamp: time.Now().UnixNano(),
		Alive:     true,
	}

	c.members.List[me.Address] = &me
}

func (c *cluster) connectToCluster() error {
	log.Printf("Connecting to cluster at %s \n", c.clusterAddress+"/join")

	/*data, err := proto.Marshal(c.members.List[c.listenAddress])
	if err != nil {
		return err
	}

	res, err := (&http.Client{Timeout: time.Duration(4 * time.Second)}).Post("http://"+c.clusterAddress+"/join", "", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	data, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}*/
	c.memberListMutex.RLock()
	conn, err := grpc.Dial(c.clusterAddress, grpc.WithInsecure(), grpc.WithTimeout(4*time.Second))
	c.memberListMutex.RUnlock()
	if err != nil {
		return err
	}
	client := gossip.NewNodeClient(conn)
	clusterMemberList, err := client.Join(context.Background(), c.members.List[c.listenAddress])
	if err != nil {
		conn.Close()
		return err
	}
	conn.Close()

	c.memberListMutex.Lock()
	for _, item := range clusterMemberList.List {
		if item.Address != c.listenAddress {
			c.members.List[item.Address] = item
			c.members.List[item.Address].Timestamp = time.Now().UnixNano()
			log.Printf("New node added when joining: %s \n", item.Address)
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
		for _, item := range c.members.List {
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
	for _, item := range c.members.List {
		if time.Now().Sub(time.Unix(0, item.Timestamp)) > c.nodeTimeout {
			if item.Alive == true {
				item.Alive = false
				item.Timestamp = time.Now().UnixNano()
				log.Printf("Node marked dead by timeout: %s \n", item.Address)
			} else {
				nodeToDelete = append(nodeToDelete, item.Address) // Better not delete in palce while for ranging.
			}
		}
	}
	for _, addr := range nodeToDelete {
		delete(c.members.List, addr)
	}
	c.memberListMutex.Unlock()
}

func (c *cluster) heartbeat() {
	// Update this node.
	c.memberListMutex.Lock()
	c.members.List[c.listenAddress].Heartbeat += 1
	c.members.List[c.listenAddress].Timestamp = time.Now().UnixNano()
	c.memberListMutex.Unlock()
}

func (c *cluster) ping() {

	// Get a list of pingable candidates.
	c.memberListMutex.RLock()
	pingCandidates := make([]string, 0)
	for _, item := range c.members.List {
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
	c.memberListMutex.RLock()
	for _, str := range addressesToPing {
		go func(addr string) {
			wg.Add(1)
			conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithTimeout(time.Second*1))
			if err != nil {
				return
			}
			client := gossip.NewNodeClient(conn)
			_, err = client.Ping(context.Background(), &c.members)
			time.Sleep(time.Second * 2)
			conn.Close()
			wg.Done()
		}(str)
	}
	wg.Wait()
	c.memberListMutex.RUnlock()
}

func (c *cluster) Ping(ctx context.Context, remoteList *gossip.MemberList) (*google_protobuf.Empty, error) {
	c.memberListMutex.Lock()
	for _, item := range remoteList.List {
		localItem, ok := c.members.List[item.Address] // Keep in mind, localItem is a pointer.
		if ok == true {
			if item.Heartbeat > c.members.List[item.Address].Heartbeat {
				// If remote heartbeat is higher, it's surely more up to date than ours.
				localItem.Alive = item.Alive
				localItem.Heartbeat = item.Heartbeat
				localItem.Timestamp = time.Now().UnixNano()
				if localItem.Alive == false {
					log.Printf("Node marked dead by gossip: %s \n", item.Address)
				}

			} else if item.Heartbeat == c.members.List[item.Address].Heartbeat && localItem.Alive == true && item.Alive == false {
				// If remote heartbeat is the same, and remotely this node is dead, it means it's dead but we haven't noticed it yet. Kill it.
				localItem.Alive = false
				localItem.Timestamp = time.Now().UnixNano()
				log.Printf("Node marked dead by gossip: %s \n", item.Address)
			}
		} else {
			// If we do not have a node saved, put it into our list, as long as it is alive.
			if item.Alive == true {
				c.members.List[item.Address] = item
				c.members.List[item.Address].Timestamp = time.Now().UnixNano()
				log.Printf("New node added by gossip: %s \n", item.Address)
			}
		}
	}
	c.memberListMutex.Unlock()

	return &google_protobuf.Empty{}, nil
}
func (c *cluster) Join(ctx context.Context, nodeToJoin *gossip.Member) (*gossip.MemberList, error) {
	c.memberListMutex.Lock()
	c.members.List[nodeToJoin.Address] = nodeToJoin
	c.members.List[nodeToJoin.Address].Timestamp = time.Now().UnixNano()
	c.memberListMutex.Unlock()
	log.Printf("New node added by join: %s \n", nodeToJoin.Address)

	c.memberListMutex.RLock()
	response := gossip.MemberList{}
	response.List = make(map[string]*gossip.Member)
	for _, item := range c.members.List {
		response.List[item.Address] = &gossip.Member{item.Address, item.Heartbeat, item.Timestamp, item.Alive}
	}
	c.memberListMutex.RUnlock()

	return &response, nil
}
func (c *cluster) NotifyLeave(ctx context.Context, nodeToRemove *gossip.Member) (*google_protobuf.Empty, error) {
	c.memberListMutex.Lock()
	c.members.List[nodeToRemove.Address] = nodeToRemove // He's no more marked as alive, so that'l do it.
	c.members.List[nodeToRemove.Address].Timestamp = time.Now().UnixNano()
	c.memberListMutex.Unlock()
	log.Printf("Node marked dead by leave: %s \n", nodeToRemove.Address)

	return &google_protobuf.Empty{}, nil
}

// TODO: Get member list from user point of view.
