package main

import (
	"github.com/cube2222/GossipMembership"
	"log"
	"os"
	"sync"
)

func main() {
	var err error
	if os.Args[1] == "start" {
		_, err = GossipMembership.NewCluster(os.Args[2]).Start()
	} else if os.Args[1] == "join" {
		_, err = GossipMembership.NewCluster(os.Args[2]).WithClusterAddress(os.Args[3]).Start()
	} else {
		log.Fatal("First argument must be <start> or <join>.")
	}
	if err != nil {
		log.Println(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
