package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"hraft_test/httpd"
	"hraft_test/store"
	"log"
	"net/http"
	"os"
	"os/signal"
)

const (
	DefaultHTTPAddr = ":9000"
	DefaultRaftAddr = ":10000"
)

var dataDir string
var raftDir string
var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string

func init() {
	flag.StringVar(&httpAddr, "http", DefaultHTTPAddr, "HTTP bind address")
	flag.StringVar(&raftAddr, "raft", DefaultRaftAddr, "Raft bind address")
	flag.StringVar(&raftDir, "raftdir", "", "Raft storage directory")
	flag.StringVar(&dataDir, "datadir", "", "Data storage directory")
	flag.StringVar(&joinAddr, "join", "", "Set join address if any")
	flag.StringVar(&nodeID, "id", "", "Node ID")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// Ensure data storage exists
	if dataDir == "" {
		fmt.Fprintf(os.Stderr, "No data storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(dataDir, 0700)

	// Ensure Raft storage exists
	if raftDir == "" {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}
	os.MkdirAll(raftDir, 0700)

	s := store.New(dataDir, raftDir, raftAddr)
	if err := s.Open(joinAddr == "", nodeID); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := httpd.New(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(joinAddr, raftAddr, nodeID); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	log.Println("node started successfully")

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("node exiting")

}

func join(joinAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
