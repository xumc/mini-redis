package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

type meta struct {
	version      uint32
	freelistPgid uint64
	elePageCount uint64
}

type freelist struct {
	ids [maxAllocSize]uint64
}

var connCount int
var countMu = sync.Mutex{}

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	db, err := LoadOrCreateDbFromFile("db")
	if err != nil {
		logrus.Fatalf("error when load db file. %s\n", err.Error())
	}
	defer func() {
		err = db.Close()
		if err != nil {
			logrus.Fatalf("db close error. %s\n", err.Error())
		}
	}()

	port := 6379

	logrus.Infof("listen on port %d", port)

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "localhost", port))
	if err != nil {
		logrus.Fatalf("listen tcp failed. %s\n", err.Error())
	}
	defer l.Close()

	go func() {
		tick := time.NewTicker(time.Second)
		for {
			<- tick.C
			countMu.Lock()
			fmt.Println("count: ", connCount)
			countMu.Unlock()
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			logrus.Errorf("accept tcp failed. %s\n", err.Error())
		}

		countMu.Lock()
		connCount++
		countMu.Unlock()

		logrus.Infof("accept conn: remote addr: %s", conn.RemoteAddr().String())

		go handleConn(conn, db)
	}
}
