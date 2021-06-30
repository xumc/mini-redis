package main

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"net"
	"net/http"
)

type meta struct {
	version      uint32
	freelistPgid uint64
	elePageCount uint64
}

type freelist struct {
	ids [maxAllocSize]uint64
}

func main() {
	logrus.SetLevel(logrus.InfoLevel)

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
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":8082", nil)
		logrus.Fatalf("unpected error when listen http server. %s", err.Error())
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			logrus.Errorf("accept tcp failed. %s\n", err.Error())
		}

		connCounterGauge.Inc()

		logrus.Infof("accept conn: remote addr: %s", conn.RemoteAddr().String())

		go handleConn(conn, db)
	}
}
