package main

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"net"
	"net/http"
	"os"
	"strings"
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

type timeFormatter struct{}

func (s *timeFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := time.Now().Local().Format("2006/01/02 15:04:05")
	msg := fmt.Sprintf("%s [%s] %s\n", timestamp, strings.ToUpper(entry.Level.String()), entry.Message)
	return []byte(msg), nil
}

func main() {
	err := os.RemoveAll("db")
	if err != nil {
		panic(err)
	}

	logrus.SetFormatter(&timeFormatter{})
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

		logrus.Debugf("accept conn: remote addr: %s", conn.RemoteAddr().String())

		go handleConn(conn, db)
	}
}
