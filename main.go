package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"net"
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
	logrus.SetLevel(logrus.TraceLevel)

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
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "localhost", port))
	if err != nil {
		logrus.Fatalf("listen tcp failed. %s\n", err.Error())
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			logrus.Errorf("accept tcp failed. %s\n", err.Error())
		}

		go func() {
			buf := make([]byte, 1024)
			for {
				_, err := conn.Read(buf)
				if err != nil {
					if err == io.EOF {
						err := conn.Close()
						if err != nil {
							logrus.Errorf("close error %s\n", err.Error())
						}
						return
					}
					logrus.Errorf("read from cli error %s\n", err.Error())
				}
				fmt.Print(string(buf))
			}
		}()
	}



}
