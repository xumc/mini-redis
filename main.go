package main

import (
	"github.com/sirupsen/logrus"
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

	// TODO connection

	err = db.Close()
	if err != nil {
		logrus.Fatalf("db close error. %s\n", err.Error())
	}
}
