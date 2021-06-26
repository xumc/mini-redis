package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
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

func main() {
	itemCount := 10

	err := os.Remove("db")
	if err != nil {
		logrus.Fatalf("error when rm db. %s", err)
	}

	logrus.SetLevel(logrus.TraceLevel)

	db, err := LoadOrCreateDbFromFile("db")
	if err != nil {
		logrus.Fatalf("error when load db file. %s\n", err.Error())
	}

	for i := 0; i < itemCount; i++ {
		err = db.Set([]byte(fmt.Sprintf("hello%d", i)), []byte(fmt.Sprintf("world%d", i)))
		if err != nil {
			logrus.Fatalf("db set error. %s\n", err.Error())
		}
	}

	err = db.Set([]byte("hello5"), []byte("1234567890"))
	if err != nil {
		logrus.Fatalf("db set hello5 error. %s\n", err.Error())
	}

	err = db.Set([]byte("hello6"), []byte("ABC"))
	if err != nil {
		logrus.Fatalf("db set hello6 error. %s\n", err.Error())
	}

	for _, str := range []string{"hello0", "hello3", "hello5", "hello9"} {
		err = db.Delete([]byte(str))
		if err != nil {
			logrus.Fatalf("db delete hello9 error. %s\n", err.Error())
		}
	}

	err = db.Close()
	if err != nil {
		logrus.Fatalf("db close error. %s\n", err.Error())
	}

	// ------------------------------------------------------------------------
	db2, err := LoadOrCreateDbFromFile("db")
	if err != nil {
		logrus.Fatalf("error when load db file. %s\n", err.Error())
	}

	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("hello%d", i)
		bs, err := db2.Get([]byte(key))
		if err != nil && err != NotFoundError {
			logrus.Fatalf("db get error. %s\n", err.Error())
		}
		if err != NotFoundError {
			fmt.Printf("%s : %s\n", key, string(bs))
		} else {
			fmt.Printf("%s : not found\n", key)
		}
	}

	err = db2.Close()
	if err != nil {
		logrus.Fatalf("db close error. %s\n", err.Error())
	}

	time.Sleep(time.Second)
}
