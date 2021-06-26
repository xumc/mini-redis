package main

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"syscall"
	"unsafe"
)

func LoadOrCreateDbFromFile(path string) (*DB, error) {
	_, err := os.Stat(path)
	if err != nil {
		var perr *os.PathError
		if !errors.As(err, &perr) {
			return nil, err
		}

		// create db file
		db, err := initDbFile(path)
		if err != nil {
			return db, err
		}
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	fstat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	m, err := unix.Mmap(int(f.Fd()), 0, int(fstat.Size()), unix.PROT_READ|unix.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	db := &DB{
		file:     f,
		data:     m,
		pageSize: uint64(os.Getpagesize()),
	}

	meta := db.page(0).meta()
	meta.version = 100
	meta.freelistPgid = 1

	err = db.flush()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func initDbFile(path string) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err.Error())
	}

	buf := make([]byte, os.Getpagesize() * (metaPageCount + freelistPageCount + indexPageCount))

	p := pageInBuffer(buf[:], 0)
	p.id = 0
	p.flags = metaPageFlag
	p.count = 0

	p = pageInBuffer(buf[:], 1)
	p.id = 1
	p.flags = freelistPageFlag
	p.count = 0

	_, err = f.Write(buf)
	if err != nil {
		return nil, err
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func pageInBuffer(b []byte, id int) *page {
	return (*page)(unsafe.Pointer(&b[id*os.Getpagesize()]))
}
