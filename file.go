package main

import (
	"errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"
)

func LoadOrCreateDbFromDir(path string) (*DB, error) {
	dbPath := filepath.Join(path, "db")

	_, err := os.Stat(dbPath)
	if err != nil {
		var perr *os.PathError
		if !errors.As(err, &perr) {
			return nil, err
		}

		// create db file
		db, err := initDbFile(dbPath)
		if err != nil {
			return db, err
		}
	}

	f, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
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

	dbFileSizeMetric.Set(float64(fstat.Size()))

	logrus.Infof("os page size: %d", os.Getpagesize())
	logrus.Infof("index page count: %d", indexPageCount)

	dbPath, err = filepath.Abs(f.Name())
	if err != nil {
		return nil, err
	}
	logrus.Infof("db path: %s", dbPath)

	db := &DB{
		dataDir: path,
		file:     f,
		data:     m,
		pageSize: uint64(os.Getpagesize()),

		mu: sync.Mutex{},
		serving: false,
	}

	err = checkRecoverWal(db, path)
	if err != nil {
		return nil, err
	}

	err = checkRollbackUndo(db, path)
	return db, nil
}

func checkRollbackUndo(db *DB, path string) error {
	undoPath := filepath.Join(path, "undo")
	undo, err := os.OpenFile(undoPath, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	err = db.flush()
	if err != nil {
		return err
	}

	undo, err = os.OpenFile(undoPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	db.undo = undo

	return nil
}

func checkRecoverWal(db *DB, path string) error {
	walPath := filepath.Join(path, "wal")
	wal, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	walStat, err := wal.Stat()
	if err != nil {
		return err
	}

	meta := db.page(0).meta()
	walFileSizeMetric.Set(float64(walStat.Size()))
	walCheckpointMetric.Set(float64(meta.checkpoint))

	checkpoint := int64(meta.checkpoint)
	if checkpoint < walStat.Size() {
		logrus.Infof("recovering... from wal log due to crash. meta.checkpoint: %d, wal size: %d", meta.checkpoint, walStat.Size())
		_, err := wal.Seek(checkpoint, 0)
		if err != nil {
			return err
		}

		devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			return err
		}

		cmdhdr := newCommandHandler(wal, devNull, wal)

		var handledBytesLength = checkpoint
		for handledBytesLength < walStat.Size() {
			originCmd, cmd, err := cmdhdr.Next()
			err = executeCmd(cmdhdr, db, originCmd, cmd, err)
			if err != nil {
				if err == io.EOF {
					break
				}
				logrus.Fatalf("excuteCmd error when recovery from wal file. err : %s", err.Error())
			}

			handledBytesLength += int64(len(originCmd))
		}

		logrus.Info("reset wal to zero size")
		err = wal.Truncate(0)
		if err != nil {
			logrus.Fatalf("reset wal to zero size error. %s", err.Error())
		}

		logrus.Infof("recover from wal done")
	}

	err = wal.Close()
	if err != nil {
		return err
	}

	meta.checkpoint = uint64(walStat.Size())
	walFileSizeMetric.Set(float64(walStat.Size()))
	walCheckpointMetric.Set(float64(meta.checkpoint))

	err = db.flush()
	if err != nil {
		return err
	}

	wal, err = os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	db.wal = wal

	return nil
}

func initDbFile(dbPath string) (*DB, error) {
	f, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		panic(err.Error())
	}

	buf := make([]byte, os.Getpagesize()*(metaPageCount+freelistPageCount+indexPageCount))

	p := pageInBuffer(buf[:], 0)
	p.id = 0
	p.flags = metaPageFlag
	p.count = 0
	meta := p.meta()
	meta.version = 1
	meta.freelistPgid = 1

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
