package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"math/rand"
	"os"
	"syscall"
	"unsafe"
)

const (
	elePageFlag      = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10

	metaPageCount     = 1
	freelistPageCount = 1
	indexPageCount    = 64
	elePageIncrementCount = 1024

	elementsCountInOnePage = uint32(256)
	hashedBitCount         = 16 // 2 ^ 16 = 65536

	maxAllocSize = 0x7FFFFFFF
)

var (
	NotFoundError = errors.New("not found")

	insufficientFreeSpaceInPageError = errors.New("insufficient free space")

	noUnfullPageError = errors.New("no unfull page error")

	elePageCount = 0
)

type DB struct {
	file     *os.File
	data     []byte
	pageSize uint64
}

type IndexEle struct {
	pgid uint64
	at   uint16
}

type Ele struct {
	flags byte // 76543210, 0=delete
	next  IndexEle
	pos   uint32
	kSize uint32
	vSize uint32
}

func (e *Ele) key() []byte {
	p := uintptr(unsafe.Pointer(e)) + uintptr(e.pos)
	bs := (*[maxAllocSize]byte)(unsafe.Pointer(p))
	return (*bs)[:e.kSize]
}

func (e *Ele) val() []byte {
	p := uintptr(unsafe.Pointer(e)) + uintptr(e.pos)
	bs := (*[maxAllocSize]byte)(unsafe.Pointer(p))
	return (*bs)[e.kSize:(e.kSize + e.vSize)]
}

func (e *Ele) isDeleted() bool {
	return e.flags&0x01 == 1
}

func (e *Ele) undelete() {
	e.flags = e.flags &^ 1
}

func (e *Ele) delete() {
	e.flags |= 1
}

type Elements struct {
	eles [elementsCountInOnePage]Ele
	data [maxAllocSize]byte
}

func (db *DB) Close() error {
	err := unix.Munmap(db.data)
	if err != nil {
		return err
	}

	return db.file.Close()
}

func (db *DB) Set(key, val []byte) error {
	preIe, ie := db.findIndexEleInChain(key)

	if ie.pgid == 0 {
		// no found in index
		return db.createEle(key, val, preIe, ie)
	}

	// found in index
	err := db.updateExistingEle(key, val, ie)
	if err == insufficientFreeSpaceInPageError {
		// TODO remove the ele and then create new one.
		logrus.Fatal("TODO : remove the ele and then create new one.")
	}

	return err
}

func (db *DB) Get(key []byte) ([]byte, error) {
	_, ie := db.findIndexEleInChain(key)

	if ie.pgid == 0 {
		return nil, NotFoundError
	}

	// found
	pg := db.page(ie.pgid)
	es := pg.elements()
	ele := &es.eles[ie.at]

	return ele.val(), nil
}

func (db *DB) Delete(key []byte) error {
	_, ie := db.findIndexEleInChain(key)
	if ie.pgid == 0 {
		return nil
	}

	pg := db.page(ie.pgid)
	es := pg.elements()
	ele := &es.eles[ie.at]

	ele.delete()
	return db.flush()
}

func (db *DB) createEleInPage(key, val []byte, ie *IndexEle, pg *page) error {
	// check sufficiency of free space.
	usedSize := pg.usedSize()
	willUseSize := uint32(len(key)+len(val)) + usedSize
	if uint64(willUseSize) > uint64(1+pg.overflow)*db.pageSize {
		return insufficientFreeSpaceInPageError
	}

	es := pg.elements()

	var kvPos uint32
	if pg.count == 0 {
		kvPos = elementsCountInOnePage * uint32(unsafe.Sizeof(Ele{}))
	} else {
		lastEle := es.eles[pg.count-1]
		kvPos = lastEle.pos + lastEle.kSize + lastEle.vSize - uint32(unsafe.Sizeof(Ele{}))
	}

	es.eles[pg.count] = Ele{
		flags: 0x00,
		pos:   kvPos,
		kSize: uint32(len(key)),
		vSize: uint32(len(val)),
	}

	kv := append(key, val...)
	eleOffsetSize := (elementsCountInOnePage - uint32(pg.count)) * uint32(unsafe.Sizeof(Ele{}))

	copy(es.data[(kvPos-eleOffsetSize):], kv)

	ie.pgid = uint64(pg.id)
	ie.at = pg.count

	pg.count++

	return db.flush()
}

func (db *DB) createEle(key, val []byte, preIe, ie *IndexEle) error {
	var tryCount int
	var pgid uint64

	if preIe != nil {
		pgid = preIe.pgid
	}

	const maxGetUnfullCount = 3

	for tryCount < maxGetUnfullCount {
		if pgid == 0 {
			var err error
			pgid, err = db.getUnfullPgid()
			if err != nil {
				if err == noUnfullPageError {
					pgid, err = db.growPages()
					if err != nil {
						return err
					}
				} else {
					return err
				}
			}
		}

		if pgid == 0 && tryCount == (maxGetUnfullCount - 1) {
			firstPgid, err := db.growPages()
			if err != nil {
				return err
			}

			pgid = firstPgid
		}

		pg := db.page(pgid)
		if uint32(pg.count) == elementsCountInOnePage {
			db.removeFullPgid(pgid)
		}

		err := db.createEleInPage(key, val, ie, pg)
		if err != nil {
			if err != insufficientFreeSpaceInPageError {
				return err
			}
		} else {
			return nil
		}

		pgid = 0
		tryCount++
	}

	logrus.Errorln("unreachable code")
	return errors.New("unreachable code")
}

func (db *DB) updateExistingEle(key, val []byte, ie *IndexEle) error {
	pg := db.page(ie.pgid)
	es := pg.elements()
	ele := &es.eles[ie.at]

	eleOffsetSize := (elementsCountInOnePage - uint32(ie.at)) * uint32(unsafe.Sizeof(Ele{}))
	lastEle := es.eles[pg.count-1]

	// if the key val size is larger than before.
	if ele.vSize < uint32(len(val)) {
		// check sufficiency of free space.
		willUseSize := uint32(len(key)+len(val)) - ele.vSize + pg.usedSize()
		if uint64(willUseSize) > uint64(1+pg.overflow)*db.pageSize {
			return insufficientFreeSpaceInPageError
		}
	}

	kvLen := len(key) + len(val)
	kv := append(key, val...)
	oldKVLen := int(ele.kSize + ele.vSize)

	oldDataLen := int(lastEle.pos + lastEle.kSize + lastEle.vSize - (elementsCountInOnePage-uint32(pg.count))*uint32(unsafe.Sizeof(Ele{})))

	copy(es.data[(int(ele.pos+ele.kSize+ele.vSize-eleOffsetSize)+kvLen-oldKVLen):], es.data[(ele.pos-eleOffsetSize+ele.kSize+ele.vSize):oldDataLen])

	copy(es.data[(ele.pos-eleOffsetSize):], kv)

	for i := ie.at + 1; i < pg.count; i++ {
		es.eles[i].pos += uint32(kvLen - oldKVLen)
	}

	ele.kSize = uint32(len(key))
	ele.vSize = uint32(len(val))

	return db.flush()
}

func (db *DB) findIndexEleInChain(key []byte) (preIe, ie *IndexEle) {
	hbs := md5.Sum(key)
	hashedPos := binary.BigEndian.Uint16(hbs[:hashedBitCount])

	pos := uint64(hashedPos)*uint64(unsafe.Sizeof(IndexEle{})) + (metaPageCount+freelistPageCount)*db.pageSize
	ie = (*IndexEle)(unsafe.Pointer(&db.data[pos]))

	if ie.pgid == 0 {
		// no found in index
		return preIe, ie
	}

	// found in index
	pg := db.page(ie.pgid)
	es := pg.elements()
	ele := &es.eles[ie.at]

	// linear search same hash value linklist
	for ie.pgid > 0 {
		// found in chain
		if bytes.Equal(ele.key(), key) && !ele.isDeleted() {
			return preIe, ie
		}

		preIe = ie
		// not found in chain, try next
		ie = &ele.next
		pg = db.page(ie.pgid)
		es = pg.elements()
		ele = &es.eles[ie.at]
	}

	return preIe, ie
}

func (db *DB) getUnfullPgid() (uint64, error) {
	pgid := db.page(0).meta().freelistPgid
	pg := db.page(pgid)
	fl := pg.freelist()

	if pg.count == 0 {
		return 0, noUnfullPageError
	}

	rand := rand.Intn(int(pg.count))

	pgid = fl.ids[rand]
	logrus.Debugf("geted unfull pgid %d\n", pgid)
	return pgid, nil
}

func (db *DB) growPages() (firstPgid uint64, err error) {
	logrus.Debugf("growing... up pages, current elePageCount:%d", elePageCount)
	fstat, err := db.file.Stat()
	if err != nil {
		return 0, err
	}

    err = db.file.Truncate(fstat.Size() + int64(elePageIncrementCount * db.pageSize))
    if err != nil {
    	return 0, err
	}

	fstat, err = db.file.Stat()
	if err != nil {
		return 0, err
	}

	m, err := unix.Mmap(int(db.file.Fd()), 0, int(fstat.Size()), unix.PROT_READ|unix.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, err
	}
	db.data = m

	pgids := make([]uint64, 0, elePageIncrementCount)
	for i := metaPageCount + freelistPageCount + indexPageCount + elePageCount; i < (metaPageCount + freelistPageCount + indexPageCount + elePageCount + elePageIncrementCount); i++ {
		pgids = append(pgids, uint64(i))

		pg := db.page(uint64(i))
		pg.id = i
		pg.flags = elePageFlag
		pg.count = 0
	}

	meta := db.page(0).meta()
	flPg := db.page(meta.freelistPgid)
	fl := flPg.freelist()

	copy(fl.ids[flPg.count:], pgids)
	flPg.count += elePageIncrementCount

	firstPgid = uint64(metaPageCount + freelistPageCount + indexPageCount + elePageCount)

	elePageCount += elePageIncrementCount

	return firstPgid, db.flush()
}


func (db *DB) flush() error {
	return unix.Msync(db.data, unix.MS_SYNC)
}

func (db *DB) page(pgid uint64) *page {
	pos := pgid * db.pageSize
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

func (db *DB) removeFullPgid(rmPgid uint64) {
	pgid := db.page(0).meta().freelistPgid
	pg := db.page(pgid)
	fl := pg.freelist()

	for i := 0; i <= int(pg.count); i++ {
		if fl.ids[i] == rmPgid && i+1 < int(pg.count) {
			copy(fl.ids[i:], fl.ids[(i+1):pg.count])
			pg.count--
			return
		}
	}
}

