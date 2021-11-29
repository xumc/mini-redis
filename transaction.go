package main

import (
	"context"
	"sync/atomic"
)

var (
	transactionID uint64
)
func (db *DB) Transaction(fn func(ctx context.Context, transactionID uint64) error) error {
	txid := db.getTransactionID()

	defer func() {
		if r := recover(); r != nil {
			err := db.rollback(txid)
			if err != nil {
				panic(err)
			}
		}
	}()

	ctx := context.Background()

	err := fn(ctx, txid)
	if err != nil {
		nerr := db.rollback(txid)
		if nerr != nil {
			panic(nerr)
		} else {
			return err
		}
	}

	return nil
}

func (db *DB) getTransactionID() uint64 {
	txid := atomic.AddUint64(&transactionID, 1)
	return txid
}

func (db *DB) rollback(txid uint64) error {

	return nil
}

