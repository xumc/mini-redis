package main

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

const a = "A_ACCOUNT_MONEY"
const b = "B_ACCOUNT_MONEY"

func getCurrentValues(db *DB) (av, bv int, err error) {
	aam, err := db.GetString(a)
	if err != nil {
		return 0, 0, err
	}
	bam, err := db.GetString(b)
	if err != nil {
		return 0, 0, err
	}

	aami, err := strconv.Atoi(aam)
	if err != nil {
		return 0, 0, err
	}
	bami, err := strconv.Atoi(bam)
	if err != nil {
		return 0, 0, err
	}

	return aami,  bami, nil
}

func buildNormalTransaction(db *DB) func(ctx context.Context, transactionID uint64) error {
	return func(ctx context.Context, transactionID uint64) error {
		aami, bami, err := getCurrentValues(db)
		if err != nil {
			return err
		}

		aami -= 1
		bami += 1

		err = db.SetString([]byte{}, a, strconv.Itoa(aami))
		if err != nil {
			return err
		}

		err = db.SetString([]byte{}, b, strconv.Itoa(bami))
		if err != nil {
			return err
		}

		return nil
	}
}

func buildUnexpectedTransaction(db *DB) func(ctx context.Context, transactionID uint64) error {
	return func(ctx context.Context, transactionID uint64) error {
		aami, bami, err := getCurrentValues(db)
		if err != nil {
			return err
		}

		aami -= 1
		bami += 1

		err = db.SetString([]byte{}, a, strconv.Itoa(aami))
		if err != nil {
			return err
		}

		if aami == 99 {
			return errors.New("unexpected error")
		}

		err = db.SetString([]byte{}, b, strconv.Itoa(bami))
		if err != nil {
			return err
		}

		return nil
	}
}

func TestDB_Transaction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, t.Name()+"_db")
	err := os.MkdirAll(path, 0777)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	db, err := LoadOrCreateDbFromDir(path)
	assert.Nil(t, err)
	defer db.Close()

	initOps := []op{
		{"S", a, "100"},
		{"S", b, "0"},
	}

	t.Run("normal", func(t *testing.T) {
		rawExecuteCases(t, db, initOps)

		oerr := db.Transaction(buildNormalTransaction(db))

		assert.Nil(t, oerr)
		assertOps := []op{
			{"G", a, "99"},
			{"G", b, "1"},
		}
		rawExecuteCases(t, db, assertOps)
	})

	t.Run("unexpected", func(t *testing.T) {
		rawExecuteCases(t, db, initOps)

		oerr := db.Transaction(buildUnexpectedTransaction(db))
		assert.Equal(t, "unexpected error", oerr.Error())

		assertOps := []op{
			{"G", a, "100"},
			{"G", b, "0"},
		}
		rawExecuteCases(t, db, assertOps)
	})
}

func Test_getTransactionID(t *testing.T) {
	db := &DB{}
	txid := db.getTransactionID()
	assert.Equal(t, uint64(1), txid)

	txid = db.getTransactionID()
	assert.Equal(t, uint64(2), txid)
}