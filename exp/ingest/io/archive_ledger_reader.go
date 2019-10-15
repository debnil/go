package io

import (
	"io"
	"sync"

	"github.com/stellar/go/support/historyarchive"

	"github.com/stellar/go/xdr"
)

// ArchiveLedgerReader placeholder
// type ArchiveLedgerReader interface {
// 	GetSequence() uint32
// 	Read() (bool, xdr.Transaction, xdr.TransactionResult, error)
// }

// TODO: Determine if we need a separate interface at all.
// ArchiveLedgerReader provides access to transactions within a ledger from the history archive.
type ArchiveLedgerReader interface {
	GetSequence() uint32
	GetHeader() xdr.LedgerHeaderHistoryEntry
	// Read should return the next transaction. If there are
	// no more transactions, it should return `io.EOF` error.
	Read() (LedgerTransaction, error)
	// Close should be called when reading is finished.
	Close() error
}

// TransactionsArchiveLedgerReader implements the io.ArchiveLedgerReader interface.
// TODO: Rename TransactionsArchiveLedgerReader.
type TransactionsArchiveLedgerReader struct {
	sequence     uint32
	archive      historyarchive.Archive
	header       xdr.LedgerHeaderHistoryEntry
	transactions []LedgerTransaction
	readIdx      int
	initOnce     sync.Once
	readMutex    sync.Mutex
}

// Ensure TransactionsArchiveLedgerReader implements ArchiveLedgerReader.
var _ ArchiveLedgerReader = (*TransactionsArchiveLedgerReader)(nil)

// GetSequence returns the sequence number of the ledger data stored by this object.
func (talr *TransactionsArchiveLedgerReader) GetSequence() uint32 {
	return talr.sequence
}

// Read returns the next transaction in the ledger, ordered by tx number.
// When there are no more transactions to return, an EOF error is returned.
func (talr *TransactionsArchiveLedgerReader) Read() (LedgerTransaction, error) {
	var err error
	talr.initOnce.Do(func() { err = talr.init() })
	if err != nil {
		return LedgerTransaction{}, err
	}

	// Protect any access to talr.readIdx
	talr.readMutex.Lock()
	defer talr.readMutex.Unlock()

	if talr.readIdx < len(talr.transactions) {
		talr.readIdx++
		return talr.transactions[talr.readIdx-1], nil
	}
	return LedgerTransaction{}, io.EOF
}

// GetHeader returns the XDR Header data associated with the stored ledger.
func (talr *TransactionsArchiveLedgerReader) GetHeader() xdr.LedgerHeaderHistoryEntry {
	var err error

	talr.initOnce.Do(func() { err = talr.init() })
	if err != nil {
		panic(err)
	}
	return talr.header
}

// Close moves the read pointer so subsequent calls to Read() return EOF.
func (talr *TransactionsArchiveLedgerReader) Close() error {
	talr.readMutex.Lock()
	talr.readIdx = len(talr.transactions)
	talr.readMutex.Unlock()
	return nil
}

// Init pulls data from thee archive backend to set up this object.
func (talr *TransactionsArchiveLedgerReader) init() error {
	// TODO: Initialize state using archive.
	return nil
}
