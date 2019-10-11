package io

import (
	"io"
	"sync"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/support/errors"

	"github.com/stellar/go/xdr"
)

// ArchiveLedgerReader provides access to transactions within a ledger from the history archive.
// TODO: Determine if we need a separate interface at all.
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
	backend      ledgerbackend.ArchiveLedgerBackend
	header       xdr.LedgerHeaderHistoryEntry
	transactions []LedgerTransaction
	readIdx      int
	initOnce     sync.Once
	readMutex    sync.Mutex
}

// Ensure TransactionsArchiveLedgerReader implements ArchiveLedgerReader.
var _ ArchiveLedgerReader = (*TransactionsArchiveLedgerReader)(nil)

// NewTransactionsArchiveLedgerReader is a factory method for TransactionsArchiveLedgerReader.
func NewTransactionsArchiveLedgerReader(sequence uint32, backend ledgerbackend.ArchiveLedgerBackend) (*TransactionsArchiveLedgerReader, error) {
	reader := &TransactionsArchiveLedgerReader{
		sequence: sequence,
		backend:  backend,
	}

	var err error
	reader.initOnce.Do(func() { err = reader.init() })
	if err != nil {
		return nil, err
	}
	return reader, nil
}

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
	exists, ledgerCloseMeta, err := talr.backend.GetLedger(talr.sequence)
	if err != nil {
		return errors.Wrap(err, "error reading ledger from backend")
	}

	if !exists {
		return ErrNotFound
	}
	talr.header = ledgerCloseMeta.LedgerHeader
	talr.storeTransactions(ledgerCloseMeta)
	return nil
}

// storeTransactions maps the close meta data into a slice of LedgerTransaction structs.
// This provides a per-transaction view of the data when Read() is called.
func (talr *TransactionsArchiveLedgerReader) storeTransactions(lcm ledgerbackend.LedgerCloseMeta) {
	for i := range lcm.TransactionEnvelope {
		talr.transactions = append(talr.transactions, LedgerTransaction{
			Index:      uint32(i + 1), // Transactions start at '1'
			Envelope:   lcm.TransactionEnvelope[i],
			Result:     lcm.TransactionResult[i],
			Meta:       lcm.TransactionMeta[i],
			FeeChanges: lcm.TransactionFeeChanges[i],
		})
	}
}
