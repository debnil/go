package ledgerbackend

import (
	"sync"

	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/historyarchive"
)

// Ensure ArchiveLedgerBackend implements LedgerBackend.
var _ LedgerBackend = (*ArchiveLedgerBackend)(nil)

// readResult is the result of reading a category file.
// TODO: Figure out the fields that get populated.
type readResult struct {
	e error
}

// ArchiveLedgerBackend implements a history archive data store.
type ArchiveLedgerBackend struct {
	archive   *historyarchive.Archive
	tempStore TempSet
	// TODO: Check if we need a `sequence` field for a singular sequence number.
	readChan   chan readResult
	streamOnce sync.Once
	closeOnce  sync.Once
	done       chan bool
}

// NewArchiveLedgerBackend is a factory method
func NewArchiveLedgerBackend(archiveURL string, connectOptions historyarchive.ConnectOptions) (*ArchiveLedgerBackend, error) {
	archive, err := historyarchive.Connect(archiveURL, connectOptions)
	if err != nil {
		return nil, err
	}
	return &ArchiveLedgerBackend{archive: archive}, nil
}

// GetLatestLedgerSequence returns the most recent ledger sequence number present in the archives.
func (alb *ArchiveLedgerBackend) GetLatestLedgerSequence() (uint32, error) {
	var ledger []ledgerHeader
	// TODO: Populate ledger using archive.
	return ledger[0].LedgerSeq, nil
}

// GetLedger returns the LedgerCloseMeta for the given ledger sequence number. The first returned
// value is false when the ledger does not exist in the database.
// TODO: Implement.
func (alb *ArchiveLedgerBackend) GetLedger(sequence uint32) (bool, LedgerCloseMeta, error) {
	lcm := LedgerCloseMeta{}
	// TODO: Get relevant entries.
	// TODO: Store the lcm header.
	// TODO: Add found transactions in the LedgerCloseMeta.
	return true, lcm, nil
}

// Close disconnects from the history archives.
// TODO: Implement.
func (alb *ArchiveLedgerBackend) Close() error {
	alb.closeOnce.Do(alb.close)
	return nil
}

func (alb *ArchiveLedgerBackend) streamCategory() {
	defer func() {
		err := alb.tempStore.Close()
		if err != nil {
			alb.readChan <- alb.error(errors.New("Error closing tempStore"))
		}
		alb.closeOnce.Do(alb.close)
		close(alb.readChan)
	}()

}

func (alb *ArchiveLedgerBackend) error(err error) readResult {
	// TODO: Fill in the actual result type.
	return readResult{err}
}

func (alb *ArchiveLedgerBackend) close() {
	close(alb.done)
}
