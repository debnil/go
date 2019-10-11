package ledgerbackend

import "github.com/stellar/go/support/historyarchive"

// Ensure ArchiveLedgerBackend implements LedgerBackend.
var _ LedgerBackend = (*ArchiveLedgerBackend)(nil)

// ArchiveLedgerBackend implements a history archive data store.
type ArchiveLedgerBackend struct {
	archive *historyarchive.Archive
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
	return nil
}
