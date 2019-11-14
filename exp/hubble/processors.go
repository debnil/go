// +build go1.13

package hubble

import (
	"context"
	"fmt"
	stdio "io"

	"github.com/stellar/go/exp/ingest/io"
	ingestPipeline "github.com/stellar/go/exp/ingest/pipeline"
	supportPipeline "github.com/stellar/go/exp/support/pipeline"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// ESProcessor serializes ledger change entries as JSONs and writes them
// to an ElasticSearch cluster. For now, it only writes 25 examples of each entry
// for quicker debugging and testing of our printing process.
type ESProcessor struct{}

var _ ingestPipeline.StateProcessor = &ESProcessor{}

// Reset is a no-op for this processor.
func (p *ESProcessor) Reset() {}

// ProcessState reads, prints, and writes changes to ledger state to ElasticSearch.
// Right now, that is limited to 25 entries of each ledger entry type.
func (p *ESProcessor) ProcessState(ctx context.Context, store *supportPipeline.Store, r io.StateReader, w io.StateWriter) error {
	defer w.Close()
	defer r.Close()

	numEntries := 0
	entriesCountDict := make(map[string]int)
	for {
		entry, err := r.Read()
		if err != nil {
			if err == stdio.EOF {
				break
			} else {
				return err
			}
		}

		// If we have found 100 total entries, exit the loop.
		if numEntries == 100 {
			break
		}

		// Skip entries that are not of type `State`.
		// This can be swapped with other types: Removed, Created, Updated.
		if entry.Type != xdr.LedgerEntryChangeTypeLedgerEntryState {
			continue
		}

		// Ensure that we have up to 25 examples of each of the 4 ledger
		// entry types.
		entryType := entry.EntryType().String()
		if currEntryCount, ok := entriesCountDict[entryType]; ok {
			if currEntryCount == 25 {
				continue
			}
			entriesCountDict[entryType]++
		} else {
			entriesCountDict[entryType] = 1
		}
		numEntries++

		bytes, err := serializeLedgerEntryChange(entry)
		if err != nil {
			return errors.Wrap(err, "converting ledgerentry to json")
		}
		fmt.Printf("%s\n", bytes)

		select {
		case <-ctx.Done():
			return nil
		default:
			continue
		}
	}

	fmt.Printf("Found %d total entries\n", numEntries)
	for entryType, numTypeEntries := range entriesCountDict {
		fmt.Printf("Entry Type %s has %d examples\n", entryType, numTypeEntries)
	}
	return nil
}

// Name returns the processor name.
func (p *ESProcessor) Name() string {
	return "ESProcessor"
}
