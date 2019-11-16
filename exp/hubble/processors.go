// +build go1.13

package hubble

import (
	"context"
	"fmt"
	stdio "io"
	"strconv"

	"github.com/olivere/elastic/v7"
	"github.com/stellar/go/exp/ingest/io"
	ingestPipeline "github.com/stellar/go/exp/ingest/pipeline"
	supportPipeline "github.com/stellar/go/exp/support/pipeline"
	"github.com/stellar/go/support/errors"
)

// ESProcessor serializes ledger change entries as JSONs and writes them
// to an ElasticSearch cluster. For now, it only writes 25 examples of each entry
// for quicker debugging and testing of our printing process.
type ESProcessor struct {
	client *elastic.Client
	index string
}

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

		// Step 1: convert entry to JSON-ified string.
		// TODO: Move this to a separate processor. Currently, this is not possible,
		// as the ingestion system does not read and write custom structs
		// between pipeline nodes.
		entryJsonStr, err := serializeLedgerEntryChange(entry)
		if err != nil {
			return errors.Wrap(err, "couldn't convert ledgerentry to json")
		}

		// Step 2: Augment the JSON-fied data.
		// TODO: Implement Step 2 as a separate processor.

		// Step 3: put entry as JSON in ElasticSearch.
		err = p.PutEntry(ctx, entryJsonStr, numEntries)
		if err != nil {
			return errors.Wrap(err, "couldn't put entry json in elasticsearch")
		}

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

func (p *ESProcessor) PutEntry(ctx context.Context, entry string, id int) error {
	idStr := strconv.Itoa(id)
	_, err := p.client.Index().Index(p.index).Id(idStr).BodyString(entry).Do(ctx)
	return err
}