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
	"github.com/stellar/go/xdr"
)

// ESProcessor serializes ledger change entries as JSONs and writes them
// to an ElasticSearch cluster. For now, it only writes 25 examples of each entry
// for quicker debugging and testing of our printing process.
type ESProcessor struct {
	url   string
	index string
}

var _ ingestPipeline.StateProcessor = &ESProcessor{}

// Reset is a no-op for this processor.
func (p *ESProcessor) Reset() {}

func (p *ESProcessor) newESClient(ctx context.Context) (*elastic.Client, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(p.url),
	)
	if err != nil {
		return nil, errors.Wrap(err, "creating elasticsearch client")
	}

	_, _, err = client.Ping(p.url).Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "pinging elasticsearch server")
	}
	return client, nil
}

func (p *ESProcessor) createIndexIfNotExists(ctx context.Context, client *elastic.Client) error {
	exists, err := client.IndexExists(p.index).Do(ctx)
	if err != nil {
		return errors.Wrap(err, "checking elasticsearch index existence")
	}
	if exists {
		return nil
	}

	_, err = client.CreateIndex(p.index).Do(ctx)
	return errors.Wrap(err, "creating elasticsearch index")

}

// ProcessState reads, prints, and writes changes to ledger state to ElasticSearch.
// Right now, that is limited to 25 entries of each ledger entry type.
func (p *ESProcessor) ProcessState(ctx context.Context, store *supportPipeline.Store, r io.StateReader, w io.StateWriter) error {
	defer w.Close()
	defer r.Close()

	client, err := p.newESClient(ctx)
	if err != nil {
		return errors.Wrap(err, "getting new elasticsearch client")
	}

	err = p.createIndexIfNotExists(ctx, client)
	if err != nil {
		return errors.Wrap(err, "checking and creating elasticsearch index")
	}

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

		// Convert entry to JSON-ified string.
		entryJsonBytes, err := serializeLedgerEntryChange(entry)
		if err != nil {
			return errors.Wrap(err, "converting ledgerentry to json")
		}
		entryJsonStr := fmt.Sprintf("%s\n", entryJsonBytes)

		// Put entry as JSON in database.
		entryId := strconv.Itoa(numEntries)
		_, err = client.Index().Index(p.index).Id(entryId).BodyString(entryJsonStr).Do(ctx)
		if err != nil {
			return errors.Wrap(err, "putting entry in elasticsearch")
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
