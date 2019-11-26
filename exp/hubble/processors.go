// +build go1.13

package hubble

import (
	"context"
	"encoding/json"
	"fmt"
	stdio "io"
	"strconv"

	"github.com/kr/pretty"
	"github.com/olivere/elastic/v7"
	"github.com/stellar/go/exp/ingest/io"
	ingestPipeline "github.com/stellar/go/exp/ingest/pipeline"
	supportPipeline "github.com/stellar/go/exp/support/pipeline"
	"github.com/stellar/go/support/errors"
)

// ESProcessor serializes ledger change entries as JSONs and writes them
// to an ElasticSearch cluster.
type ESProcessor struct {
	client *elastic.Client
	index  string
}

var _ ingestPipeline.StateProcessor = &ESProcessor{}

// Reset is a no-op for this processor.
func (p *ESProcessor) Reset() {}

// ProcessState reads, prints, and writes changes to ledger state to ElasticSearch.
func (p *ESProcessor) ProcessState(ctx context.Context, store *supportPipeline.Store, r io.StateReader, w io.StateWriter) error {
	defer w.Close()
	defer r.Close()

	numEntries := 0
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
		entryJSONStr, err := entryChangeAsJSONStr(entry)
		if err != nil {
			return errors.Wrap(err, "couldn't convert ledgerentry to json")
		}

		// Step 2: Augment the JSON-fied data.
		// TODO: Implement Step 2 as a separate processor.

		// Step 3: put entry as JSON in ElasticSearch.
		// TODO: Take ID from entry, rather than a counter.
		err = p.PutEntry(ctx, entryJSONStr, numEntries)
		if err != nil {
			return errors.Wrap(err, "couldn't put entry json in elasticsearch")
		}
		numEntries++

		select {
		case <-ctx.Done():
			return nil
		default:
			continue
		}
	}

	fmt.Printf("Found %d total entries\n", numEntries)
	return nil
}

// Name returns the processor name.
func (p *ESProcessor) Name() string {
	return "ESProcessor"
}

// PutEntry puts a ledger entry in ElasticSearch.
func (p *ESProcessor) PutEntry(ctx context.Context, entry string, id int) error {
	idStr := strconv.Itoa(id)
	_, err := p.client.Index().Index(p.index).Id(idStr).BodyString(entry).Do(ctx)
	return err
}

// CurrentStateProcessor stores only the current state of each account
// on the ledger.
type CurrentStateProcessor struct {
	ledgerState map[string]accountState
}

var _ ingestPipeline.StateProcessor = &CurrentStateProcessor{}

// ProcessState updates the global state using current entries.
func (p *CurrentStateProcessor) ProcessState(ctx context.Context, store *supportPipeline.Store, r io.StateReader, w io.StateWriter) error {
	defer w.Close()
	defer r.Close()

	for {
		entry, err := r.Read()
		if err != nil {
			if err == stdio.EOF {
				break
			} else {
				return err
			}
		}

		accountID, err := makeAccountID(&entry)
		if err != nil {
			return errors.Wrap(err, "could not get ledger account address")
		}
		currentState := p.ledgerState[accountID]
		newState, err := makeNewAccountState(&currentState, &entry)
		if err != nil {
			return errors.Wrap(err, "could not update account state")
		}
		p.ledgerState[accountID] = *newState

		select {
		case <-ctx.Done():
			return nil
		default:
			continue
		}
	}
	fmt.Printf("%# v", pretty.Formatter(p.ledgerState))
	return nil
}

// Reset makes the internal ledger state an empty map.
func (p *CurrentStateProcessor) Reset() {
	p.ledgerState = make(map[string]accountState)
}

// Name returns the name of the processor.
func (p *CurrentStateProcessor) Name() string {
	return "CSProcessor"
}

// CurrentStateEntriesElasticSearchProcessor stores the current state of accounts on
// the ledger along with all entries as documents in an ElasticSearch cluster.
type CurrentStateEntriesElasticSearchProcessor struct {
	client *elastic.Client
	// TODO: Check if we want any other fields.
}

var _ ingestPipeline.StateProcessor = &CurrentStateEntriesElasticSearchProcessor{}

const changeIndex = "changes"

const stateIndex = "state"

func makeNewCSEProcessor(ctx context.Context, client *elastic.Client) (*CurrentStateEntriesElasticSearchProcessor, error) {
	p := CurrentStateEntriesElasticSearchProcessor{client: client}

	// Reset and recreate the index to store entry changes.
	changeIndexExists, err := p.client.IndexExists(changeIndex).Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not check change index existence")
	}
	if changeIndexExists {
		_, err = p.client.DeleteIndex(changeIndex).Do(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "could not delete change index")
		}
	}
	_, err = p.client.CreateIndex(changeIndex).Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not create change index")
	}

	// Reset and recreate the index to store current state documents.
	stateIndexExists, err := p.client.IndexExists(stateIndex).Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not check state index existence")
	}
	if stateIndexExists {
		_, err = p.client.DeleteIndex(stateIndex).Do(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "could not delete state index")
		}
	}
	_, err = p.client.CreateIndex(stateIndex).Do(ctx)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// Reset is a no-op for now.
// TODO: Check how to reset.
func (p *CurrentStateEntriesElasticSearchProcessor) Reset() {}

// ProcessState stores all entry changes to ElasticSearch, along with a document
// containing the current state of each account.
func (p *CurrentStateEntriesElasticSearchProcessor) ProcessState(ctx context.Context, store *supportPipeline.Store, r io.StateReader, w io.StateWriter) error {
	defer w.Close()
	defer r.Close()

	numEntries := 0
	for {
		change, err := r.Read()
		if err != nil {
			if err == stdio.EOF {
				break
			} else {
				return err
			}
		}

		if numEntries == 500 {
			break
		}

		// Step 1: Check if entry change has already been processed.
		// If so, continue.
		// TODO: Replace the counter with the makeChangeID function when implemented.
		// Currently, this is a no-op.
		changeID := strconv.Itoa(numEntries)
		processed, err := p.processedChange(ctx, changeID)
		if err != nil {
			return errors.Wrap(err, "couldn't check if entry change was processed")
		}
		if processed {
			continue
		}

		// Step 2: Convert the entry to JSON.
		changeJSON, err := entryChangeAsJSONStr(change)
		if err != nil {
			return errors.Wrap(err, "couldn't convert ledger entry change to json")
		}

		// Step 3: Augment the entry JSON with more info.
		// TODO: Implement this as separate processor.

		// Step 4: Put the entry JSON in Elasticsearch.
		err = p.putChange(ctx, changeID, changeJSON)
		if err != nil {
			return errors.Wrap(err, "couldn't put entry in elasticsearch")
		}

		// Step 5: Get the current state of the account.
		accountID, err := makeAccountID(&change)
		if err != nil {
			return errors.Wrap(err, "couldn't make account id")
		}

		currentState, err := p.getAccountState(ctx, accountID)
		if err != nil {
			return errors.Wrap(err, "couldn't get current state from elasticsearch")
		}

		// Step 6: Update account state.
		newState, err := makeNewAccountState(&currentState, &change)
		if err != nil {
			return errors.Wrap(err, "couldn't make new account state")
		}

		// Step 7: Put account state in Elasticsearch.
		err = p.putAccountState(ctx, accountID, newState)
		if err != nil {
			return errors.Wrap(err, "couldn't put new state in elasticsearch")
		}

		numEntries++
		select {
		case <-ctx.Done():
			return nil
		default:
			continue
		}
	}
	return nil
}

// Name returns the processor name.
func (p *CurrentStateEntriesElasticSearchProcessor) Name() string {
	return "CurrentStateEntriesElasticSearchProcessor"
}

func (p *CurrentStateEntriesElasticSearchProcessor) processedChange(ctx context.Context, changeID string) (bool, error) {
	changeExists, err := p.client.Exists().Index(changeIndex).Id(changeID).Do(ctx)
	if err != nil {
		return false, errors.Wrap(err, "couldn't execute exists query")
	}
	return changeExists, nil
}

func (p *CurrentStateEntriesElasticSearchProcessor) putChange(ctx context.Context, changeID, changeJSON string) error {
	_, err := p.client.Index().Index(changeIndex).Id(changeID).BodyString(changeJSON).Do(ctx)
	return err
}

func (p *CurrentStateEntriesElasticSearchProcessor) getAccountState(ctx context.Context, accountID string) (accountState, error) {
	docExists, err := p.client.Exists().Index(stateIndex).Id(accountID).Do(ctx)
	if err != nil {
		return accountState{}, errors.Wrap(err, "could not check account state doc exists")
	}
	if !docExists {
		return accountState{}, nil
	}

	fsc := elastic.NewFetchSourceContext(true)
	result, err := p.client.Get().Index(stateIndex).Id(accountID).FetchSourceContext(fsc).Do(ctx)
	if err != nil {
		return accountState{}, errors.Wrap(err, "could not get account state doc from elasticsearch")
	}
	fmt.Printf("Got a result from ES: %v\n", result)
	fmt.Printf("%# v\n", pretty.Formatter(result))
	fmt.Printf("%T\n", result)

	var state accountState
	err = json.Unmarshal(result.Source, &state)
	if err != nil {
		return accountState{}, errors.Wrap(err, "could not unmarshal result json")
	}
	fmt.Println(state)
	return state, nil
}

func (p *CurrentStateEntriesElasticSearchProcessor) putAccountState(ctx context.Context, accountID string, state *accountState) error {
	_, err := p.client.Index().Index(stateIndex).Id(accountID).BodyJson(*state).Do(ctx)
	return err
}
