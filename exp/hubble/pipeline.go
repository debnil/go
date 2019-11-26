// +build go1.13

package hubble

import (
	"context"

	"github.com/olivere/elastic/v7"
	"github.com/stellar/go/exp/ingest"
	"github.com/stellar/go/exp/ingest/pipeline"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/historyarchive"
)

const archivesURL = "http://history.stellar.org/prd/core-live/core_live_001/"

// PipelineDefaultType is the default type of state pipeline.
// It will track the current state and store entries in Elasticsearch.
const PipelineDefaultType = "currentStateEntries"

// PipelineCurrentStateType will just collect and store the current ledger state in memory.
const PipelineCurrentStateType = "currentState"

// PipelineSearchType is the other choice for type of state pipeline.
// This state pipeline writes entries to a running Elasticsearch instance.
const PipelineSearchType = "elasticSearch"

// NewStatePipelineSession returns a single ledger state session.
func NewStatePipelineSession(pipelineType, esURL, esIndex string) (*ingest.SingleLedgerSession, error) {
	archive, err := newArchive()
	if err != nil {
		return nil, errors.Wrap(err, "could not create archive")
	}

	var statePipeline *pipeline.StatePipeline
	switch pipelineType {
	case PipelineDefaultType:
		statePipeline, err = newCurrentStateEntriesPipeline(esURL)
		if err != nil {
			return nil, errors.Wrap(err, "could not create current state entries pipeline")
		}
	case PipelineSearchType:
		statePipeline, err = newElasticSearchPipeline(esURL, esIndex)
		if err != nil {
			return nil, errors.Wrap(err, "could not create elastic search pipeline")
		}
	case PipelineCurrentStateType:
		statePipeline, err = newCurrentStatePipeline()
		if err != nil {
			return nil, errors.Wrap(err, "could not create current state pipeline")
		}
	default:
		return nil, errors.Errorf("invalid state pipeline type: %s", pipelineType)
	}

	session := &ingest.SingleLedgerSession{
		Archive:       archive,
		StatePipeline: statePipeline,
	}
	return session, nil
}

func newArchive() (*historyarchive.Archive, error) {
	archive, err := historyarchive.Connect(
		archivesURL,
		historyarchive.ConnectOptions{},
	)
	if err != nil {
		return nil, err
	}
	return archive, nil
}

func newCurrentStatePipeline() (*pipeline.StatePipeline, error) {
	sp := &pipeline.StatePipeline{}
	csProcessor := &CurrentStateProcessor{
		ledgerState: make(map[string]accountState),
	}
	sp.SetRoot(
		pipeline.StateNode(csProcessor),
	)
	return sp, nil
}

func newElasticSearchPipeline(esURL, esIndex string) (*pipeline.StatePipeline, error) {
	sp := &pipeline.StatePipeline{}
	client, err := newClientWithIndex(esURL, esIndex)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create new es client and index")
	}
	esProcessor := ESProcessor{
		client: client,
		index:  esIndex,
	}
	sp.SetRoot(
		pipeline.StateNode(&esProcessor),
	)
	return sp, nil
}

func newCurrentStateEntriesPipeline(esURL string) (*pipeline.StatePipeline, error) {
	sp := &pipeline.StatePipeline{}
	client, err := newClientWithIndex(esURL)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create new es client")
	}
	ctx := context.Background()
	cseProcessor, err := makeNewCSEProcessor(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't make new current state + entries processor")
	}
	sp.SetRoot(pipeline.StateNode(cseProcessor))
	return sp, nil
}

func newClientWithIndex(esURL string, esIndexes ...string) (*elastic.Client, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(esURL),
	)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create es client")
	}

	ctx := context.Background()
	_, _, err = client.Ping(esURL).Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't ping es server")
	}

	if len(esIndexes) == 0 {
		return client, nil
	}

	for _, index := range esIndexes {
		exists, err := client.IndexExists(index).Do(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't check es index %s existence", index)
		}

		if !exists {
			_, err = client.CreateIndex(index).Do(ctx)
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't create es index %s", index)
			}
		}
	}

	return client, nil
}
