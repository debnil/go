// +build go1.13

package hubble

import (
	"github.com/stellar/go/exp/ingest"
	"github.com/stellar/go/exp/ingest/pipeline"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/historyarchive"
)

const archivesURL = "http://history.stellar.org/prd/core-live/core_live_001/"

// NewStatePipelineSession runs a single ledger session.
func NewStatePipelineSession(esUrl, esIndex string) (*ingest.SingleLedgerSession, error) {
	archive, err := newArchive()
	if err != nil {
		return nil, errors.Wrap(err, "creating archive")
	}
	statePipeline := newStatePipeline(esUrl, esIndex)
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

func newStatePipeline(esUrl, esIndex string) *pipeline.StatePipeline {
	sp := &pipeline.StatePipeline{}
	esProcessor := &ESProcessor{
		url:   esUrl,
		index: esIndex,
	}

	sp.SetRoot(
		pipeline.StateNode(esProcessor),
	)
	return sp
}
