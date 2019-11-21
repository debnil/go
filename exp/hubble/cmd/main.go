// +build go1.13

package main

import (
	"flag"

	"github.com/stellar/go/exp/hubble"
	"github.com/stellar/go/exp/ingest"
	"github.com/stellar/go/exp/ingest/pipeline"
	"github.com/stellar/go/support/errors"
)

// If no configuration settings are provided, the default is that
// the user is running a standard local ElasticSearch setup.
const elasticSearchDefaultURL = "http://127.0.0.1:9200"

// Set a default generic index for ElasticSearch.
const elasticSearchDefaultIndex = "testindex"

// Set the default type of the pipeline to `current`, which means
// that it will just collect and store the current ledger state in memory.
const pipelineDefaultType = "current"

func main() {
	// TODO: Remove pipelineTypePtr flag once current state pipeline and elastic search are merged.
	pipelineTypePtr := flag.String("type", "current", "type of state pipeline, either current or search")
	esURLPtr := flag.String("esurl", elasticSearchDefaultURL, "URL of running ElasticSearch server")
	esIndexPtr := flag.String("esindex", elasticSearchDefaultIndex, "index for ElasticSearch")
	flag.Parse()

	// Validate that pipeline type is either "current" or "search".
	if (*pipelineTypePtr != "current") && (*pipelineTypePtr != "search") {
		panic(errors.Errorf("invalid pipeline type %s, must be 'current' or 'state'", *pipelineTypePtr))
	}

	session, err := NewStatePipelineSession(*pipelineTypePtr, *esURLPtr, *esIndexPtr)
	if err != nil {
		panic(errors.Wrap(err, "could not make new state pipeline session"))
	}
	err = session.Run()
	if err != nil {
		panic(errors.Wrap(err, "could not run session"))
	}
}
