// +build go1.13

package main

import (
	"flag"
	"fmt"

	"github.com/stellar/go/exp/hubble"
	"github.com/stellar/go/support/errors"
)

// If no configuration settings are provided, the default is that
// the user is running a standard local ElasticSearch setup.
const elasticSearchDefaultURL = "http://127.0.0.1:9200"

// Set a default generic index for ElasticSearch.
const elasticSearchDefaultIndex = "testindex"

// Set the default type of the pipeline to `currentState`, which means
// that it will just collect and store the current ledger state in memory.
const pipelineDefaultType = "currentState"

// The other choice for type of state pipeline is `elasticSearch`, which would
// write entries to a running ElasticSearch instance.
const pipelineSearchType = "elasticSearch"

func main() {
	// TODO: Remove pipelineTypePtr flag once current state pipeline and elastic search are merged.
	pipelineTypePtr := flag.String("type", pipelineDefaultType, "type of state pipeline, choices are currentState and elasticSearch")
	esURLPtr := flag.String("esurl", elasticSearchDefaultURL, "URL of running ElasticSearch server")
	esIndexPtr := flag.String("esindex", elasticSearchDefaultIndex, "index for ElasticSearch")
	flag.Parse()

	pipelineType := *pipelineTypePtr
	// Validate that pipeline type is either "current" or "search".
	if (pipelineType != pipelineDefaultType) && (pipelineType != pipelineSearchType) {
		panic(errors.Errorf("invalid pipeline type %s, must be 'current' or 'state'", pipelineType))
	}

	session, err := hubble.NewStatePipelineSession(pipelineType, *esURLPtr, *esIndexPtr)
	if err != nil {
		panic(errors.Wrap(err, "could not make new state pipeline session"))
	}
	fmt.Printf("Running state pipeline session of type %s\n", pipelineType)
	err = session.Run()
	if err != nil {
		panic(errors.Wrap(err, "could not run session"))
	}
}
