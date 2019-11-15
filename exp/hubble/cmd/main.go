// +build go1.13

package main

import (
	"flag"
	"fmt"

	"github.com/stellar/go/exp/hubble"
)

const elasticSearchDefaultUrl = "http://127.0.0.1:9200"

func main() {
	urlPtr := flag.String("esurl", elasticSearchDefaultUrl, "URL of running ElasticSearch server")
	fmt.Println("Running the pipeline to serialize XDR entries...")
	session, err := hubble.NewStatePipelineSession(*urlPtr)
	if err != nil {
		panic(err)
	}
	session.Run()
}
