// +build go1.13

package hubble

import (
	"bytes"

	"github.com/stellar/go/xdr"
	goxdr "github.com/xdrpp/goxdr/xdr"
	"github.com/xdrpp/stc/stcdetail"
	"github.com/xdrpp/stc/stx"
)

func entryChangeAsJSONStr(change xdr.LedgerEntryChange) (string, error) {
	stxChange := stx.LedgerEntryChange{}
	changeBytes, err := change.MarshalBinary()
	if err != nil {
		return "", err
	}
	stx.XDR_LedgerEntryChange(&stxChange).XdrMarshal(&goxdr.XdrIn{In: bytes.NewReader(changeBytes)}, "")
	changeJSONBytes, err := stcdetail.XdrToJson(&stxChange)
	if err != nil {
		return "", err
	}
	return string(changeJSONBytes), nil
}

func makeChangeID(change *xdr.LedgerEntryChange) (string, error) {
	changeID := ""
	// TODO: Implement.
	return changeID, nil
}
