// +build go1.13

package hubble

import (
	"bytes"
	"fmt"

	"github.com/stellar/go/xdr"
	goxdr "github.com/xdrpp/goxdr/xdr"
	"github.com/xdrpp/stc/stcdetail"
	"github.com/xdrpp/stc/stx"
)

func xdrEntryToJSONStr(lec xdr.LedgerEntryChange) (string, error) {
	stxlec, err := xdrEntryToStx(lec)
	if err != nil {
		return "", err
	}
	lecJSONStr, err := stxEntryToStr(stxlec)
	if err != nil {
		return "", err
	}
	return lecJSONStr, nil
}

func xdrEntryToStx(lec xdr.LedgerEntryChange) (stx.LedgerEntryChange, error) {
	stxlec := stx.LedgerEntryChange{}
	lecBytes, err := lec.MarshalBinary()
	if err != nil {
		return stxlec, err
	}
	stx.XDR_LedgerEntryChange(&stxlec).XdrMarshal(&goxdr.XdrIn{In: bytes.NewReader(lecBytes)}, "")
	return stxlec, nil
}

func stxEntryToStr(stxlec stx.LedgerEntryChange) (string, error) {
	lecJSONBytes, err := stcdetail.XdrToJson(&stxlec)
	if err != nil {
		return "", err
	}
	lecJSONStr := fmt.Sprintf("%s\n", lecJSONBytes)
	return lecJSONStr, nil
}
