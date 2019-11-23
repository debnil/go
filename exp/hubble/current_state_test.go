// +build go1.13

package hubble

import (
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestGetAccountIDFromState(t *testing.T) {
	wantAddress := "GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF"
	state := accountState{address: wantAddress}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
			},
		},
	}
	gotAddress, err := getAccountID(&change, &state)
	if err != nil {
		t.Error(err)
	}
	if wantAddress != gotAddress {
		t.Fatalf("got address %s, want address %s", gotAddress, wantAddress)
	}
}

func TestGetAccountIDFromChange(t *testing.T) {
	wantAddress := "GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF"
	accountID, err := xdr.AddressToAccountId(wantAddress)
	if err != nil {
		t.Error(err)
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId: accountID,
				},
			},
		},
	}
	gotAddress, err := getAccountID(&change)
	if err != nil {
		t.Error(err)
	}
	if wantAddress != gotAddress {
		t.Fatalf("got address %s, want address %s", wantAddress, gotAddress)
	}
}

func TestGetSeqnumFromNonRemoved(t *testing.T) {
	wantSeqnum := uint32(2947523)
	state := accountState{seqnum: 11}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(wantSeqnum),
			Data: xdr.LedgerEntryData{
				Type:    xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{},
			},
		},
	}
	gotSeqnum, err := getSeqnum(&state, &change)
	if err != nil {
		t.Error(err)
	}
	if wantSeqnum != gotSeqnum {
		t.Fatalf("got seqnum %d, want seqnum %d", gotSeqnum, wantSeqnum)
	}
}

func TestGetSeqnumFromRemoved(t *testing.T) {
	wantSeqnum := uint32(0)
	state := accountState{seqnum: 11}
	change := xdr.LedgerEntryChange{
		Type:  xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		State: &xdr.LedgerEntry{},
	}
	gotSeqnum, err := getSeqnum(&state, &change)
	if err != nil {
		t.Error(err)
	}
	if wantSeqnum != gotSeqnum {
		t.Fatalf("got seqnum %d, want seqnum %d", gotSeqnum, wantSeqnum)
	}
}

func TestGetAccountEntryNotAccount(t *testing.T) {
	accountID, err := xdr.AddressToAccountId("GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF")
	if err != nil {
		t.Error(err)
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeData,
				Data: &xdr.DataEntry{
					AccountId: accountID,
					DataName:  xdr.String64("name"),
					DataValue: xdr.DataValue([]byte("value")),
				},
			},
		},
	}

	entry, err := getAccountEntry(&change)
	if err != nil {
		t.Error(err)
	}
	if entry != nil {
		t.Fatal("got account entry non-nil, want account entry nil")
	}
}

func TestGetAccountEntryRemoved(t *testing.T) {
	accountID, err := xdr.AddressToAccountId("GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF")
	if err != nil {
		t.Error(err)
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Removed: &xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.LedgerKeyAccount{
				AccountId: accountID,
			},
		},
	}
	accountEntry, err := getAccountEntry(&change)
	if err != nil {
		t.Error(err)
	}
	if accountEntry != nil {
		t.Fatal("got account entry non-nil for removal, want account entry nil")
	}
}

func TestGetAccountEntryNotRemoved(t *testing.T) {
	wantAddress := "GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF"
	accountID, err := xdr.AddressToAccountId(wantAddress)
	if err != nil {
		t.Error(err)
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					AccountId: accountID,
				},
			},
		},
	}
	accountEntry, err := getAccountEntry(&change)
	if err != nil {
		t.Error(err)
	}
	gotAddress := accountEntry.AccountId.Address()
	if gotAddress != wantAddress {
		t.Fatalf("got address %s, want address %s", gotAddress, wantAddress)
	}
}

func TestGetBalanceNotChanged(t *testing.T) {
	wantBalance := uint32(999)
	state := accountState{
		balance: wantBalance,
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Removed: &xdr.LedgerKey{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &xdr.LedgerKeyAccount{},
		},
	}
	gotBalance, err := getBalance(&state, &change)
	if err != nil {
		t.Error(err)
	}
	if gotBalance != wantBalance {
		t.Fatalf("got balance %d, want balance %d", gotBalance, wantBalance)
	}

}

func TestGetBalanceChanged(t *testing.T) {
	originalBalance := uint32(111)
	wantBalance := uint32(222)
	state := accountState{
		balance: originalBalance,
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					Balance: xdr.Int64(wantBalance),
				},
			},
		},
	}
	gotBalance, err := getBalance(&state, &change)
	if err != nil {
		t.Error(err)
	}
	if gotBalance != wantBalance {
		t.Fatalf("got balance %d, want balance %d", gotBalance, wantBalance)
	}
}

func TestGetSignersNotAccount(t *testing.T) {
	wantSigners := []signer{}
	wantSigners = append(wantSigners, signer{address: "GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF", weight: uint32(1)})
	state := accountState{
		signers: wantSigners,
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Removed: &xdr.LedgerKey{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &xdr.LedgerKeyAccount{},
		},
	}
	gotSigners, err := getSigners(&state, &change)
	if err != nil {
		t.Error(err)
	}

	if !assert.Equal(t, gotSigners, wantSigners) {
		t.Fatalf("got signers %v, want signers %v", gotSigners, wantSigners)
	}
}

func TestGetSignersNotChanged(t *testing.T) {
	wantAddress := "GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF"
	wantSigners := []signer{}
	wantSigners = append(wantSigners, signer{address: wantAddress, weight: uint32(1)})

	xdrSigners := []xdr.Signer{}
	signerKeyPtr := &xdr.SignerKey{}
	err := signerKeyPtr.SetAddress(wantAddress)
	if err != nil {
		t.Error(err)
	}
	xdrSigner := xdr.Signer{
		Key:    *signerKeyPtr,
		Weight: xdr.Uint32(1),
	}
	xdrSigners = append(xdrSigners, xdrSigner)

	state := accountState{
		signers: wantSigners,
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{
					Signers: xdrSigners,
				},
			},
		},
	}
	gotSigners, err := getSigners(&state, &change)
	if err != nil {
		t.Error(err)
	}

	if !assert.Equal(t, gotSigners, wantSigners) {
		t.Fatalf("got signers %v, want signers %v", gotSigners, wantSigners)
	}
}

func TestGetSignersChanged(t *testing.T) {
	originalAddress := "GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF"
	originalSigners := []signer{}
	originalSigners = append(originalSigners, signer{address: originalAddress, weight: uint32(1)})

	state := accountState{
		signers: originalSigners,
	}
	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type:    xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{},
			},
		},
	}
	gotSigners, err := getSigners(&state, &change)
	if err != nil {
		t.Error(err)
	}
	if gotSigners != nil {
		t.Fatalf("got signers %v, want signers nil", gotSigners)
	}
}

func TestGetTrustlinesNotTrustline(t *testing.T) {
	wantTrustlines := make(map[string]trustline)
	asset := "USD"
	newTrustline := trustline{
		asset:   asset,
		balance: uint32(10),
		limit:   uint32(100),
	}
	wantTrustlines[asset] = newTrustline

	state := accountState{
		trustlines: wantTrustlines,
	}

	change := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
		State: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type:    xdr.LedgerEntryTypeAccount,
				Account: &xdr.AccountEntry{},
			},
		},
	}
	gotTrustlines, err := getTrustlines(&state, &change)
	if err != nil {
		t.Error(err)
	}
	if !assert.Equal(t, gotTrustlines, wantTrustlines) {
		t.Fatalf("got trustlines %v, want trustlines %v", gotTrustlines, wantTrustlines)
	}
}

// func TestGetTrustlinesRemoved(t *testing.T) {
// 	originalTrustlines := make(map[string]trustline)
// 	assetCode := "USD"
// 	assetIssuer := "GBDT3K42LOPSHNAEHEJ6AVPADIJ4MAR64QEKKW2LQPBSKLYD22KUEH4P"
// 	newTrustline := trustline{
// 		asset:   assetCode,
// 		balance: uint32(10),
// 		limit:   uint32(100),
// 	}
// 	originalTrustlines[asset] = newTrustline

// 	// wantAddress := "GBFLTCDLOE6YQ74B66RH3S2UW5I2MKZ5VLTM75F4YMIWUIXRIFVNRNIF"
// 	assetIssuerAccountID, err := xdr.AddressToAccountId(assetIssuer)
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	change := xdr.LedgerEntryChange{
// 		Type: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
// 		Removed: &xdr.LedgerKey{
// 			Type: xdr.LedgerEntryTypeTrustline,
// 			Trustline: xdr.LedgerKeyTrustLine{
// 				AccountId: assetIssuerAccountID,
// 				Asset:     xdr.MustNewCreditAsset(assetCode, assetIssuer),
// 			},
// 		},
// 	}
// }
