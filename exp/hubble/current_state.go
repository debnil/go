package hubble

import (
	"fmt"

	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// TODO: Maybe define custom structs in place of the XDR ones
// for easier serialization.
type accountState struct {
	address               string
	lastModifiedLedgerSeq uint32
	balance               int64
	signers               []xdr.Signer
	trustlines            map[string]trustline
	// TODO: May want to track other fields in AccountEntry.
	// TODO: Track offers.
	// TODO: Track data.
}

type trustline struct {
	asset      string
	balance    int64
	limit      int64
	authorized bool
	// TODO: Add liabilities.
}

// TODO: Do not hand roll serialization.
func (state *accountState) String() string {
	returnStr := "{\n"
	returnStr += fmt.Sprintf("\taddress: %s\n", state.address)
	returnStr += fmt.Sprintf("\tlastModifiedLedgerSeq: %d\n", state.lastModifiedLedgerSeq)
	returnStr += fmt.Sprintf("\tbalance: %d\n", state.balance)
	returnStr += fmt.Sprintf("\tsigners: {\n")
	for _, signer := range state.signers {
		returnStr += fmt.Sprintf("\t\tsig: key %s, weight %d\n", signer.Key.Address(), signer.Weight)
	}
	returnStr += "\t}\n"
	returnStr += fmt.Sprintf("\ttrustlines: {\n")
	for _, trustline := range state.trustlines {
		returnStr += fmt.Sprintf("\t\ttrustline: %v\n", trustline)
	}
	returnStr += "\t}\n}\n"
	return returnStr
}

func (state *accountState) updateAccountState(change xdr.LedgerEntryChange) error {
	// TODO: Do not assume LEDGER_ENTRY_STATE type entry change.
	// We can assume this now because the SingleLedgerStateReader only writes
	// `xdr.LedgerEntryChange` structs of this type.
	err := state.setID(change)
	if err != nil {
		return errors.Wrap(err, "could not set initial account id")
	}
	err = state.setLedgerSeq(change)
	if err != nil {
		return errors.Wrap(err, "could not set initial seqnum")
	}
	err = state.setBalance(change)
	if err != nil {
		return errors.Wrap(err, "could not set balance")
	}
	err = state.setSigners(change)
	if err != nil {
		return errors.Wrap(err, "could not set signers")
	}
	err = state.updateTrustlines(change)
	if err != nil {
		return errors.Wrap(err, "could not update trustlines")
	}
	// TODO: Update data, offers, trustlines.
	return nil
}

func (state *accountState) setLedgerSeq(change xdr.LedgerEntryChange) error {
	var seqnum xdr.Uint32
	switch entryType := change.Type; entryType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		seqnum = change.MustCreated().LastModifiedLedgerSeq
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		seqnum = change.MustUpdated().LastModifiedLedgerSeq
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		seqnum = change.MustState().LastModifiedLedgerSeq

	// We do not need to update the seqnum for removed changes, because
	// we just remove the accompanying account's state.
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return nil
	default:
		panic(fmt.Errorf("Unknown entry type: %v", entryType))
	}
	state.lastModifiedLedgerSeq = uint32(seqnum)
	return nil
}

func (state *accountState) setID(change xdr.LedgerEntryChange) error {
	accountID, err := getAccountID(change)
	if err != nil {
		return err
	}
	state.address = accountID.Address()
	return nil
}

func getAccountID(change xdr.LedgerEntryChange) (xdr.AccountId, error) {
	key := change.LedgerKey()
	var accountID xdr.AccountId
	switch keyType := key.Type; keyType {
	case xdr.LedgerEntryTypeAccount:
		return key.MustAccount().AccountId, nil
	case xdr.LedgerEntryTypeTrustline:
		return key.MustTrustLine().AccountId, nil
	case xdr.LedgerEntryTypeOffer:
		return key.MustOffer().SellerId, nil
	case xdr.LedgerEntryTypeData:
		return key.MustData().AccountId, nil
	default:
		return accountID, fmt.Errorf("Unknown entry type: %v", keyType)
	}
}

func (state *accountState) setBalance(change xdr.LedgerEntryChange) error {
	account, err := getAccountEntry(change)
	if err != nil {
		return err
	}
	if account == nil {
		return nil
	}
	state.balance = int64(account.Balance)
	return nil
}

func (state *accountState) setSigners(change xdr.LedgerEntryChange) error {
	account, err := getAccountEntry(change)
	if err != nil {
		return err
	}
	if account == nil {
		return nil
	}
	state.signers = account.Signers
	return nil
}

func getAccountEntry(change xdr.LedgerEntryChange) (*xdr.AccountEntry, error) {
	if change.EntryType() != xdr.LedgerEntryTypeAccount {
		return nil, nil
	}
	var account xdr.AccountEntry
	switch entryType := change.Type; entryType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		account = change.MustCreated().Data.MustAccount()
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		account = change.MustUpdated().Data.MustAccount()
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		account = change.MustState().Data.MustAccount()
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return nil, nil
	default:
		return nil, fmt.Errorf("Unknown entry type: %v", entryType)
	}
	return &account, nil
}

func (state *accountState) updateTrustlines(change xdr.LedgerEntryChange) error {
	if change.EntryType() != xdr.LedgerEntryTypeTrustline {
		return nil
	}

	if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryRemoved {
		asset := change.MustRemoved().TrustLine.Asset.String()
		state.trustlines[asset] = nil
		return nil
	}

	var trustlineEntry xdr.TrustLineEntry
	switch entryType := change.Type; entryType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		trustlineEntry = change.MustCreated().Data.MustTrustLine()
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		trustlineEntry = change.MustUpdated().Data.MustTrustLine()
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		trustlineEntry = change.MustState().Data.MustTrustLine()
	default:
		return fmt.Errorf("Unknown entry type: %v", entryType)
	}

	assetKey := trustlineEntry.Asset.String()
	newTrustline := trustline{
		asset:      assetKey,
		balance:    int64(trustlineEntry.Balance),
		limit:      int64(trustlineEntry.Limit),
		authorized: (trustlineEntry.Flags != 0),
	}

	state.trustlines[assetKey] = newTrustline
	return nil
}
