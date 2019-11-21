package hubble

import (
	"fmt"

	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type accountState struct {
	seqnum     uint32
	balance    int64
	signers    []signer
	trustlines map[string]trustline
	offers     map[int64]offer
	data       map[string][]byte
	// TODO: May want to track other fields in AccountEntry.
}

// TODO: Determine if it's easiest to use custom structs,
// goxdr structs, or the original XDRs. Custom structs have been
// chosen so we can use basic types, enabling easiest serialization.
type signer struct {
	address string
	weight  uint32
}

type trustline struct {
	asset      string
	balance    int64
	limit      int64
	authorized bool
	// TODO: Add liabilities.
}

type offer struct {
	id         int64
	seller     string // seller address
	selling    string // selling asset
	buying     string // buying asset
	amount     int64
	priceNum   int32
	priceDenom int32
	// TODO: Add flags.
}

func (state *accountState) updateAccountState(change xdr.LedgerEntryChange) error {
	// TODO: Properly handle Removed Account changes.
	err := state.setSeqnum(change)
	if err != nil {
		return errors.Wrap(err, "could not set seqnum")
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
	err = state.updateOffers(change)
	if err != nil {
		return errors.Wrap(err, "could not update offers")
	}
	err = state.updateData(change)
	if err != nil {
		return errors.Wrap(err, "could not update data")
	}
	return nil
}

func (state *accountState) setSeqnum(change xdr.LedgerEntryChange) error {
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
	state.seqnum = uint32(seqnum)
	return nil
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

	// TODO: Check if we need a custom signer struct.
	// TODO: Determine more efficient process to update signers.
	var newSigners []signer
	for _, accountSigner := range account.Signers {
		newSigners = append(newSigners, signer{
			address: accountSigner.Key.Address(),
			weight:  uint32(accountSigner.Weight),
		})
	}
	state.signers = newSigners
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

	// If no trustlines have been added yet, create the
	// map to store trustlines.
	if len(state.trustlines) == 0 {
		state.trustlines = make(map[string]trustline)
	}

	// If the change is of removed type, remove the corresponding trustline.
	if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryRemoved {
		asset := change.MustRemoved().TrustLine.Asset.String()
		if _, ok := state.trustlines[asset]; ok {
			delete(state.trustlines, asset)
		}
		return nil
	}

	// Get and store the trustline.
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

	// TODO: Check if we need a custom struct for trustlines.
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

func (state *accountState) updateOffers(change xdr.LedgerEntryChange) error {
	if change.EntryType() != xdr.LedgerEntryTypeOffer {
		return nil
	}

	// If no offers have been added yet, create the
	// map to store offers.
	if len(state.offers) == 0 {
		state.offers = make(map[int64]offer)
	}

	// If the change is of removed type, remove the corresponding offer.
	if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryRemoved {
		id := int64(change.MustRemoved().Offer.OfferId)
		if _, ok := state.offers[id]; ok {
			delete(state.offers, id)
		}
		return nil
	}

	// Get and store the offer.
	var offerEntry xdr.OfferEntry
	switch entryType := change.Type; entryType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		offerEntry = change.MustCreated().Data.MustOffer()
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		offerEntry = change.MustUpdated().Data.MustOffer()
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		offerEntry = change.MustState().Data.MustOffer()
	default:
		return fmt.Errorf("Unknown entry type: %v", entryType)
	}

	// TODO: Check if we need a custom struct for offers.
	offerID := int64(offerEntry.OfferId)
	newOffer := offer{
		id:         offerID,
		seller:     offerEntry.SellerId.Address(),
		selling:    offerEntry.Selling.String(),
		buying:     offerEntry.Buying.String(),
		amount:     int64(offerEntry.Amount),
		priceNum:   int32(offerEntry.Price.N),
		priceDenom: int32(offerEntry.Price.D),
	}
	state.offers[offerID] = newOffer
	return nil
}

func (state *accountState) updateData(change xdr.LedgerEntryChange) error {
	if change.EntryType() != xdr.LedgerEntryTypeData {
		return nil
	}

	// If no data key-value pairs have been added yet, create the
	// map to store data.
	if len(state.data) == 0 {
		state.data = make(map[string][]byte)
	}

	// If the change is of Removed type, remove the corresponding data.
	if change.Type == xdr.LedgerEntryChangeTypeLedgerEntryRemoved {
		name := string(change.MustRemoved().Data.DataName)
		if _, ok := state.data[name]; ok {
			delete(state.data, name)
		}
		return nil
	}

	// Get and store the data key-value pair.
	var dataEntry xdr.DataEntry
	switch entryType := change.Type; entryType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		dataEntry = change.MustCreated().Data.MustData()
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		dataEntry = change.MustUpdated().Data.MustData()
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		dataEntry = change.MustState().Data.MustData()
	default:
		return fmt.Errorf("Unknown entry type: %v", entryType)
	}
	state.data[string(dataEntry.DataName)] = dataEntry.DataValue
	return nil
}
