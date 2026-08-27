package payments

import (
	"context"
	"errors"
	"strings"
)

// SettlementCommand is deliberately not constructible from a DecisionRecord. It is
// created only by the case workflow after external payment authorization.
type SettlementCommand struct {
	TenantID         string
	CaseUID          string
	AuthorizedBy     string
	AuthorizationRef string
	AmountMinor      int64
	Currency         string
	PayeeAlias       string
}

// NetworkAdapter represents the optional Mojaloop payment-interoperability integration.
type NetworkAdapter interface {
	SubmitAuthorizedSettlement(ctx context.Context, command SettlementCommand) error
}

func ValidateSettlement(command SettlementCommand) error {
	if strings.TrimSpace(command.TenantID) == "" || strings.TrimSpace(command.CaseUID) == "" {
		return errors.New("settlement requires tenant and case identifiers")
	}
	if strings.TrimSpace(command.AuthorizedBy) == "" || strings.TrimSpace(command.AuthorizationRef) == "" {
		return errors.New("settlement requires external human authorization evidence")
	}
	if command.AmountMinor <= 0 || command.Currency != "USD" || strings.TrimSpace(command.PayeeAlias) == "" {
		return errors.New("settlement command is incomplete")
	}
	return nil
}

// DisabledAdapter is the default. It makes accidental settlement impossible in all
// environments until feature policy and payment-network credentials are provisioned.
type DisabledAdapter struct{}

func (DisabledAdapter) SubmitAuthorizedSettlement(_ context.Context, command SettlementCommand) error {
	if err := ValidateSettlement(command); err != nil {
		return err
	}
	return errors.New("mojaloop settlement adapter is disabled by platform policy")
}
