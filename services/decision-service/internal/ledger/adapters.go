package ledger

import (
	"context"
	"errors"

	"github.com/munisp/cmscrush/services/decision-service/internal/decision"
)

// Poster abstracts the TigerBeetle client. Implementations must submit two-sided,
// idempotent financial-control postings. A posting does not authorize payment or settlement.
type Poster interface {
	Post(ctx context.Context, intent decision.LedgerPostingIntent) error
}

// ValidateIntent protects the control ledger boundary from automatic reserve holds.
func ValidateIntent(intent decision.LedgerPostingIntent) error {
	if intent.Amount < 0 || intent.Currency != "USD" {
		return errors.New("ledger intent has invalid amount or currency")
	}
	if intent.DebitAccount == "" || intent.CreditAccount == "" || intent.DebitAccount == intent.CreditAccount {
		return errors.New("ledger intent must specify distinct debit and credit accounts")
	}
	if intent.Kind == "RESERVE_HOLD" && (!intent.Authorization.HumanApproved || intent.Authorization.ApproverID == nil) {
		return errors.New("reserve hold requires a recorded human approval")
	}
	return nil
}

// NoopPoster is intentionally the development default. Production selects a pinned
// TigerBeetle client implementation and records its cluster/ledger configuration as evidence.
type NoopPoster struct{}

func (NoopPoster) Post(_ context.Context, intent decision.LedgerPostingIntent) error {
	return ValidateIntent(intent)
}
