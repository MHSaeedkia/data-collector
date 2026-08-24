// Package domain holds the vocabulary of the service: what a market subscription is,
// which states it can be in, and which transitions this service is allowed to make.
package domain

import "fmt"

// Status mirrors the postgres enum `subscription_status` (postgres/01_schema.sql).
// The values are the enum labels verbatim — they are written straight back to the
// column, so they must not be prettified here.
type Status string

const (
	Subscribed         Status = "subscribe"
	Unsubscribed       Status = "unsubscribe"
	PendingSubscribe   Status = "pending-subscribe"
	PendingUnsubscribe Status = "pending-unsubscribe"
)

// Action is what an operator asked for. It is deliberately NOT a Status: an operator
// requests "subscribe", and what gets written is the PENDING form of it.
type Action string

const (
	ActionSubscribe   Action = "subscribe"
	ActionUnsubscribe Action = "unsubscribe"
)

// ParseAction validates a request's action, so an unknown string can never reach the
// NiFi URL builder or the status column.
func ParseAction(s string) (Action, error) {
	switch Action(s) {
	case ActionSubscribe:
		return ActionSubscribe, nil
	case ActionUnsubscribe:
		return ActionUnsubscribe, nil
	default:
		return "", fmt.Errorf("unknown action %q (want %q or %q)", s, ActionSubscribe, ActionUnsubscribe)
	}
}

// Pending is the status this service writes when an operator requests the action.
//
// This service NEVER writes a settled status. It writes the pending form, asks NiFi to
// do the work, and NiFi writes `subscribe`/`unsubscribe` back to the row once the feed
// is really on or off. A row sitting in a pending state therefore means "NiFi has been
// asked and has not confirmed yet" — a real, observable state, not a UI artifact.
func (a Action) Pending() Status {
	if a == ActionSubscribe {
		return PendingSubscribe
	}
	return PendingUnsubscribe
}

// Subscription is one row of exchange_markets joined to its exchange.
type Subscription struct {
	ID            int64  `json:"id"`
	ExchangeID    int64  `json:"exchange_id"`
	ExchangeName  string `json:"exchange_name"`
	ExchangeLabel string `json:"exchange_label"`
	Market        string `json:"market"`
	MarketID      int64  `json:"market_id"`
	Status        Status `json:"status"`
}

// Exchange is one row of the exchanges table, for the UI's filter list.
type Exchange struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Label string `json:"label"`
}

// Result is the per-row outcome of a bulk request. Every requested id gets one, so the
// UI can show exactly which markets moved and which did not.
type Result struct {
	ID     int64  `json:"id"`
	Market string `json:"market"`
	Status Status `json:"status"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
}
