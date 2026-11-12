package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

var Types = []string{"UserRegistered", "UserLoggedIn", "PageViewed", "ProductViewed", "CartUpdated", "CheckoutStarted", "PaymentAttempted", "PaymentSucceeded", "PaymentFailed", "OrderCreated", "OrderCompleted", "ServiceLatencyRecorded", "ApplicationErrorRecorded"}
var uuid = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
var currency = regexp.MustCompile(`^[A-Z]{3}$`)
var identifier = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,128}$`)

type Payload struct {
	UserID        string `json:"user_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	ProductID     string `json:"product_id,omitempty"`
	OrderID       string `json:"order_id,omitempty"`
	AmountMinor   int64  `json:"amount_minor,omitempty"`
	Currency      string `json:"currency,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
	Service       string `json:"service,omitempty"`
	LatencyMS     int    `json:"latency_ms,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	Quantity      int    `json:"quantity,omitempty"`
	Page          string `json:"page,omitempty"`
	Padding       string `json:"padding,omitempty"`
}
type Event struct {
	ID          string            `json:"event_id"`
	Tenant      string            `json:"tenant_id"`
	Type        string            `json:"event_type"`
	Version     int               `json:"schema_version"`
	Key         string            `json:"partition_key"`
	Producer    string            `json:"producer_id"`
	Time        time.Time         `json:"event_timestamp"`
	Ingested    time.Time         `json:"ingestion_timestamp,omitempty"`
	Trace       string            `json:"trace_id,omitempty"`
	Correlation string            `json:"correlation_id,omitempty"`
	Payload     Payload           `json:"payload"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func Decode(b []byte) (Event, error) {
	var e Event
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&e); err != nil {
		return e, fmt.Errorf("invalid JSON: %w", err)
	}
	if d.Decode(new(any)) != io.EOF {
		return e, errors.New("multiple JSON values")
	}
	return e, e.Validate()
}
func (e Event) PartitionKey() string { return e.Tenant + "/" + e.Key }
func (e Event) Validate() error {
	if !uuid.MatchString(e.ID) {
		return errors.New("invalid event_id UUID")
	}
	if !identifier.MatchString(e.Tenant) || !identifier.MatchString(e.Key) || !identifier.MatchString(e.Producer) {
		return errors.New("invalid tenant, partition_key or producer_id")
	}
	if e.Version != 1 {
		return errors.New("unsupported schema_version")
	}
	if e.Time.IsZero() || e.Time.Year() < 2020 || e.Time.Year() > 2100 {
		return errors.New("invalid event_timestamp")
	}
	if len(e.Metadata) > 16 || len(e.Correlation) > 128 || len(e.Trace) > 64 || len(e.Payload.Padding) > 16384 {
		return errors.New("metadata or padding too large")
	}
	for k, v := range e.Metadata {
		if len(k) > 64 || len(v) > 256 {
			return errors.New("metadata too large")
		}
	}
	p := e.Payload
	if p.AmountMinor < 0 || p.AmountMinor > 1_000_000_000_000 || p.LatencyMS < 0 || p.LatencyMS > 60000 || p.Quantity < 0 || p.Quantity > 10000 {
		return errors.New("payload numeric value outside supported range")
	}
	for _, s := range []string{p.UserID, p.SessionID, p.ProductID, p.OrderID, p.Currency, p.FailureReason, p.Service, p.ErrorCode, p.Page} {
		if len(s) > 256 {
			return errors.New("payload string too large")
		}
	}
	switch e.Type {
	case "UserRegistered", "UserLoggedIn":
		if p.UserID == "" {
			return errors.New("user_id required")
		}
	case "PageViewed":
		if p.SessionID == "" || p.Page == "" {
			return errors.New("session_id and page required")
		}
	case "ProductViewed", "CartUpdated":
		if p.ProductID == "" || p.SessionID == "" {
			return errors.New("product_id and session_id required")
		}
	case "CheckoutStarted", "OrderCreated", "OrderCompleted", "PaymentAttempted", "PaymentSucceeded", "PaymentFailed":
		if p.OrderID == "" {
			return errors.New("order_id required")
		}
		if e.Type == "PaymentSucceeded" || e.Type == "PaymentAttempted" {
			if !currency.MatchString(p.Currency) {
				return errors.New("ISO currency required")
			}
		}
		if e.Type == "PaymentFailed" && p.FailureReason == "" {
			return errors.New("failure_reason required")
		}
	case "ServiceLatencyRecorded":
		if p.Service == "" {
			return errors.New("service required")
		}
	case "ApplicationErrorRecorded":
		if p.Service == "" || p.ErrorCode == "" {
			return errors.New("service and error_code required")
		}
	default:
		return errors.New("unsupported event_type")
	}
	return nil
}
