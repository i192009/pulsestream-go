package event_test

import (
	"encoding/json"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/synthetic"
	"strings"
	"testing"
	"time"
)

func TestSchemas(t *testing.T) {
	g := synthetic.New(42, time.Now())
	seen := map[string]bool{}
	for i := int64(0); i < 1000; i++ {
		e := g.Event(i, "tenant-demo")
		if err := e.Validate(); err != nil {
			t.Fatal(e.Type, err)
		}
		seen[e.Type] = true
		b, _ := json.Marshal(e)
		if _, err := event.Decode(b); err != nil {
			t.Fatal(err)
		}
		if e.PartitionKey() != "tenant-demo/"+e.Key {
			t.Fatal("tenant key lost")
		}
	}
	if len(seen) != len(event.Types) {
		t.Fatal("generator does not cover schemas")
	}
}
func TestStrictValidation(t *testing.T) {
	e := synthetic.New(42, time.Now()).Event(1, "tenant-demo")
	b, _ := json.Marshal(e)
	for _, raw := range []string{string(b[:len(b)-1]) + `,"unknown":1}`, string(b) + ` {}`, `{"event_id":"bad"}`, strings.Replace(string(b), `"schema_version":1`, `"schema_version":99`, 1)} {
		if _, err := event.Decode([]byte(raw)); err == nil {
			t.Fatal("accepted invalid input", raw)
		}
	}
	e.Type = "PaymentSucceeded"
	e.Payload = event.Payload{OrderID: "x", Currency: "USD", AmountMinor: -1}
	if e.Validate() == nil {
		t.Fatal("negative payment")
	}
}
