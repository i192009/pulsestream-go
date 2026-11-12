package store

import (
	"bytes"
	"testing"
)

func TestCanonicalChecksumPreservesInt64(t *testing.T) {
	a, err := Canonical([]byte(`{"revenue":9007199254740992,"count":1}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Canonical([]byte(`{"count":1,"revenue":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	if Hash(a) == Hash(b) {
		t.Fatal("checksum lost integer precision")
	}
	c, err := Canonical([]byte(`{ "count" : 1, "revenue": 9007199254740992 }`))
	if err != nil || !bytes.Equal(a, c) {
		t.Fatal("canonical order/whitespace", string(a), string(c), err)
	}
}
