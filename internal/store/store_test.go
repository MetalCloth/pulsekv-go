package store

import (
	"testing"
	"time"
)

func TestSetClearsPreviousExpiry(t *testing.T) {
	s := New()
	deadline := time.Now().Add(20 * time.Millisecond)
	if ok, err := s.Set("key", []byte("old"), &deadline, false, false); err != nil || !ok {
		t.Fatalf("initial set: %v %v", ok, err)
	}
	if ok, err := s.Set("key", []byte("new"), nil, false, false); err != nil || !ok {
		t.Fatalf("overwrite: %v %v", ok, err)
	}
	time.Sleep(35 * time.Millisecond)
	value, ok, err := s.Get("key")
	if err != nil || !ok || string(value) != "new" {
		t.Fatalf("overwritten value expired: %q %v %v", value, ok, err)
	}
}

func TestListAndStreamSemantics(t *testing.T) {
	s := New()
	if _, err := s.ListPush("list", []string{"a", "b", "c"}, false); err != nil {
		t.Fatal(err)
	}
	values, err := s.ListRange("list", -2, -1)
	if err != nil || len(values) != 2 || values[0] != "b" || values[1] != "c" {
		t.Fatalf("negative list range: %#v %v", values, err)
	}
	first, err := s.XAdd("stream", "100-*", []string{"field", "value"})
	if err != nil || first.ID.String() != "100-0" {
		t.Fatalf("stream auto ID: %#v %v", first, err)
	}
	second, err := s.XAdd("stream", "*", []string{"other", "value"})
	if err != nil || second.ID.Compare(first.ID) <= 0 {
		t.Fatalf("stream monotonic ID: %#v %v", second, err)
	}
	if _, err := s.XAdd("stream", "1-0", []string{"x", "y"}); err == nil {
		t.Fatal("expected decreasing stream ID to fail")
	}
}

func TestBitmapsAndSortedSets(t *testing.T) {
	s := New()
	if old, err := s.SetBit("bits", 9, 1); err != nil || old != 0 {
		t.Fatalf("setbit: %d %v", old, err)
	}
	if bit, err := s.GetBit("bits", 9); err != nil || bit != 1 {
		t.Fatalf("getbit: %d %v", bit, err)
	}
	if count, err := s.BitCount("bits", nil, nil); err != nil || count != 1 {
		t.Fatalf("bitcount: %d %v", count, err)
	}
	if added, err := s.ZAdd("scores", []SortedMember{{"b", 2}, {"a", 1}}); err != nil || added != 2 {
		t.Fatalf("zadd: %d %v", added, err)
	}
	rangeValues, err := s.ZRange("scores", 0, -1)
	if err != nil || len(rangeValues) != 2 || rangeValues[0].Member != "a" {
		t.Fatalf("zrange: %#v %v", rangeValues, err)
	}
}
