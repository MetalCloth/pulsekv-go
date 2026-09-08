// Package store contains the synchronized in-memory data model.  Commands
// never reach into the maps directly; this keeps expiry, type checks, version
// tracking, and wake-ups consistent across all command paths.
package store

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrWrongType  = errors.New("WRONGTYPE operation against a key holding the wrong kind of value")
	ErrNoKey      = errors.New("no such key")
	ErrInvalidID  = errors.New("invalid stream ID")
	ErrIDTooSmall = errors.New("stream ID is equal or smaller than the last entry")
	ErrZeroID     = errors.New("stream ID must be greater than 0-0")
)

type Type string

const (
	TypeNone   Type = "none"
	TypeString Type = "string"
	TypeList   Type = "list"
	TypeStream Type = "stream"
	TypeZSet   Type = "zset"
)

type StreamID struct {
	Ms  uint64
	Seq uint64
}

func (id StreamID) String() string { return fmt.Sprintf("%d-%d", id.Ms, id.Seq) }

func (id StreamID) Compare(other StreamID) int {
	if id.Ms < other.Ms || id.Ms == other.Ms && id.Seq < other.Seq {
		return -1
	}
	if id == other {
		return 0
	}
	return 1
}

type StreamEntry struct {
	ID     StreamID
	Fields []string // field/value pairs, kept ordered for deterministic replies
}

type StreamReadRequest struct {
	Key   string
	Start StreamID
}

type SortedMember struct {
	Member string
	Score  float64
}

type GeoLocation struct {
	Longitude float64
	Latitude  float64
	Score     float64
}

type Store struct {
	mu       sync.RWMutex
	strings  map[string][]byte
	lists    map[string][]string
	streams  map[string][]StreamEntry
	sorted   map[string]map[string]float64
	geos     map[string]map[string]GeoLocation
	expires  map[string]time.Time
	versions map[string]uint64
	change   chan struct{}
}

func New() *Store {
	return &Store{
		strings:  make(map[string][]byte),
		lists:    make(map[string][]string),
		streams:  make(map[string][]StreamEntry),
		sorted:   make(map[string]map[string]float64),
		geos:     make(map[string]map[string]GeoLocation),
		expires:  make(map[string]time.Time),
		versions: make(map[string]uint64),
		change:   make(chan struct{}),
	}
}

func (s *Store) touchLocked(key string) {
	s.versions[key]++
	close(s.change)
	s.change = make(chan struct{})
}

func (s *Store) purgeExpiredLocked(key string, now time.Time) bool {
	deadline, ok := s.expires[key]
	if !ok || deadline.After(now) {
		return false
	}
	delete(s.expires, key)
	delete(s.strings, key)
	delete(s.lists, key)
	delete(s.streams, key)
	delete(s.sorted, key)
	delete(s.geos, key)
	s.touchLocked(key)
	return true
}

func (s *Store) kindLocked(key string) Type {
	if _, ok := s.strings[key]; ok {
		return TypeString
	}
	if _, ok := s.lists[key]; ok {
		return TypeList
	}
	if _, ok := s.streams[key]; ok {
		return TypeStream
	}
	if _, ok := s.sorted[key]; ok {
		return TypeZSet
	}
	return TypeNone
}

func (s *Store) Type(key string) Type {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	return s.kindLocked(key)
}

func (s *Store) Exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	return s.kindLocked(key) != TypeNone
}

func (s *Store) Get(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if _, ok := s.strings[key]; !ok {
		if s.kindLocked(key) != TypeNone {
			return nil, false, ErrWrongType
		}
		return nil, false, nil
	}
	return append([]byte(nil), s.strings[key]...), true, nil
}

func (s *Store) Set(key string, value []byte, deadline *time.Time, onlyIfAbsent, onlyIfPresent bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	kind := s.kindLocked(key)
	if kind != TypeNone && kind != TypeString {
		return false, ErrWrongType
	}
	present := kind == TypeString
	if onlyIfAbsent && present || onlyIfPresent && !present {
		return false, nil
	}
	s.strings[key] = append([]byte(nil), value...)
	delete(s.expires, key)
	if deadline != nil {
		s.expires[key] = *deadline
	}
	s.touchLocked(key)
	return true, nil
}

func (s *Store) Delete(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	for _, key := range keys {
		s.purgeExpiredLocked(key, time.Now())
		if s.kindLocked(key) == TypeNone {
			continue
		}
		delete(s.strings, key)
		delete(s.lists, key)
		delete(s.streams, key)
		delete(s.sorted, key)
		delete(s.geos, key)
		delete(s.expires, key)
		s.touchLocked(key)
		deleted++
	}
	return deleted
}

func (s *Store) Incr(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if s.kindLocked(key) != TypeNone && s.kindLocked(key) != TypeString {
		return 0, ErrWrongType
	}
	var current int64
	if value, ok := s.strings[key]; ok {
		var err error
		current, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, errors.New("value is not an integer or out of range")
		}
	}
	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return 0, errors.New("increment or decrement would overflow")
	}
	current += delta
	s.strings[key] = []byte(strconv.FormatInt(current, 10))
	s.touchLocked(key)
	return current, nil
}

func (s *Store) MGet(keys ...string) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([][]byte, len(keys))
	for i, key := range keys {
		s.purgeExpiredLocked(key, time.Now())
		if s.kindLocked(key) == TypeNone {
			continue
		}
		if value, ok := s.strings[key]; ok {
			result[i] = append([]byte(nil), value...)
		} else {
			return nil, ErrWrongType
		}
	}
	return result, nil
}

func (s *Store) MSet(pairs [][2]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pair := range pairs {
		s.purgeExpiredLocked(pair[0], time.Now())
		if kind := s.kindLocked(pair[0]); kind != TypeNone && kind != TypeString {
			return ErrWrongType
		}
	}
	for _, pair := range pairs {
		s.strings[pair[0]] = []byte(pair[1])
		delete(s.expires, pair[0])
		s.touchLocked(pair[0])
	}
	return nil
}

func (s *Store) Expire(key string, duration time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if s.kindLocked(key) == TypeNone {
		return false, nil
	}
	s.expires[key] = time.Now().Add(duration)
	s.touchLocked(key)
	return true, nil
}

func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if _, ok := s.expires[key]; !ok || s.kindLocked(key) == TypeNone {
		return false
	}
	delete(s.expires, key)
	s.touchLocked(key)
	return true
}

func (s *Store) TTL(key string, precision time.Duration) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if s.kindLocked(key) == TypeNone {
		return -2
	}
	deadline, ok := s.expires[key]
	if !ok {
		return -1
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		delete(s.expires, key)
		delete(s.strings, key)
		delete(s.lists, key)
		delete(s.streams, key)
		delete(s.sorted, key)
		delete(s.geos, key)
		s.touchLocked(key)
		return -2
	}
	return int64(remaining / precision)
}

func (s *Store) Change() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.change
}

func (s *Store) ListPush(key string, values []string, left bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeList {
		return 0, ErrWrongType
	}
	if left {
		copyValues := append([]string(nil), values...)
		slicesReverse(copyValues)
		s.lists[key] = append(copyValues, s.lists[key]...)
	} else {
		s.lists[key] = append(s.lists[key], values...)
	}
	s.touchLocked(key)
	return len(s.lists[key]), nil
}

func slicesReverse(values []string) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func (s *Store) ListRange(key string, start, stop int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeList {
		return nil, ErrWrongType
	}
	values := s.lists[key]
	start, stop, ok := normalizeRange(len(values), start, stop)
	if !ok {
		return []string{}, nil
	}
	return append([]string(nil), values[start:stop+1]...), nil
}

func (s *Store) ListLen(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeList {
		return 0, ErrWrongType
	}
	return len(s.lists[key]), nil
}

func (s *Store) ListPop(key string, left bool, count *int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeList {
		return nil, ErrWrongType
	}
	values := s.lists[key]
	if len(values) == 0 {
		return nil, nil
	}
	n := 1
	if count != nil {
		n = *count
		if n < 0 {
			return nil, errors.New("count must be non-negative")
		}
		if n == 0 {
			return []string{}, nil
		}
	}
	if n > len(values) {
		n = len(values)
	}
	result := make([]string, n)
	if left {
		copy(result, values[:n])
		s.lists[key] = append([]string(nil), values[n:]...)
	} else {
		for i := range result {
			result[i] = values[len(values)-1-i]
		}
		s.lists[key] = append([]string(nil), values[:len(values)-n]...)
	}
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}
	s.touchLocked(key)
	return result, nil
}

func (s *Store) XAdd(key, requestedID string, fields []string) (StreamEntry, error) {
	if len(fields) == 0 || len(fields)%2 != 0 {
		return StreamEntry{}, errors.New("XADD requires field-value pairs")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeStream {
		return StreamEntry{}, ErrWrongType
	}
	entries := s.streams[key]
	var last StreamID
	if len(entries) > 0 {
		last = entries[len(entries)-1].ID
	}
	id, err := nextStreamID(requestedID, last)
	if err != nil {
		return StreamEntry{}, err
	}
	entry := StreamEntry{ID: id, Fields: append([]string(nil), fields...)}
	s.streams[key] = append(entries, entry)
	s.touchLocked(key)
	return entry, nil
}

func parseStreamID(value string, end bool) (StreamID, error) {
	if value == "-" {
		return StreamID{0, 0}, nil
	}
	if value == "+" {
		return StreamID{math.MaxUint64, math.MaxUint64}, nil
	}
	parts := strings.Split(value, "-")
	if len(parts) > 2 || parts[0] == "" {
		return StreamID{}, ErrInvalidID
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, ErrInvalidID
	}
	seq := uint64(0)
	if len(parts) == 2 {
		if parts[1] == "" {
			return StreamID{}, ErrInvalidID
		}
		seq, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return StreamID{}, ErrInvalidID
		}
	} else if end {
		seq = math.MaxUint64
	}
	return StreamID{ms, seq}, nil
}

func ParseStreamID(value string, end bool) (StreamID, error) {
	return parseStreamID(value, end)
}

func nextStreamID(requested string, last StreamID) (StreamID, error) {
	if requested == "*" {
		id := StreamID{uint64(time.Now().UnixMilli()), 0}
		if id.Ms == last.Ms {
			id.Seq = last.Seq + 1
		}
		if id.Ms == 0 && id.Seq == 0 {
			id.Seq = 1
		}
		if id.Compare(last) <= 0 {
			id = StreamID{last.Ms, last.Seq + 1}
		}
		return id, nil
	}
	parts := strings.Split(requested, "-")
	if len(parts) == 2 && parts[1] == "*" {
		ms, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return StreamID{}, ErrInvalidID
		}
		seq := uint64(0)
		if ms == last.Ms {
			seq = last.Seq + 1
		} else if ms < last.Ms {
			return StreamID{}, ErrIDTooSmall
		}
		if ms == 0 && seq == 0 {
			seq = 1
		}
		return StreamID{ms, seq}, nil
	}
	id, err := parseStreamID(requested, false)
	if err != nil {
		return StreamID{}, err
	}
	if id.Ms == 0 && id.Seq == 0 {
		return StreamID{}, ErrZeroID
	}
	if id.Compare(last) <= 0 {
		return StreamID{}, ErrIDTooSmall
	}
	return id, nil
}

func (s *Store) XRange(key, start, end string, count int) ([]StreamEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeStream {
		return nil, ErrWrongType
	}
	first, err := parseStreamID(start, false)
	if err != nil {
		return nil, err
	}
	last, err := parseStreamID(end, true)
	if err != nil {
		return nil, err
	}
	result := make([]StreamEntry, 0)
	for _, entry := range s.streams[key] {
		if entry.ID.Compare(first) >= 0 && entry.ID.Compare(last) <= 0 {
			result = append(result, copyStreamEntry(entry))
			if count > 0 && len(result) >= count {
				break
			}
		}
	}
	return result, nil
}

func (s *Store) XRead(requests []StreamReadRequest, count int) []struct {
	Key     string
	Entries []StreamEntry
} {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]struct {
		Key     string
		Entries []StreamEntry
	}, 0, len(requests))
	for _, request := range requests {
		entries := make([]StreamEntry, 0)
		for _, entry := range s.streams[request.Key] {
			if entry.ID.Compare(request.Start) > 0 {
				entries = append(entries, copyStreamEntry(entry))
				if count > 0 && len(entries) >= count {
					break
				}
			}
		}
		if len(entries) > 0 {
			result = append(result, struct {
				Key     string
				Entries []StreamEntry
			}{request.Key, entries})
		}
	}
	return result
}

func (s *Store) StreamLastID(key string) (StreamID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := s.streams[key]
	if len(entries) == 0 {
		return StreamID{}, false
	}
	return entries[len(entries)-1].ID, true
}

func copyStreamEntry(entry StreamEntry) StreamEntry {
	return StreamEntry{ID: entry.ID, Fields: append([]string(nil), entry.Fields...)}
}

func (s *Store) ZAdd(key string, members []SortedMember) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return 0, ErrWrongType
	}
	if s.sorted[key] == nil {
		s.sorted[key] = make(map[string]float64)
	}
	added := 0
	for _, member := range members {
		if _, exists := s.sorted[key][member.Member]; !exists {
			added++
		}
		s.sorted[key][member.Member] = member.Score
	}
	if len(members) > 0 {
		s.touchLocked(key)
	}
	return added, nil
}

func (s *Store) ZRem(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return 0, ErrWrongType
	}
	set := s.sorted[key]
	removed := 0
	for _, member := range members {
		if _, ok := set[member]; ok {
			delete(set, member)
			removed++
		}
	}
	if len(set) == 0 {
		delete(s.sorted, key)
		delete(s.geos, key)
	}
	if removed > 0 {
		s.touchLocked(key)
	}
	return removed, nil
}

func (s *Store) sortedMembersLocked(key string) []SortedMember {
	set := s.sorted[key]
	result := make([]SortedMember, 0, len(set))
	for member, score := range set {
		result = append(result, SortedMember{Member: member, Score: score})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Score == result[j].Score {
			return result[i].Member < result[j].Member
		}
		return result[i].Score < result[j].Score
	})
	return result
}

func (s *Store) ZRange(key string, start, stop int) ([]SortedMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return nil, ErrWrongType
	}
	values := s.sortedMembersLocked(key)
	start, stop, ok := normalizeRange(len(values), start, stop)
	if !ok {
		return []SortedMember{}, nil
	}
	return append([]SortedMember(nil), values[start:stop+1]...), nil
}

func (s *Store) ZRank(key, member string) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return 0, false, ErrWrongType
	}
	values := s.sortedMembersLocked(key)
	for i, value := range values {
		if value.Member == member {
			return i, true, nil
		}
	}
	return 0, false, nil
}

func (s *Store) ZScore(key, member string) (float64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return 0, false, ErrWrongType
	}
	score, ok := s.sorted[key][member]
	return score, ok, nil
}

func (s *Store) ZCard(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return 0, ErrWrongType
	}
	return len(s.sorted[key]), nil
}

func (s *Store) GeoPut(key, member string, location GeoLocation) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return false, ErrWrongType
	}
	if s.sorted[key] == nil {
		s.sorted[key] = make(map[string]float64)
	}
	if s.geos[key] == nil {
		s.geos[key] = make(map[string]GeoLocation)
	}
	_, existed := s.geos[key][member]
	location.Score = GeoHash(location.Longitude, location.Latitude)
	s.geos[key][member] = location
	s.sorted[key][member] = location.Score
	s.touchLocked(key)
	return !existed, nil
}

func (s *Store) GeoPos(key string, members ...string) ([]*GeoLocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return nil, ErrWrongType
	}
	result := make([]*GeoLocation, len(members))
	for i, member := range members {
		if location, ok := s.geos[key][member]; ok {
			copy := location
			result[i] = &copy
		}
	}
	return result, nil
}

func (s *Store) GeoLocation(key, member string) (GeoLocation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return GeoLocation{}, false, ErrWrongType
	}
	location, ok := s.geos[key][member]
	return location, ok, nil
}

func (s *Store) GeoMembers(key string) (map[string]GeoLocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeZSet {
		return nil, ErrWrongType
	}
	result := make(map[string]GeoLocation, len(s.geos[key]))
	for member, location := range s.geos[key] {
		result[member] = location
	}
	return result, nil
}

func (s *Store) SetBit(key string, offset int, bit byte) (int, error) {
	if offset < 0 {
		return 0, errors.New("bit offset is not an unsigned integer")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeString {
		return 0, ErrWrongType
	}
	index := offset / 8
	if index >= len(s.strings[key]) {
		grown := make([]byte, index+1)
		copy(grown, s.strings[key])
		s.strings[key] = grown
	}
	mask := byte(1 << (7 - (offset % 8)))
	old := 0
	if s.strings[key][index]&mask != 0 {
		old = 1
	}
	if bit == 1 {
		s.strings[key][index] |= mask
	} else {
		s.strings[key][index] &^= mask
	}
	s.touchLocked(key)
	return old, nil
}

func (s *Store) GetBit(key string, offset int) (int, error) {
	if offset < 0 {
		return 0, errors.New("bit offset is not an unsigned integer")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeString {
		return 0, ErrWrongType
	}
	index := offset / 8
	if index >= len(s.strings[key]) {
		return 0, nil
	}
	if s.strings[key][index]&(1<<(7-offset%8)) != 0 {
		return 1, nil
	}
	return 0, nil
}

func (s *Store) BitCount(key string, start, end *int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(key, time.Now())
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeString {
		return 0, ErrWrongType
	}
	data := s.strings[key]
	if start == nil || end == nil {
		startValue, endValue := 0, len(data)-1
		start, end = &startValue, &endValue
	}
	lo, hi, ok := normalizeRange(len(data), *start, *end)
	if !ok {
		return 0, nil
	}
	count := 0
	for _, value := range data[lo : hi+1] {
		count += bitsSet(value)
	}
	return count, nil
}

func bitsSet(value byte) int {
	count := 0
	for value != 0 {
		value &= value - 1
		count++
	}
	return count
}

func (s *Store) BitOp(operation, destination string, sources ...string) (int, error) {
	operation = strings.ToUpper(operation)
	if operation != "AND" && operation != "OR" && operation != "XOR" && operation != "NOT" {
		return 0, errors.New("unsupported BITOP operation")
	}
	if operation == "NOT" && len(sources) != 1 || operation != "NOT" && len(sources) == 0 {
		return 0, errors.New("wrong number of BITOP arguments")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range append(append([]string(nil), sources...), destination) {
		s.purgeExpiredLocked(key, time.Now())
		if kind := s.kindLocked(key); kind != TypeNone && kind != TypeString {
			return 0, ErrWrongType
		}
	}
	length := 0
	for _, key := range sources {
		if len(s.strings[key]) > length {
			length = len(s.strings[key])
		}
	}
	result := make([]byte, length)
	if operation == "NOT" {
		for i := range result {
			if i < len(s.strings[sources[0]]) {
				result[i] = ^s.strings[sources[0]][i]
			} else {
				result[i] = 0xff
			}
		}
	} else {
		for i := range result {
			value := byte(0)
			if operation == "AND" {
				value = 0xff
			}
			for _, key := range sources {
				var current byte
				if i < len(s.strings[key]) {
					current = s.strings[key][i]
				}
				switch operation {
				case "AND":
					value &= current
				case "OR":
					value |= current
				case "XOR":
					value ^= current
				}
			}
			result[i] = value
		}
	}
	s.strings[destination] = result
	delete(s.expires, destination)
	s.touchLocked(destination)
	return length, nil
}

func normalizeRange(length, start, stop int) (int, int, bool) {
	if length == 0 {
		return 0, -1, false
	}
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 || start >= length || start > stop {
		return 0, -1, false
	}
	if stop >= length {
		stop = length - 1
	}
	return start, stop, true
}

type StringSnapshot struct {
	Key       string
	Value     []byte
	ExpiresAt *time.Time
}

func (s *Store) StringSnapshot() []StringSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	keys := make([]string, 0, len(s.strings))
	for key := range s.strings {
		s.purgeExpiredLocked(key, now)
		if _, ok := s.strings[key]; ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]StringSnapshot, 0, len(keys))
	for _, key := range keys {
		var expires *time.Time
		if deadline, ok := s.expires[key]; ok {
			d := deadline
			expires = &d
		}
		result = append(result, StringSnapshot{key, append([]byte(nil), s.strings[key]...), expires})
	}
	return result
}

func (s *Store) LoadString(key string, value []byte, deadline *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if kind := s.kindLocked(key); kind != TypeNone && kind != TypeString {
		return ErrWrongType
	}
	s.strings[key] = append([]byte(nil), value...)
	delete(s.expires, key)
	if deadline != nil {
		s.expires[key] = *deadline
	}
	s.touchLocked(key)
	return nil
}

func (s *Store) Versions(keys ...string) map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]uint64, len(keys))
	for _, key := range keys {
		result[key] = s.versions[key]
	}
	return result
}

func (s *Store) Version(key string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[key]
}

// GeoHash encodes coordinates into a sortable score.  A 26-bit grid per axis
// matches Redis's compact geohash precision while keeping the implementation
// dependency-free.
func GeoHash(longitude, latitude float64) float64 {
	const bits = 26
	const max = uint64(1 << bits)
	const lonMin, lonMax = -180.0, 180.0
	const latMin, latMax = -85.05112878, 85.05112878
	lon := uint64((longitude - lonMin) / (lonMax - lonMin) * float64(max-1))
	lat := uint64((latitude - latMin) / (latMax - latMin) * float64(max-1))
	var score uint64
	for i := 0; i < bits; i++ {
		score |= ((lat >> i) & 1) << (2 * i)
		score |= ((lon >> i) & 1) << (2*i + 1)
	}
	return float64(score)
}

func GeoDecode(score float64) (longitude, latitude float64) {
	const bits = 26
	const max = uint64(1 << bits)
	const lonMin, lonMax = -180.0, 180.0
	const latMin, latMax = -85.05112878, 85.05112878
	encoded := uint64(score)
	var lon, lat uint64
	for i := 0; i < bits; i++ {
		lat |= ((encoded >> (2 * i)) & 1) << i
		lon |= ((encoded >> (2*i + 1)) & 1) << i
	}
	longitude = (float64(lon)+0.5)/float64(max)*(lonMax-lonMin) + lonMin
	latitude = (float64(lat)+0.5)/float64(max)*(latMax-latMin) + latMin
	return
}

func DistanceMeters(lon1, lat1, lon2, lat2 float64) float64 {
	const radius = 6372797.560856
	toRad := func(value float64) float64 { return value * math.Pi / 180 }
	lat1r, lat2r := toRad(lat1), toRad(lat2)
	dLat, dLon := toRad(lat2-lat1), toRad(lon2-lon1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1r)*math.Cos(lat2r)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * radius * math.Asin(math.Sqrt(a))
}
