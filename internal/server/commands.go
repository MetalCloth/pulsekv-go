package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MetalCloth/pulsekv-go/internal/persistence"
	"github.com/MetalCloth/pulsekv-go/internal/resp"
	"github.com/MetalCloth/pulsekv-go/internal/store"
)

func (s *Server) execute(command []string, client *Client, replay, alreadyLocked bool) (resp.Value, bool) {
	if len(command) == 0 {
		return resp.ErrorValue("ERR empty command"), false
	}
	name := strings.ToUpper(command[0])
	if isMutation(name) && !alreadyLocked {
		s.mutationMu.Lock()
		defer s.mutationMu.Unlock()
	}
	response, mutated := s.executeUnlocked(command, client)
	if mutated && !replay {
		s.recordMutation(mutationCommand(command, response))
	}
	return response, mutated
}

func mutationCommand(command []string, response resp.Value) []string {
	if len(command) > 0 && (strings.EqualFold(command[0], "BLPOP") || strings.EqualFold(command[0], "BRPOP")) && len(response.Array) >= 2 {
		name := "LPOP"
		if strings.EqualFold(command[0], "BRPOP") {
			name = "RPOP"
		}
		key, ok := response.Array[0].StringValue()
		if ok {
			return []string{name, key}
		}
	}
	return command
}

func isMutation(name string) bool {
	switch strings.ToUpper(name) {
	case "SET", "DEL", "INCR", "DECR", "EXPIRE", "PEXPIRE", "PERSIST", "MSET",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "XADD", "ZADD", "ZREM",
		"GEOADD", "SETBIT", "BITOP":
		return true
	default:
		return false
	}
}

func (s *Server) executeUnlocked(command []string, client *Client) (resp.Value, bool) {
	name := strings.ToUpper(command[0])
	switch name {
	case "PING":
		if len(command) > 2 {
			return wrongArgs(name), false
		}
		if client != nil && client.inSubscriptionMode() {
			message := ""
			if len(command) == 2 {
				message = command[1]
			}
			return resp.ArrayValue(resp.BulkStringValue("pong"), resp.BulkStringValue(message)), false
		}
		if len(command) == 2 {
			return resp.BulkStringValue(command[1]), false
		}
		return resp.Simple("PONG"), false

	case "ECHO":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		return resp.BulkStringValue(command[1]), false

	case "COMMAND":
		return resp.ArrayValue(), false

	case "SELECT":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		if _, err := strconv.Atoi(command[1]); err != nil {
			return resp.ErrorValue("ERR invalid DB index"), false
		}
		return resp.Simple("OK"), false

	case "CONFIG":
		return s.configCommand(command), false

	case "INFO":
		return s.infoCommand(command), false

	case "SET":
		if len(command) < 3 {
			return wrongArgs(name), false
		}
		var deadline *time.Time
		onlyIfAbsent, onlyIfPresent := false, false
		for i := 3; i < len(command); i++ {
			switch strings.ToUpper(command[i]) {
			case "NX":
				onlyIfAbsent = true
			case "XX":
				onlyIfPresent = true
			case "PX", "EX":
				if i+1 >= len(command) {
					return wrongArgs(name), false
				}
				amount, err := strconv.ParseInt(command[i+1], 10, 64)
				if err != nil || amount <= 0 {
					return resp.ErrorValue("ERR invalid expire time in set"), false
				}
				unit := time.Millisecond
				if strings.EqualFold(command[i], "EX") {
					unit = time.Second
				}
				t := time.Now().Add(time.Duration(amount) * unit)
				deadline = &t
				i++
			default:
				return resp.ErrorValue("ERR syntax error"), false
			}
		}
		if onlyIfAbsent && onlyIfPresent {
			return resp.ErrorValue("ERR syntax error"), false
		}
		ok, err := s.store.Set(command[1], []byte(command[2]), deadline, onlyIfAbsent, onlyIfPresent)
		if err != nil {
			return storeError(err), false
		}
		if !ok {
			return resp.Bulk(nil), false
		}
		return resp.Simple("OK"), true

	case "GET":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		value, ok, err := s.store.Get(command[1])
		if err != nil {
			return storeError(err), false
		}
		if !ok {
			return resp.Bulk(nil), false
		}
		return resp.Bulk(value), false

	case "DEL":
		if len(command) < 2 {
			return wrongArgs(name), false
		}
		return resp.IntegerValue(int64(s.store.Delete(command[1:]...))), true

	case "EXISTS":
		if len(command) < 2 {
			return wrongArgs(name), false
		}
		count := 0
		for _, key := range command[1:] {
			if s.store.Exists(key) {
				count++
			}
		}
		return resp.IntegerValue(int64(count)), false

	case "INCR", "DECR":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		delta := int64(1)
		if name == "DECR" {
			delta = -1
		}
		value, err := s.store.Incr(command[1], delta)
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(value), true

	case "MGET":
		if len(command) < 2 {
			return wrongArgs(name), false
		}
		values, err := s.store.MGet(command[1:]...)
		if err != nil {
			return storeError(err), false
		}
		result := make([]resp.Value, len(values))
		for i, value := range values {
			result[i] = resp.Bulk(value)
		}
		return resp.ArrayValue(result...), false

	case "MSET":
		if len(command) < 3 || len(command)%2 == 0 {
			return wrongArgs(name), false
		}
		pairs := make([][2]string, 0, (len(command)-1)/2)
		for i := 1; i < len(command); i += 2 {
			pairs = append(pairs, [2]string{command[i], command[i+1]})
		}
		if err := s.store.MSet(pairs); err != nil {
			return storeError(err), false
		}
		return resp.Simple("OK"), true

	case "EXPIRE", "PEXPIRE":
		if len(command) != 3 {
			return wrongArgs(name), false
		}
		amount, err := strconv.ParseInt(command[2], 10, 64)
		if err != nil {
			return resp.ErrorValue("ERR value is not an integer or out of range"), false
		}
		unit := time.Second
		if name == "PEXPIRE" {
			unit = time.Millisecond
		}
		ok, err := s.store.Expire(command[1], time.Duration(amount)*unit)
		if err != nil {
			return storeError(err), false
		}
		if !ok {
			return resp.IntegerValue(0), false
		}
		return resp.IntegerValue(1), true

	case "TTL", "PTTL":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		precision := time.Second
		if name == "PTTL" {
			precision = time.Millisecond
		}
		return resp.IntegerValue(s.store.TTL(command[1], precision)), false

	case "PERSIST":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		if s.store.Persist(command[1]) {
			return resp.IntegerValue(1), true
		}
		return resp.IntegerValue(0), false

	case "TYPE":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		return resp.Simple(string(s.store.Type(command[1]))), false

	case "LPUSH", "RPUSH":
		if len(command) < 3 {
			return wrongArgs(name), false
		}
		length, err := s.store.ListPush(command[1], command[2:], name == "LPUSH")
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(length)), true

	case "LRANGE":
		if len(command) != 4 {
			return wrongArgs(name), false
		}
		start, err1 := strconv.Atoi(command[2])
		stop, err2 := strconv.Atoi(command[3])
		if err1 != nil || err2 != nil {
			return resp.ErrorValue("ERR value is not an integer or out of range"), false
		}
		values, err := s.store.ListRange(command[1], start, stop)
		if err != nil {
			return storeError(err), false
		}
		return bulkArray(values), false

	case "LLEN":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		length, err := s.store.ListLen(command[1])
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(length)), false

	case "LPOP", "RPOP":
		if len(command) < 2 || len(command) > 3 {
			return wrongArgs(name), false
		}
		var count *int
		if len(command) == 3 {
			value, err := strconv.Atoi(command[2])
			if err != nil || value < 0 {
				return resp.ErrorValue("ERR value is not an integer or out of range"), false
			}
			count = &value
		}
		values, err := s.store.ListPop(command[1], name == "LPOP", count)
		if err != nil {
			return storeError(err), false
		}
		if len(command) == 2 {
			if len(values) == 0 {
				return resp.Bulk(nil), false
			}
			return resp.BulkStringValue(values[0]), true
		}
		return bulkArray(values), len(values) > 0

	case "BLPOP", "BRPOP":
		return s.blockingPop(command, name == "BLPOP")

	case "XADD":
		if len(command) < 5 || len(command[3:])%2 != 0 {
			return wrongArgs(name), false
		}
		entry, err := s.store.XAdd(command[1], command[2], command[3:])
		if err != nil {
			return storeError(err), false
		}
		return resp.BulkStringValue(entry.ID.String()), true

	case "XRANGE":
		if len(command) < 4 {
			return wrongArgs(name), false
		}
		count := 0
		if len(command) > 4 {
			if len(command) != 6 || !strings.EqualFold(command[4], "COUNT") {
				return resp.ErrorValue("ERR syntax error"), false
			}
			var err error
			count, err = strconv.Atoi(command[5])
			if err != nil || count < 0 {
				return resp.ErrorValue("ERR value is not an integer or out of range"), false
			}
		}
		entries, err := s.store.XRange(command[1], command[2], command[3], count)
		if err != nil {
			return storeError(err), false
		}
		return streamEntries(entries), false

	case "XREAD":
		return s.xread(command)

	case "ZADD":
		if len(command) < 4 || len(command[2:])%2 != 0 {
			return wrongArgs(name), false
		}
		members := make([]store.SortedMember, 0, (len(command)-2)/2)
		for i := 2; i < len(command); i += 2 {
			score, err := strconv.ParseFloat(command[i], 64)
			if err != nil || math.IsNaN(score) {
				return resp.ErrorValue("ERR value is not a valid float"), false
			}
			members = append(members, store.SortedMember{Member: command[i+1], Score: score})
		}
		added, err := s.store.ZAdd(command[1], members)
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(added)), true

	case "ZRANGE":
		if len(command) < 4 || len(command) > 5 {
			return wrongArgs(name), false
		}
		start, err1 := strconv.Atoi(command[2])
		stop, err2 := strconv.Atoi(command[3])
		if err1 != nil || err2 != nil {
			return resp.ErrorValue("ERR value is not an integer or out of range"), false
		}
		withScores := len(command) == 5 && strings.EqualFold(command[4], "WITHSCORES")
		if len(command) == 5 && !withScores {
			return resp.ErrorValue("ERR syntax error"), false
		}
		members, err := s.store.ZRange(command[1], start, stop)
		if err != nil {
			return storeError(err), false
		}
		result := make([]resp.Value, 0, len(members)*2)
		for _, member := range members {
			result = append(result, resp.BulkStringValue(member.Member))
			if withScores {
				result = append(result, resp.BulkStringValue(strconv.FormatFloat(member.Score, 'f', -1, 64)))
			}
		}
		return resp.ArrayValue(result...), false

	case "ZRANK":
		if len(command) != 3 {
			return wrongArgs(name), false
		}
		rank, ok, err := s.store.ZRank(command[1], command[2])
		if err != nil {
			return storeError(err), false
		}
		if !ok {
			return resp.Bulk(nil), false
		}
		return resp.IntegerValue(int64(rank)), false

	case "ZCARD":
		if len(command) != 2 {
			return wrongArgs(name), false
		}
		card, err := s.store.ZCard(command[1])
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(card)), false

	case "ZSCORE":
		if len(command) != 3 {
			return wrongArgs(name), false
		}
		score, ok, err := s.store.ZScore(command[1], command[2])
		if err != nil {
			return storeError(err), false
		}
		if !ok {
			return resp.Bulk(nil), false
		}
		return resp.BulkStringValue(strconv.FormatFloat(score, 'f', -1, 64)), false

	case "ZREM":
		if len(command) < 3 {
			return wrongArgs(name), false
		}
		removed, err := s.store.ZRem(command[1], command[2:]...)
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(removed)), removed > 0

	case "SETBIT":
		if len(command) != 4 {
			return wrongArgs(name), false
		}
		offset, err := strconv.Atoi(command[2])
		if err != nil || offset < 0 {
			return resp.ErrorValue("ERR bit offset is not an integer or out of range"), false
		}
		if command[3] != "0" && command[3] != "1" {
			return resp.ErrorValue("ERR bit is not an integer or out of range"), false
		}
		old, err := s.store.SetBit(command[1], offset, command[3][0]-'0')
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(old)), true

	case "GETBIT":
		if len(command) != 3 {
			return wrongArgs(name), false
		}
		offset, err := strconv.Atoi(command[2])
		if err != nil || offset < 0 {
			return resp.ErrorValue("ERR bit offset is not an integer or out of range"), false
		}
		value, err := s.store.GetBit(command[1], offset)
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(value)), false

	case "BITCOUNT":
		if len(command) != 2 && len(command) != 4 {
			return wrongArgs(name), false
		}
		var start, end *int
		if len(command) == 4 {
			startValue, err1 := strconv.Atoi(command[2])
			endValue, err2 := strconv.Atoi(command[3])
			if err1 != nil || err2 != nil {
				return resp.ErrorValue("ERR value is not an integer or out of range"), false
			}
			start, end = &startValue, &endValue
		}
		count, err := s.store.BitCount(command[1], start, end)
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(count)), false

	case "BITOP":
		if len(command) < 4 {
			return wrongArgs(name), false
		}
		length, err := s.store.BitOp(command[1], command[2], command[3:]...)
		if err != nil {
			return storeError(err), false
		}
		return resp.IntegerValue(int64(length)), true

	case "GEOADD":
		if len(command) < 5 || len(command[2:])%3 != 0 {
			return wrongArgs(name), false
		}
		added := 0
		for i := 2; i < len(command); i += 3 {
			longitude, err1 := strconv.ParseFloat(command[i], 64)
			latitude, err2 := strconv.ParseFloat(command[i+1], 64)
			if err1 != nil || err2 != nil {
				return resp.ErrorValue("ERR invalid longitude or latitude"), false
			}
			if err := persistence.ValidateCoordinate(longitude, latitude); err != nil {
				return storeError(err), false
			}
			created, err := s.store.GeoPut(command[1], command[i+2], store.GeoLocation{Longitude: longitude, Latitude: latitude})
			if err != nil {
				return storeError(err), false
			}
			if created {
				added++
			}
		}
		return resp.IntegerValue(int64(added)), true

	case "GEOPOS":
		if len(command) < 3 {
			return wrongArgs(name), false
		}
		locations, err := s.store.GeoPos(command[1], command[2:]...)
		if err != nil {
			return storeError(err), false
		}
		result := make([]resp.Value, len(locations))
		for i, location := range locations {
			if location == nil {
				result[i] = resp.Bulk(nil)
				continue
			}
			result[i] = resp.ArrayValue(
				resp.BulkStringValue(strconv.FormatFloat(location.Longitude, 'f', 10, 64)),
				resp.BulkStringValue(strconv.FormatFloat(location.Latitude, 'f', 10, 64)),
			)
		}
		return resp.ArrayValue(result...), false

	case "GEODIST":
		if len(command) < 4 || len(command) > 5 {
			return wrongArgs(name), false
		}
		first, ok, err := s.store.GeoLocation(command[1], command[2])
		if err != nil {
			return storeError(err), false
		}
		if !ok {
			return resp.Bulk(nil), false
		}
		second, ok, err := s.store.GeoLocation(command[1], command[3])
		if err != nil {
			return storeError(err), false
		}
		if !ok {
			return resp.Bulk(nil), false
		}
		unit := "m"
		if len(command) == 5 {
			unit = strings.ToLower(command[4])
		}
		distance, ok := convertDistance(store.DistanceMeters(first.Longitude, first.Latitude, second.Longitude, second.Latitude), unit)
		if !ok {
			return resp.ErrorValue("ERR unsupported unit"), false
		}
		return resp.BulkStringValue(strconv.FormatFloat(distance, 'f', 4, 64)), false

	case "GEOSEARCH":
		return s.geoSearch(command)

	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE":
		return resp.ErrorValue("ERR subscription command routing failure"), false

	case "PUBLISH":
		if len(command) != 3 {
			return wrongArgs(name), false
		}
		return resp.IntegerValue(int64(s.publish(command[1], command[2]))), false

	case "REPLCONF":
		if len(command) < 2 {
			return wrongArgs(name), false
		}
		switch strings.ToUpper(command[1]) {
		case "GETACK":
			return resp.ArrayValue(resp.BulkStringValue("REPLCONF"), resp.BulkStringValue("ACK"), resp.BulkStringValue(strconv.FormatInt(s.replicaOffset(), 10))), false
		case "ACK":
			if len(command) != 3 {
				return wrongArgs(name), false
			}
			offset, err := strconv.ParseInt(command[2], 10, 64)
			if err != nil {
				return resp.ErrorValue("ERR invalid replication offset"), false
			}
			if client != nil {
				s.updateReplicaAck(client, offset)
			}
			return resp.Value{}, false
		default:
			return resp.Simple("OK"), false
		}

	case "WAIT":
		if len(command) != 3 {
			return wrongArgs(name), false
		}
		count, err1 := strconv.Atoi(command[1])
		timeout, err2 := strconv.Atoi(command[2])
		if err1 != nil || err2 != nil || count < 0 || timeout < 0 {
			return resp.ErrorValue("ERR timeout is not an integer or out of range"), false
		}
		return resp.IntegerValue(int64(s.waitForReplicas(count, time.Duration(timeout)*time.Millisecond))), false

	case "PSYNC":
		return resp.ErrorValue("ERR PSYNC must be handled by the connection loop"), false

	case "ACL":
		return s.aclCommand(command, client), false

	case "AUTH":
		return s.authCommand(command, client), false

	case "SAVE":
		return s.saveCommand(), false

	default:
		return resp.ErrorValue("ERR unknown command '" + command[0] + "'"), false
	}
}

func wrongArgs(name string) resp.Value {
	return resp.ErrorValue("ERR wrong number of arguments for '" + strings.ToLower(name) + "' command")
}

func storeError(err error) resp.Value {
	if errors.Is(err, store.ErrWrongType) {
		return resp.ErrorValue("WRONGTYPE " + err.Error())
	}
	return resp.ErrorValue("ERR " + err.Error())
}

func bulkArray(values []string) resp.Value {
	result := make([]resp.Value, len(values))
	for i, value := range values {
		result[i] = resp.BulkStringValue(value)
	}
	return resp.ArrayValue(result...)
}

func streamEntries(entries []store.StreamEntry) resp.Value {
	result := make([]resp.Value, 0, len(entries))
	for _, entry := range entries {
		fields := make([]resp.Value, len(entry.Fields))
		for i, field := range entry.Fields {
			fields[i] = resp.BulkStringValue(field)
		}
		result = append(result, resp.ArrayValue(resp.BulkStringValue(entry.ID.String()), resp.ArrayValue(fields...)))
	}
	return resp.ArrayValue(result...)
}

func (s *Server) blockingPop(command []string, left bool) (resp.Value, bool) {
	if len(command) < 3 {
		return wrongArgs(command[0]), false
	}
	timeoutSeconds, err := strconv.ParseFloat(command[len(command)-1], 64)
	if err != nil || timeoutSeconds < 0 {
		return resp.ErrorValue("ERR timeout is not a float or out of range"), false
	}
	keys := command[1 : len(command)-1]
	deadline := time.Time{}
	if timeoutSeconds > 0 {
		deadline = time.Now().Add(time.Duration(timeoutSeconds * float64(time.Second)))
	}
	for {
		changed := s.store.Change()
		for _, key := range keys {
			values, err := s.store.ListPop(key, left, nil)
			if err != nil {
				return storeError(err), false
			}
			if len(values) > 0 {
				return resp.ArrayValue(resp.BulkStringValue(key), resp.BulkStringValue(values[0])), true
			}
		}
		// Capture the notification channel before checking the list. If a
		// producer wins the race after a failed pop, retry instead of waiting on
		// a channel that was already replaced.
		if s.store.Change() != changed {
			continue
		}
		if timeoutSeconds == 0 {
			select {
			case <-changed:
			case <-s.closed:
				return resp.NullArrayValue(), false
			}
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return resp.NullArrayValue(), false
		}
		timer := time.NewTimer(remaining)
		select {
		case <-changed:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			return resp.NullArrayValue(), false
		case <-s.closed:
			if !timer.Stop() {
				<-timer.C
			}
			return resp.NullArrayValue(), false
		}
	}
}

func (s *Server) parseXRead(command []string) (requests []store.StreamReadRequest, block time.Duration, blocking bool, count int, err error) {
	if len(command) < 4 {
		return nil, 0, false, 0, errors.New("wrong number of arguments for XREAD")
	}
	index := 1
	if strings.EqualFold(command[index], "COUNT") {
		if index+1 >= len(command) {
			return nil, 0, false, 0, errors.New("syntax error")
		}
		count, err = strconv.Atoi(command[index+1])
		if err != nil || count < 0 {
			return nil, 0, false, 0, errors.New("value is not an integer or out of range")
		}
		index += 2
	}
	if index < len(command) && strings.EqualFold(command[index], "BLOCK") {
		if index+1 >= len(command) {
			return nil, 0, false, 0, errors.New("syntax error")
		}
		milliseconds, parseErr := strconv.ParseInt(command[index+1], 10, 64)
		if parseErr != nil || milliseconds < 0 {
			return nil, 0, false, 0, errors.New("timeout is not an integer or out of range")
		}
		blocking = true
		block = time.Duration(milliseconds) * time.Millisecond
		index += 2
	}
	if index >= len(command) || !strings.EqualFold(command[index], "STREAMS") {
		return nil, 0, false, 0, errors.New("syntax error")
	}
	index++
	remaining := len(command) - index
	if remaining < 2 || remaining%2 != 0 {
		return nil, 0, false, 0, errors.New("syntax error")
	}
	streamCount := remaining / 2
	keys := command[index : index+streamCount]
	starts := command[index+streamCount:]
	requests = make([]store.StreamReadRequest, streamCount)
	for i, key := range keys {
		start, parseErr := store.ParseStreamID(starts[i], false)
		if starts[i] == "$" {
			if last, ok := s.store.StreamLastID(key); ok {
				start = last
			} else {
				start = store.StreamID{}
			}
			parseErr = nil
		}
		if parseErr != nil {
			return nil, 0, false, 0, parseErr
		}
		requests[i] = store.StreamReadRequest{Key: key, Start: start}
	}
	return requests, block, blocking, count, nil
}

func (s *Server) xread(command []string) (resp.Value, bool) {
	requests, block, blocking, count, err := s.parseXRead(command)
	if err != nil {
		return resp.ErrorValue("ERR " + err.Error()), false
	}
	for {
		changed := s.store.Change()
		result := s.store.XRead(requests, count)
		if len(result) > 0 {
			outer := make([]resp.Value, 0, len(result))
			for _, streamResult := range result {
				outer = append(outer, resp.ArrayValue(resp.BulkStringValue(streamResult.Key), streamEntries(streamResult.Entries)))
			}
			return resp.ArrayValue(outer...), false
		}
		if !blocking {
			return resp.NullArrayValue(), false
		}
		if s.store.Change() != changed {
			continue
		}
		if block == 0 {
			select {
			case <-changed:
			case <-s.closed:
				return resp.NullArrayValue(), false
			}
			continue
		}
		timer := time.NewTimer(block)
		select {
		case <-changed:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			return resp.NullArrayValue(), false
		case <-s.closed:
			if !timer.Stop() {
				<-timer.C
			}
			return resp.NullArrayValue(), false
		}
	}
}

func (s *Server) configCommand(command []string) resp.Value {
	if len(command) != 3 || !strings.EqualFold(command[1], "GET") {
		return resp.ErrorValue("ERR only CONFIG GET is supported")
	}
	key := strings.ToLower(command[2])
	var value string
	switch key {
	case "dir":
		value = s.cfg.Dir
	case "dbfilename":
		value = s.cfg.DBFilename
	case "appendonly":
		if s.cfg.AppendOnly {
			value = "yes"
		} else {
			value = "no"
		}
	case "appenddirname":
		value = s.cfg.AppendDir
	case "appendfilename":
		value = s.cfg.AppendFilename
	case "appendfsync":
		value = s.cfg.AppendFsync
	case "port":
		value = portFromAddr(s.cfg.Addr)
	default:
		return resp.ArrayValue()
	}
	return resp.ArrayValue(resp.BulkStringValue(command[2]), resp.BulkStringValue(value))
}

func (s *Server) infoCommand(command []string) resp.Value {
	section := ""
	if len(command) > 2 {
		return wrongArgs("INFO")
	}
	if len(command) == 2 {
		section = strings.ToLower(command[1])
	}
	s.replMu.Lock()
	role := "master"
	if s.cfg.ReplicaOf != "" {
		role = "slave"
	}
	info := fmt.Sprintf("# Replication\r\nrole:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\nconnected_slaves:%d\r\n", role, s.replID, s.replOffset, len(s.replicas))
	s.replMu.Unlock()
	if section != "" && section != "replication" {
		return resp.BulkStringValue("")
	}
	return resp.BulkStringValue(info)
}

func (s *Server) saveCommand() resp.Value {
	path := s.cfg.DBFilename
	if path == "" {
		path = "dump.rdb"
	}
	if s.cfg.Dir != "" && !strings.HasPrefix(path, "/") {
		path = s.cfg.Dir + "/" + path
	}
	if err := osWriteFile(path, persistence.EncodeStrings(s.store.StringSnapshot())); err != nil {
		return resp.ErrorValue("ERR " + err.Error())
	}
	return resp.Simple("OK")
}

func (s *Server) authCommand(command []string, client *Client) resp.Value {
	if client == nil {
		return resp.ErrorValue("ERR AUTH requires a client")
	}
	var name, password string
	if len(command) == 2 {
		name, password = "default", command[1]
	} else if len(command) == 3 {
		name, password = command[1], command[2]
	} else {
		return wrongArgs("AUTH")
	}
	s.usersMu.RLock()
	user := s.users[name]
	s.usersMu.RUnlock()
	if user == nil || !user.Enabled {
		return resp.ErrorValue("WRONGPASS invalid username-password pair or user is disabled")
	}
	if user.NoPass {
		client.user, client.authenticated = user, true
		return resp.Simple("OK")
	}
	digest := sha256.Sum256([]byte(password))
	valid := false
	for expected := range user.Passwords {
		if subtle.ConstantTimeCompare(expected[:], digest[:]) == 1 {
			valid = true
			break
		}
	}
	if !valid {
		return resp.ErrorValue("WRONGPASS invalid username-password pair or user is disabled")
	}
	client.user, client.authenticated = user, true
	return resp.Simple("OK")
}

func (s *Server) aclCommand(command []string, client *Client) resp.Value {
	if len(command) < 2 {
		return wrongArgs("ACL")
	}
	switch strings.ToUpper(command[1]) {
	case "WHOAMI":
		if client == nil || client.user == nil {
			return resp.BulkStringValue("default")
		}
		return resp.BulkStringValue(client.user.Name)
	case "GETUSER":
		if len(command) != 3 {
			return wrongArgs("ACL GETUSER")
		}
		s.usersMu.RLock()
		user := s.users[command[2]]
		s.usersMu.RUnlock()
		if user == nil {
			return resp.Bulk(nil)
		}
		flags := []resp.Value{resp.BulkStringValue("on")}
		if !user.Enabled {
			flags = []resp.Value{resp.BulkStringValue("off")}
		}
		if user.NoPass {
			flags = append(flags, resp.BulkStringValue("nopass"))
		}
		passwords := make([]resp.Value, 0, len(user.Passwords))
		for digest := range user.Passwords {
			passwords = append(passwords, resp.BulkStringValue(fmt.Sprintf("%x", digest[:])))
		}
		return resp.ArrayValue(resp.BulkStringValue("flags"), resp.ArrayValue(flags...), resp.BulkStringValue("passwords"), resp.ArrayValue(passwords...))
	case "SETUSER":
		if len(command) < 3 {
			return wrongArgs("ACL SETUSER")
		}
		name := command[2]
		s.usersMu.Lock()
		user := s.users[name]
		if user == nil {
			user = &User{Name: name, Enabled: true, NoPass: true, Passwords: make(map[[32]byte]struct{})}
			s.users[name] = user
		}
		for _, token := range command[3:] {
			switch {
			case token == "on":
				user.Enabled = true
			case token == "off":
				user.Enabled = false
			case token == "nopass":
				user.NoPass = true
			case token == "resetpass":
				user.Passwords = make(map[[32]byte]struct{})
				user.NoPass = false
			case strings.HasPrefix(token, ">"):
				if len(token) == 1 {
					s.usersMu.Unlock()
					return resp.ErrorValue("ERR password cannot be empty")
				}
				digest := sha256.Sum256([]byte(token[1:]))
				user.Passwords[digest] = struct{}{}
				user.NoPass = false
			case strings.HasPrefix(token, "<"):
				digest := sha256.Sum256([]byte(token[1:]))
				delete(user.Passwords, digest)
			default:
				// Access-control categories are outside this challenge's
				// command surface; retain them as documented flags.
			}
		}
		s.usersMu.Unlock()
		return resp.Simple("OK")
	default:
		return resp.ErrorValue("ERR unsupported ACL subcommand")
	}
}

func (s *Server) publish(channel, message string) int {
	s.pubMu.Lock()
	direct := make([]*Client, 0, len(s.channels[channel]))
	for client := range s.channels[channel] {
		direct = append(direct, client)
	}
	patternTargets := make([]struct {
		client  *Client
		pattern string
	}, 0)
	for pattern, clients := range s.patterns {
		matched, _ := path.Match(pattern, channel)
		if !matched {
			continue
		}
		for client := range clients {
			patternTargets = append(patternTargets, struct {
				client  *Client
				pattern string
			}{client, pattern})
		}
	}
	s.pubMu.Unlock()
	for _, client := range direct {
		_ = client.send(resp.ArrayValue(resp.BulkStringValue("message"), resp.BulkStringValue(channel), resp.BulkStringValue(message)))
	}
	for _, target := range patternTargets {
		_ = target.client.send(resp.ArrayValue(resp.BulkStringValue("pmessage"), resp.BulkStringValue(target.pattern), resp.BulkStringValue(channel), resp.BulkStringValue(message)))
	}
	return len(direct) + len(patternTargets)
}

func (s *Server) subscriptionCommand(client *Client, command []string) []resp.Value {
	name := strings.ToUpper(command[0])
	values := command[1:]
	if len(values) == 0 && name != "UNSUBSCRIBE" && name != "PUNSUBSCRIBE" {
		return []resp.Value{wrongArgs(name)}
	}
	if len(values) == 0 {
		if name == "UNSUBSCRIBE" {
			values = make([]string, 0, len(client.subs))
			for channel := range client.subs {
				values = append(values, channel)
			}
		} else {
			values = make([]string, 0, len(client.patterns))
			for pattern := range client.patterns {
				values = append(values, pattern)
			}
		}
	}
	result := make([]resp.Value, 0, len(values))
	for _, value := range values {
		s.pubMu.Lock()
		subscribed := false
		count := 0
		switch name {
		case "SUBSCRIBE":
			if !client.subs[value] {
				client.subs[value] = true
				if s.channels[value] == nil {
					s.channels[value] = make(map[*Client]struct{})
				}
				s.channels[value][client] = struct{}{}
			}
			subscribed = true
		case "UNSUBSCRIBE":
			delete(client.subs, value)
			if subscribers := s.channels[value]; subscribers != nil {
				delete(subscribers, client)
				if len(subscribers) == 0 {
					delete(s.channels, value)
				}
			}
			subscribed = false
		case "PSUBSCRIBE":
			if !client.patterns[value] {
				client.patterns[value] = true
				if s.patterns[value] == nil {
					s.patterns[value] = make(map[*Client]struct{})
				}
				s.patterns[value][client] = struct{}{}
			}
			subscribed = true
		case "PUNSUBSCRIBE":
			delete(client.patterns, value)
			if subscribers := s.patterns[value]; subscribers != nil {
				delete(subscribers, client)
				if len(subscribers) == 0 {
					delete(s.patterns, value)
				}
			}
		}
		count = len(client.subs) + len(client.patterns)
		s.pubMu.Unlock()
		kind := strings.ToLower(name)
		if strings.HasPrefix(kind, "p") {
			kind = kind[1:]
		}
		_ = subscribed
		result = append(result, resp.ArrayValue(resp.BulkStringValue(kind), resp.BulkStringValue(value), resp.IntegerValue(int64(count))))
	}
	return result
}

func convertDistance(meters float64, unit string) (float64, bool) {
	switch strings.ToLower(unit) {
	case "m":
		return meters, true
	case "km":
		return meters / 1000, true
	case "mi":
		return meters / 1609.344, true
	case "ft":
		return meters / 0.3048, true
	default:
		return 0, false
	}
}

func (s *Server) geoSearch(command []string) (resp.Value, bool) {
	if len(command) < 8 || !strings.EqualFold(command[2], "FROMLONLAT") || !strings.EqualFold(command[5], "BYRADIUS") {
		return resp.ErrorValue("ERR syntax error"), false
	}
	longitude, err1 := strconv.ParseFloat(command[3], 64)
	latitude, err2 := strconv.ParseFloat(command[4], 64)
	radius, err3 := strconv.ParseFloat(command[6], 64)
	if err1 != nil || err2 != nil || err3 != nil || radius < 0 {
		return resp.ErrorValue("ERR invalid GEOSEARCH argument"), false
	}
	if err := persistence.ValidateCoordinate(longitude, latitude); err != nil {
		return storeError(err), false
	}
	unit := strings.ToLower(command[7])
	if _, ok := convertDistance(1, unit); !ok {
		return resp.ErrorValue("ERR unsupported unit"), false
	}
	withDist, withCoord := false, false
	count := 0
	for i := 8; i < len(command); i++ {
		switch strings.ToUpper(command[i]) {
		case "WITHDIST":
			withDist = true
		case "WITHCOORD":
			withCoord = true
		case "COUNT":
			if i+1 >= len(command) {
				return resp.ErrorValue("ERR syntax error"), false
			}
			count, err1 = strconv.Atoi(command[i+1])
			if err1 != nil || count < 0 {
				return resp.ErrorValue("ERR invalid COUNT"), false
			}
			i++
		default:
			return resp.ErrorValue("ERR syntax error"), false
		}
	}
	locations, err := s.store.GeoMembers(command[1])
	if err != nil {
		return storeError(err), false
	}
	type candidate struct {
		member   string
		location store.GeoLocation
		distance float64
	}
	candidates := make([]candidate, 0, len(locations))
	for member, location := range locations {
		distance := store.DistanceMeters(longitude, latitude, location.Longitude, location.Latitude)
		converted, _ := convertDistance(distance, unit)
		if converted <= radius {
			candidates = append(candidates, candidate{member, location, converted})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].distance == candidates[j].distance {
			return candidates[i].member < candidates[j].member
		}
		return candidates[i].distance < candidates[j].distance
	})
	if count > 0 && count < len(candidates) {
		candidates = candidates[:count]
	}
	result := make([]resp.Value, 0, len(candidates))
	for _, item := range candidates {
		if !withDist && !withCoord {
			result = append(result, resp.BulkStringValue(item.member))
			continue
		}
		row := []resp.Value{resp.BulkStringValue(item.member)}
		if withDist {
			row = append(row, resp.BulkStringValue(strconv.FormatFloat(item.distance, 'f', 4, 64)))
		}
		if withCoord {
			row = append(row, resp.ArrayValue(
				resp.BulkStringValue(strconv.FormatFloat(item.location.Longitude, 'f', 10, 64)),
				resp.BulkStringValue(strconv.FormatFloat(item.location.Latitude, 'f', 10, 64)),
			))
		}
		result = append(result, resp.ArrayValue(row...))
	}
	return resp.ArrayValue(result...), false
}

func osWriteFile(name string, data []byte) error {
	return os.WriteFile(name, data, 0o640)
}
