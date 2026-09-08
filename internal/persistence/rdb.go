package persistence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/MetalCloth/pulsekv-go/internal/store"
)

const maxRDBString = 64 << 20

type rdbReader struct {
	data []byte
	pos  int
}

func (r *rdbReader) byte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, errors.New("unexpected end of RDB")
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *rdbReader) bytes(n int) ([]byte, error) {
	if n < 0 || n > maxRDBString || r.pos+n > len(r.data) {
		return nil, errors.New("invalid RDB length")
	}
	result := r.data[r.pos : r.pos+n]
	r.pos += n
	return result, nil
}

func (r *rdbReader) length() (length int, encoded bool, err error) {
	b, err := r.byte()
	if err != nil {
		return 0, false, err
	}
	switch b >> 6 {
	case 0:
		return int(b & 0x3f), false, nil
	case 1:
		next, err := r.byte()
		if err != nil {
			return 0, false, err
		}
		return int(b&0x3f)<<8 | int(next), false, nil
	case 2:
		data, err := r.bytes(4)
		if err != nil {
			return 0, false, err
		}
		n := binary.LittleEndian.Uint32(data)
		if uint64(n) > maxRDBString {
			return 0, false, errors.New("RDB string exceeds limit")
		}
		return int(n), false, nil
	default:
		return int(b & 0x3f), true, nil
	}
}

func (r *rdbReader) string() (string, error) {
	length, encoded, err := r.length()
	if err != nil {
		return "", err
	}
	if encoded {
		switch length {
		case 0:
			b, err := r.byte()
			if err != nil {
				return "", err
			}
			return strconv.FormatInt(int64(int8(b)), 10), nil
		case 1:
			data, err := r.bytes(2)
			if err != nil {
				return "", err
			}
			return strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(data))), 10), nil
		case 2:
			data, err := r.bytes(4)
			if err != nil {
				return "", err
			}
			return strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(data))), 10), nil
		default:
			return "", errors.New("unsupported RDB string encoding")
		}
	}
	data, err := r.bytes(length)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func LoadRDB(path string, target *store.Store) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return LoadRDBBytes(data, target)
}

func LoadRDBBytes(data []byte, target *store.Store) error {
	if len(data) < 9 || string(data[:5]) != "REDIS" {
		return errors.New("not a Redis RDB file")
	}
	if _, err := strconv.Atoi(string(data[5:9])); err != nil {
		return fmt.Errorf("invalid RDB version: %w", err)
	}
	r := &rdbReader{data: data, pos: 9}
	var expiry *time.Time
	for {
		op, err := r.byte()
		if err != nil {
			return err
		}
		switch op {
		case 0xFA: // auxiliary field; unknown fields are forward-compatible.
			if _, err := r.string(); err != nil {
				return err
			}
			if _, err := r.string(); err != nil {
				return err
			}
		case 0xFE: // database selector
			if _, _, err := r.length(); err != nil {
				return err
			}
		case 0xFB: // hash table sizes
			if _, _, err := r.length(); err != nil {
				return err
			}
			if _, _, err := r.length(); err != nil {
				return err
			}
		case 0xFD: // seconds since epoch, little endian
			data, err := r.bytes(4)
			if err != nil {
				return err
			}
			t := time.Unix(int64(binary.LittleEndian.Uint32(data)), 0)
			expiry = &t
		case 0xFC: // milliseconds since epoch, little endian
			data, err := r.bytes(8)
			if err != nil {
				return err
			}
			t := time.UnixMilli(int64(binary.LittleEndian.Uint64(data)))
			expiry = &t
		case 0x00: // string value
			key, err := r.string()
			if err != nil {
				return err
			}
			value, err := r.string()
			if err != nil {
				return err
			}
			if expiry == nil || expiry.After(time.Now()) {
				if err := target.LoadString(key, []byte(value), expiry); err != nil {
					return err
				}
			}
			expiry = nil
		case 0xFF:
			// Redis appends an eight-byte checksum. A zero checksum is also
			// accepted for fixtures that intentionally omit validation.
			return nil
		default:
			return fmt.Errorf("unsupported RDB value opcode 0x%02x", op)
		}
	}
}

func encodeLength(n int) []byte {
	if n < 0 {
		panic("negative RDB length")
	}
	if n < 1<<6 {
		return []byte{byte(n)}
	}
	if n < 1<<14 {
		return []byte{byte(n>>8) | 0x40, byte(n)}
	}
	result := []byte{0x80, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(result[1:], uint32(n))
	return result
}

func EncodeStrings(snapshot []store.StringSnapshot) []byte {
	result := append([]byte("REDIS0011"), 0xFE, 0x00, 0xFB)
	result = append(result, encodeLength(len(snapshot))...)
	expires := 0
	for _, item := range snapshot {
		if item.ExpiresAt != nil {
			expires++
		}
	}
	result = append(result, encodeLength(expires)...)
	for _, item := range snapshot {
		if item.ExpiresAt != nil {
			result = append(result, 0xFC)
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(item.ExpiresAt.UnixMilli()))
			result = append(result, buf[:]...)
		}
		result = append(result, 0x00)
		result = append(result, encodeLength(len(item.Key))...)
		result = append(result, item.Key...)
		result = append(result, encodeLength(len(item.Value))...)
		result = append(result, item.Value...)
	}
	result = append(result, 0xFF)
	var buf [8]byte
	// The loader accepts the checksum trailer but deliberately keeps checksum
	// verification separate from the string-value subset used by this project.
	// A zero trailer is valid for our generated snapshots and is easy for older
	// Redis-compatible readers to ignore.
	return append(result, buf[:]...)
}

func EmptyRDB() []byte { return EncodeStrings(nil) }

func ValidateCoordinate(longitude, latitude float64) error {
	if math.IsNaN(longitude) || math.IsInf(longitude, 0) || longitude < -180 || longitude > 180 {
		return errors.New("invalid longitude")
	}
	if math.IsNaN(latitude) || math.IsInf(latitude, 0) || latitude < -85.05112878 || latitude > 85.05112878 {
		return errors.New("invalid latitude")
	}
	return nil
}
