// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/types"
)

// First byte in the encoded value which specifies the encoding type.
const (
	NilFlag          byte = 0
	bytesFlag        byte = 1
	compactBytesFlag byte = 2
	intFlag          byte = 3
	uintFlag         byte = 4
	floatFlag        byte = 5
	decimalFlag      byte = 6
	durationFlag     byte = 7
	varintFlag       byte = 8
	uvarintFlag      byte = 9
	maxFlag          byte = 250
)

func encode(b []byte, val types.Datum, comparable bool) ([]byte, error) {
	switch val.Kind() {
	case types.KindInt64:
		b = encodeSignedInt(b, val.GetInt64())
	case types.KindUint64:
		b = encodeUnsignedInt(b, val.GetUint64())
	case types.KindFloat32, types.KindFloat64:
		b = append(b, floatFlag)
		b = EncodeFloat(b, val.GetFloat64())
	case types.KindString, types.KindBytes:
		b = encodeBytes(b, val.GetBytes(), comparable)
	case types.KindMysqlTime:
		b = append(b, uintFlag)
		v, err := val.GetMysqlTime().ToPackedUint()
		if err != nil {
			return nil, errors.Trace(err)
		}
		b = EncodeUint(b, v)
	case types.KindMysqlDuration:
		// duration may have negative value, so we cannot use String to encode directly.
		b = append(b, durationFlag)
		b = EncodeInt(b, int64(val.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		b = append(b, decimalFlag)
		b = EncodeDecimal(b, val)
	case types.KindMysqlHex:
		b = encodeSignedInt(b, int64(val.GetMysqlHex().ToNumber()))
	case types.KindMysqlBit:
		b = encodeUnsignedInt(b, uint64(val.GetMysqlBit().ToNumber()))
	case types.KindMysqlEnum:
		b = encodeUnsignedInt(b, uint64(val.GetMysqlEnum().ToNumber()))
	case types.KindMysqlSet:
		b = encodeUnsignedInt(b, uint64(val.GetMysqlSet().ToNumber()))
	case types.KindNull:
		b = append(b, NilFlag)
	case types.KindMinNotNull:
		b = append(b, bytesFlag)
	case types.KindMaxValue:
		b = append(b, maxFlag)
	default:
		return nil, errors.Errorf("unsupport encode type %d", val.Kind())
	}
	return b, nil
}

func encodeBytes(b []byte, v []byte, comparable bool) []byte {
	if comparable {
		b = append(b, bytesFlag)
		b = EncodeBytes(b, v)
	} else {
		b = append(b, compactBytesFlag)
		b = EncodeCompactBytes(b, v)
	}
	return b
}

func encodeSignedInt(b []byte, v int64) []byte {
	b = append(b, varintFlag)
	b = EncodeComparableVarint(b, v)
	return b
}

func encodeUnsignedInt(b []byte, v uint64) []byte {
	b = append(b, uvarintFlag)
	b = EncodeComparableUvarint(b, v)
	return b
}

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
// For Decimal type, datum must set datum's length and frac.
func EncodeKey(b []byte, v ...types.Datum) ([]byte, error) {
	for _, val := range v {
		var err error
		b, err = encode(b, val, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return b, nil
}

// EncodeValue appends the encoded values to byte slice b, returning the appended
// slice. It does not guarantee the order for comparison.
func EncodeValue(b []byte, v ...types.Datum) ([]byte, error) {
	for _, val := range v {
		var err error
		b, err = encode(b, val, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return b, nil
}

// Decode decodes values from a byte slice generated with EncodeKey or EncodeValue
// before.
// size is the size of decoded datum slice.
func Decode(b []byte, size int) ([]types.Datum, error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}

	var (
		err    error
		values = make([]types.Datum, 0, size)
	)

	for len(b) > 0 {
		var d types.Datum
		b, d, err = DecodeOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}

		values = append(values, d)
	}

	return values, nil
}

// DecodeOne decodes on datum from a byte slice generated with EncodeKey or EncodeValue.
func DecodeOne(b []byte) (remain []byte, d types.Datum, err error) {
	if len(b) < 1 {
		return nil, d, errors.New("invalid encoded key")
	}
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		var v int64
		b, v, err = DecodeInt(b)
		d.SetInt64(v)
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		d.SetUint64(v)
	case varintFlag:
		var v int64
		b, v, err = DecodeComparableVarint(b)
		d.SetInt64(v)
	case uvarintFlag:
		var v uint64
		b, v, err = DecodeComparableUvarint(b)
		d.SetUint64(v)
	case floatFlag:
		var v float64
		b, v, err = DecodeFloat(b)
		d.SetFloat64(v)
	case bytesFlag:
		var v []byte
		b, v, err = DecodeBytes(b)
		d.SetBytes(v)
	case compactBytesFlag:
		var v []byte
		b, v, err = DecodeCompactBytes(b)
		d.SetBytes(v)
	case decimalFlag:
		b, d, err = DecodeDecimal(b)
	case durationFlag:
		var r int64
		b, r, err = DecodeInt(b)
		if err == nil {
			// use max fsp, let outer to do round manually.
			v := types.Duration{Duration: time.Duration(r), Fsp: types.MaxFsp}
			d.SetValue(v)
		}
	case NilFlag:
	default:
		return b, d, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return b, d, errors.Trace(err)
	}
	return b, d, nil
}

// CutOne cuts the first encoded value from b.
// It will return the first encoded item and the remains as byte slice.
func CutOne(b []byte) (data []byte, remain []byte, err error) {
	l, err := peek(b)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return b[:l], b[l:], nil
}

// peeks the first encoded value from b and returns its length.
func peek(b []byte) (length int, err error) {
	if len(b) < 1 {
		return 0, errors.New("invalid encoded key")
	}
	flag := b[0]
	length++
	b = b[1:]
	var l int
	switch flag {
	case NilFlag:
	case intFlag, uintFlag, floatFlag, durationFlag:
		// Those types are stored in 8 bytes.
		l = 8
	case bytesFlag:
		l, err = peekBytes(b, false)
	case compactBytesFlag:
		l, err = peekCompactBytes(b)
	case decimalFlag:
		l, err = types.DecimalPeak(b)
	case varintFlag, uvarintFlag:
		l, err = peekVarint(b)
	default:
		return 0, errors.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	length += l
	return
}

func peekComparableVarint(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("insufficient bytes to decode value")
	}
	first := int(b[0])
	if first < negativeTagEnd {
		return negativeTagEnd - first + 1, nil
	} else if first > positiveTagStart {
		return first - positiveTagStart + 1, nil
	}
	return 1, nil
}

func peekBytes(b []byte, reverse bool) (int, error) {
	offset := 0
	for {
		if len(b) < offset+encGroupSize+1 {
			return 0, errors.New("insufficient bytes to decode value")
		}
		// The byte slice is encoded into many groups.
		// For each group, there are 8 bytes for data and 1 byte for marker.
		marker := b[offset+encGroupSize]
		var padCount byte
		if reverse {
			padCount = marker
		} else {
			padCount = encMarker - marker
		}
		offset += encGroupSize + 1
		// When padCount is not zero, it means we get the end of the byte slice.
		if padCount != 0 {
			break
		}
	}
	return offset, nil
}

func peekCompactBytes(b []byte) (int, error) {
	// Get length.
	remained, v, err := DecodeComparableVarint(b)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if v < 0 {
		return 0, errors.Trace(errDecodeInvalid)
	}
	l := len(b) - len(remained) + int(v)
	if len(b) < l {
		return 0, errors.Trace(errDecodeInsufficient)
	}
	return l, nil
}

func peekVarint(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("insufficient bytes to decode value")
	}
	first := int(b[0])
	if first < negativeTagEnd {
		return negativeTagEnd - first + 1, nil
	} else if first > positiveTagStart {
		return first - positiveTagStart + 1, nil
	}
	return 1, nil
}
