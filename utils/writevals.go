// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package utils

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
)

func Zigzag(x int64) uint64 {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return ux
}

func UnZigzag(ux uint64) int64 {
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x
}

func WriteVarint(data []byte, pos int, val int64) int {
	return WriteUvarint(data, pos, Zigzag(val))
}

func WriteUvarint(data []byte, pos int, val uint64) int {
	p := binary.PutUvarint(data[pos:], val)
	return pos + p
}

func WriteData(data []byte, pos int, val []byte) int {
	pos = WriteUvarint(data, pos, uint64(len(val)))
	copy(data[pos:], val)
	return pos + len(val)
}

func ReadVarint(data []byte, pos int) (int64, int) {
	v, p := ReadUvarint(data, pos)
	return UnZigzag(v), p
}

func ReadUvarint(data []byte, pos int) (uint64, int) {
	v, p := binary.Uvarint(data[pos:])
	return v, pos + p
}

func ReadData(data []byte, pos int) ([]byte, int) {
	l, pos := ReadUvarint(data, pos)
	return data[pos : pos+int(l)], pos + int(l)
}

func WriteInt64(data []byte, pos int, val int64) int {
	binary.BigEndian.PutUint64(data[pos:], uint64(val))
	return pos + 8
}

func ReadInt64(data []byte, pos int) (int64, int) {
	v := binary.BigEndian.Uint64(data[pos:])
	return int64(v), pos + 8
}

func WriteFloat64(data []byte, pos int, val float64) int {
	bb := math.Float64bits(val)
	binary.BigEndian.PutUint64(data[pos:], bb)
	return pos + 8
}

func ReadFloat64(data []byte, pos int) (float64, int) {
	v := binary.BigEndian.Uint64(data[pos:])
	bb := math.Float64frombits(v)

	return bb, pos + 8
}

func WriteInt32(data []byte, pos int, val int32) int {
	binary.BigEndian.PutUint32(data[pos:], uint32(val))
	return pos + 4
}
func ReadInt32(data []byte, pos int) (int32, int) {
	v := binary.BigEndian.Uint32(data[pos:])
	return int32(v), pos + 4
}

func WriteInt16(data []byte, pos int, val int16) int {
	binary.BigEndian.PutUint16(data[pos:], uint16(val))
	return pos + 2
}
func ReadInt16(data []byte, pos int) (int16, int) {
	v := binary.BigEndian.Uint16(data[pos:])
	return int16(v), pos + 2
}

type PbfMsg struct {
	Tag   uint64
	Data  []byte
	Value uint64
}

type PbfMsgSlice []PbfMsg

func (m PbfMsgSlice) Len() int      { return len(m) }
func (m PbfMsgSlice) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m PbfMsgSlice) Less(i, j int) bool {
	if m[i].Tag == m[j].Tag {
		return m[i].Value < m[j].Value
	}
	return m[i].Tag < m[j].Tag
}

func (msgs PbfMsgSlice) Sort() { sort.Sort(msgs) }

func (msgs PbfMsgSlice) Pack() []byte {
	tl := 0
	for _, m := range msgs {
		tl += 13
		if m.Data != nil {
			tl += len(m.Data)
		}
	}
	res := make([]byte, tl)
	p := 0
	for _, m := range msgs {
		if m.Data != nil {
			res, p = WritePbfData(res, p, m.Tag, m.Data)
		} else {
			res, p = WritePbfVarint(res, p, m.Tag, m.Value)
		}
	}
	return res[:p]
}

func ReadPbfTag(buf []byte, pos int) (int, PbfMsg) {
	a, pos := ReadUvarint(buf, pos)
	ty := a & 7
	tg := (a >> 3)

	if ty == 0 {
		val, pos := ReadUvarint(buf, pos)
		return pos, PbfMsg{tg, nil, val}
	}
	if ty == 2 {
		data, pos := ReadData(buf, pos)
		return pos, PbfMsg{tg, data, 0}
	}
	panic(fmt.Sprintf("Unsupported message type %d", ty))
	return pos, PbfMsg{}
}

func ReadPbfTagSlice(buf []byte) PbfMsgSlice {
	ans := make(PbfMsgSlice, 0, 25)
	pos, tg := ReadPbfTag(buf, 0)
	for ; tg.Tag > 0; pos, tg = ReadPbfTag(buf, pos) {
		ans = append(ans, tg)
	}
	return ans
}

func WritePbfData(data []byte, pos int, tag uint64, blob []byte) ([]byte, int) {
	tg := uint64(2) | (tag << 3)
	pos += binary.PutUvarint(data[pos:], tg)
	pos += binary.PutUvarint(data[pos:], uint64(len(blob)))
	copy(data[pos:], blob)
	return data, pos + len(blob)
}

func WritePbfVarint(data []byte, pos int, tag uint64, val uint64) ([]byte, int) {
	tg := uint64(0) | (tag << 3)
	pos += binary.PutUvarint(data[pos:], tg)
	pos += binary.PutUvarint(data[pos:], val)
	return data, pos
}

func PackDeltaPackedList(vals []int64) ([]byte, error) {

	res := make([]byte, len(vals)*10)
	p := 0
	ls := int64(0)
	for _, v := range vals {
		p = WriteVarint(res, p, v-ls)
		ls = v
	}

	return res[:p], nil
}

func PackPackedList(vals []uint64) ([]byte, error) {
	res := make([]byte, len(vals)*10)
	p := 0
	for _, v := range vals {
		p = WriteUvarint(res, p, v)

	}

	return res[:p], nil
}

func ReadDeltaPackedList(data []byte) ([]int64, error) {
	pos := 0
	v, val := int64(0), int64(0)
	res := make([]int64, 0, len(data)/3)
	for pos < len(data) {
		v, pos = ReadVarint(data, pos)
		val += v
		res = append(res, val)
	}
	return res, nil
}

func ReadPackedList(data []byte) ([]uint64, error) {
	pos := 0
	val := uint64(0)
	res := make([]uint64, 0, len(data)/3)
	for pos < len(data) {
		val, pos = ReadUvarint(data, pos)
		res = append(res, val)
	}
	return res, nil
}

func Intm(f float64) int64 {
	if f > 0 {
		return int64(f*10000000 + 0.5)
	}
	return int64(f*10000000 - 0.5)
}
func AsFloat(i int64) float64 {
	return float64(i) * 0.0000001
}

func ParseStringInt(ins string) (int64, bool, error) {
	wasi := true
	ans, err := strconv.ParseInt(ins, 10, 64)
	if err != nil {
		fa := 0.0
		fa, err = strconv.ParseFloat(ins, 64)
		if err == nil {
			ans = Intm(fa)
			wasi = false
		}
	}

	return ans, wasi, err
}

type Int64Slice []int64

func (sob Int64Slice) Len() int           { return len(sob) }
func (sob Int64Slice) Swap(i, j int)      { sob[i], sob[j] = sob[j], sob[i] }
func (sob Int64Slice) Less(i, j int) bool { return sob[i] < sob[j] }
func (sob Int64Slice) Sort()              { sort.Sort(sob) }
func (sob Int64Slice) IsSorted() bool     { return sort.IsSorted(sob) }
func (sob Int64Slice) ReverseSort()       { sort.Sort(sort.Reverse(sob)) }
