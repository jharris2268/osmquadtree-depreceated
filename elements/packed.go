// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package elements

import (
	"fmt"

	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"
)

func PackFullElement(fe FullElement, data []byte) []byte {
	var info, tags []byte
	if fe.Info() != nil {
		info = fe.Info().Pack()
	}
	if fe.Tags() != nil {
		tags = fe.Tags().Pack()
	}
	return PackElement(
		fe.Type(), fe.ChangeType(),
		fe.Id(), fe.Quadtree(),
		data, info, tags)
}

func PackElement(
	et ElementType, ct ChangeType,
	id Ref, qt quadtree.Quadtree,
	data []byte, info []byte, tags []byte) []byte {

	tl := 50 + len(data) + len(info) + len(tags)
	p := 0
	res := make([]byte, tl)
	res[0] = byte(et)                        // element type, 1 byte
	res[1] = byte(ct)                        // change type, 1 byte
	p = utils.WriteInt64(res, 2, int64(id))  //write id as fixed size
	p = utils.WriteVarint(res, p, int64(qt)) //qt as varint
	p = utils.WriteData(res, p, data)        //single 0 if not present
	p = utils.WriteData(res, p, info)        //single 0 if not present
	p = utils.WriteData(res, p, tags)        //single 0 if not present

	//a packed element can be anything from 14 bytes to several mb (really big geometries)
	return res[:p]
}

//A PackedElement is a []byte which contains one packed element
type PackedElement []byte

func (po PackedElement) Type() ElementType {
	return ElementType(po[0]) // extract from first byte
}

func (po PackedElement) ChangeType() ChangeType {
	return ChangeType(po[1]) // extract from second byte
}

func (po PackedElement) Id() Ref {
	r, _ := utils.ReadInt64([]byte(po), 2)
	return Ref(r) // extract from bytes 2-10
}

func (po PackedElement) Pack() []byte {
	return []byte(po) // do nothing
}

func (po PackedElement) String() string {
	return fmt.Sprintf("Packed %d %d %10d [%d bytes]", po.ChangeType(), po.Type(), po.Id(), len(po))
}

func packInfo(vs int64, ts Timestamp, cs Ref, ui int64, user string, visible bool) []byte {
	l := 50 + len(user)
	res := make([]byte, l)
	p := utils.WriteVarint(res, 0, vs)        //version
	p = utils.WriteVarint(res, p, int64(ts))  //timestamp (integer)
	p = utils.WriteVarint(res, p, int64(cs))  //changeset
	p = utils.WriteVarint(res, p, ui)         //user id
	p = utils.WriteData(res, p, []byte(user)) //user (utf-8 string)
	if !visible {
		res[p] = 0 //single 0 byte if visible tag is false
		p++
	}
	return res[:p]
}

func unpackInfo(buf []byte) (int64, Timestamp, Ref, int64, string, bool) {
	vs, ts, cs, ui, vv := int64(0), int64(0), int64(0), int64(0), true
	us := []byte{}
	p := 0
	vs, p = utils.ReadVarint(buf, p)
	ts, p = utils.ReadVarint(buf, p)
	cs, p = utils.ReadVarint(buf, p)
	ui, p = utils.ReadVarint(buf, p)
	us, p = utils.ReadData(buf, p)
	if p < len(buf) {
		vv = buf[p] != 0
	}
	return vs, Timestamp(ts), Ref(cs), ui, string(us), vv
}

func unpackTags(buf []byte) ([]string, []string) {
	l, p := utils.ReadVarint(buf, 0)
	keys := make([]string, l)
	vals := make([]string, l)
	for i, _ := range keys {
		s := []byte{}
		s, p = utils.ReadData(buf, p)
		keys[i] = string(s)
		s, p = utils.ReadData(buf, p)
		vals[i] = string(s)
	}

	return keys, vals
}

func packTags(keys, vals []string) []byte {
	tl := 10
	for i, k := range keys {
		tl += 10 + len([]byte(k)) + len([]byte(vals[i]))
	}

	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(len(keys))) //number of tags
	for i, k := range keys {
		p = utils.WriteData(res, p, []byte(k))       //key
		p = utils.WriteData(res, p, []byte(vals[i])) //value, repeated
	}
	return res[:p]
}
func PackTags(tags Tags) []byte {
	tl := 10
	for i := 0; i < tags.Len(); i++ {
		tl += 10 + len([]byte(tags.Key(i))) + len([]byte(tags.Value(i)))
	}

	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(tags.Len()))
	for i := 0; i < tags.Len(); i++ {
		p = utils.WriteData(res, p, []byte(tags.Key(i)))
		p = utils.WriteData(res, p, []byte(tags.Value(i)))
	}
	return res[:p]
}

//Pack longnitude and latitude as 32bit integers: this is bit enough to
//store locations in the openstreetmap standard of units 0.0000001 degree
func PackLonlat(ln int64, lt int64) []byte {
	res := make([]byte, 8)
	res[0] = byte((ln >> 24) & 255)
	res[1] = byte((ln >> 16) & 255)
	res[2] = byte((ln >> 8) & 255)
	res[3] = byte((ln) & 255)
	res[4] = byte((lt >> 24) & 255)
	res[5] = byte((lt >> 16) & 255)
	res[6] = byte((lt >> 8) & 255)
	res[7] = byte((lt) & 255)
	return res

}

func unpackLonlat(buf []byte) (int64, int64) {
	if len(buf) == 0 {
		return 0, 0
	}
	a := int64(buf[0]) << 24
	a |= int64(buf[1]) << 16
	a |= int64(buf[2]) << 8
	a |= int64(buf[3])
	if a > 2147483648 {
		a -= (int64(1) << 32)
	}

	b := int64(buf[4]) << 24
	b |= int64(buf[5]) << 16
	b |= int64(buf[6]) << 8
	b |= int64(buf[7])
	if b > 2147483648 {
		b -= (int64(1) << 32)
	}
	return a, b
}

func PackRefs(nn Refs) []byte {
	res := make([]byte, 10*(1+nn.Len()))
	p := utils.WriteVarint(res, 0, int64(nn.Len())) //number of node refs
	s := Ref(0)
	for i := 0; i < nn.Len(); i++ {
		p = utils.WriteVarint(res, p, int64(nn.Ref(i)-s)) // delta packed
		s = nn.Ref(i)
	}

	return res[:p]
}

func PackWayPoints(nn WayPoints) []byte {
	res := make([]byte, 30*(1+nn.Len()))
	p := utils.WriteVarint(res, 0, int64(nn.Len())) //number of node refs
	s := Ref(0)
	for i := 0; i < nn.Len(); i++ {
		p = utils.WriteVarint(res, p, int64(nn.Ref(i)-s)) // delta packed
		s = nn.Ref(i)
	}
    
    p = utils.WriteVarint(res, p, int64(nn.Len())) //number of node refs
	ln,lt:=int64(0),int64(0)
	for i := 0; i < nn.Len(); i++ {
        a,b := nn.LonLat(i)
		p = utils.WriteVarint(res, p, int64(a-ln)) // delta packed
		ln = a
        p = utils.WriteVarint(res, p, int64(b-lt)) // delta packed
		lt = b
	}
    

	return res[:p]
}

func PackRefSlice(nn []Ref) []byte {
	res := make([]byte, 10*(1+len(nn)))
	p := utils.WriteVarint(res, 0, int64(len(nn)))
	s := int64(0)
	for _, n := range nn {
		p = utils.WriteVarint(res, p, int64(n)-s)
		s = int64(n)
	}

	return res[:p]
}

func unpackRefs(buf []byte) ([]Ref,int) {
	if len(buf) == 0 {
		return nil,0
	}
	l, p := utils.ReadVarint(buf, 0)
	if l < 0 || l == 0 && len(buf) > p {
		return nil,0
	}
	ans := make([]Ref, l)
	n, np := int64(0), int64(0)

	for i := 0; i < int(l); i++ {
		np, p = utils.ReadVarint(buf, p)
		n += np
		ans[i] = Ref(n)
	}

	return ans, p

}

func unpackLonsLats(buf []byte) ([]int64, []int64) {
    
    if len(buf) == 0 {
		return nil,nil
	}
	l, p := utils.ReadVarint(buf, 0)
	if l < 0 || l == 0 && len(buf) > p {
		return nil,nil
	}
	lons,lats := make([]int64, l), make([]int64, l)
    ln,lt, nln,nlt := int64(0), int64(0), int64(0), int64(0)
    
    for i := 0; i < int(l); i++ {
		nln, p = utils.ReadVarint(buf, p)
		ln += nln
        nlt, p = utils.ReadVarint(buf, p)
		lt += nlt
		lons[i] = ln
        lats[i] = lt
	}
    return lons,lats
}

func PackMembers(mm Members) []byte {

	tl := 10 + 15*mm.Len()
	for i := 0; i < mm.Len(); i++ {
		tl += len(mm.Role(i))
	}
	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(mm.Len())) //number of members
	s := Ref(0)
	for i := 0; i < mm.Len(); i++ {
		p = utils.WriteVarint(res, p, int64(mm.MemberType(i)))
		//println("p",r.ref,r.ref-s)
		p = utils.WriteVarint(res, p, int64(mm.Ref(i)-s)) //delta packed
		s = mm.Ref(i)
		p = utils.WriteData(res, p, []byte(mm.Role(i)))

	}
	return res[:p]
}

func packMembers(rms []relMember) []byte {
	tl := 10 + 15*len(rms)
	for _, r := range rms {
		tl += len(r.role)
	}
	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(len(rms)))
	s := int64(0)
	for _, r := range rms {
		p = utils.WriteVarint(res, p, int64(r.memType))
		//println("p",r.ref,r.ref-s)
		p = utils.WriteVarint(res, p, int64(r.ref)-s)
		s = int64(r.ref)
		p = utils.WriteData(res, p, []byte(r.role))
	}
	return res[:p]
}

func unpackMembers(buf []byte) []relMember {
	if len(buf) == 0 {
		return nil
	}

	l, p := utils.ReadVarint(buf, 0)

	t := int64(0)
	s := int64(0)
	ss := int64(0)
	var rl []byte

	ans := make([]relMember, l)
	for i, _ := range ans {
		t, p = utils.ReadVarint(buf, p)
		ans[i].memType = ElementType(t)

		ss, p = utils.ReadVarint(buf, p)
		s += ss
		ans[i].ref = Ref(s)

		rl, p = utils.ReadData(buf, p)
		ans[i].role = string(rl)
	}
	return ans
}

//UnpackElement deserializes the input data into a FullElement, which
//can be converted to FullNode,FullWay,FullRelation or PackedGeometry
//depending on the value of FullElement.Type()
func UnpackElement(buf []byte) FullElement {
	et := ElementType(buf[0])
	ct := ChangeType(buf[1])
	idi, p := utils.ReadInt64(buf, 2)
	id := Ref(idi)

	qti, p := utils.ReadVarint(buf, p)
	qt := quadtree.Quadtree(qti)

	dt, p := utils.ReadData(buf, p)
	in, p := utils.ReadData(buf, p)
	tg, p := utils.ReadData(buf, p)

	if p != len(buf) {
		panic("not at end")
	}

	var info Info
	if in != nil {
		a, b, c, d, e, f := unpackInfo(in)
		info = &unpackedInfo{a, b, c, d, e, f}
	}
	var tags Tags
	if tg != nil {
		kk, vv := unpackTags(tg)
		tags = &unpackedTags{kk, vv}
	}

	switch et {
	case Node:

		ln, lt := int64(0), int64(0)
		if dt != nil {
			ln, lt = unpackLonlat(dt)
		}
		return &fullNode{id, ct, qt, ln, lt, info, tags}
	case Way:

		refs := []Ref{}
		if dt != nil {
            pos := 0
			refs,pos = unpackRefs(dt)
            if pos < len(dt) {
                lons,lats := unpackLonsLats(dt[pos:])
                return &fullWayPoints{id,ct, qt, refs, info, tags, lons,lats}
            }
		}
		return &fullWay{id, ct, qt, refs, info, tags}
	case Relation:
		mems := []relMember{}
		if dt != nil {
			mems = unpackMembers(dt)
		}
		return &fullRelation{id, ct, qt, mems, info, tags}
	case Geometry:
		return &packedGeometry{id, ct, qt, dt, info, tags}
	}

	panic("unknown element type")
}

type refSlice []Ref

func (rf refSlice) Len() int      { return len(rf) }
func (rf refSlice) Ref(i int) Ref { return rf[i] }

//UnpackQtRefs is used by update.CalcUpdateTiles as a faster alternative
//to UnpackElement, missing out the Info and Tags strings
func UnpackQtRefs(buf []byte) (ElementType, ChangeType, Ref, quadtree.Quadtree, Refs) {
	et := ElementType(buf[0])
	ct := ChangeType(buf[1])
	idi, p := utils.ReadInt64(buf, 2)
	id := Ref(idi)

	qti, p := utils.ReadVarint(buf, p)
	qt := quadtree.Quadtree(qti)

	dt, p := utils.ReadData(buf, p)
	refs := refSlice{}
	if dt != nil {
		switch et {
		case Node:
			ln, lt := unpackLonlat(dt)
			refs = refSlice{Ref(ln), Ref(lt)}

		case Way:

			refs,_ = unpackRefs(dt)

		case Relation:
			mems := unpackMembers(dt)
			refs = make(refSlice, len(mems))
			for i, m := range mems {
				refs[i] = (Ref(m.memType) << 59) | m.ref
			}
		}
	}
	return et, ct, id, qt, refs
}
