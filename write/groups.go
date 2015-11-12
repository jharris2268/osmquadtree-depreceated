// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package write

import (
	"errors"

	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"
)

var WrongTypeErr = errors.New("Wrong Type")

type intPair struct {
	f, t int
	ct   elements.ChangeType
}

func groupElements(bl elements.Block, ischange bool) []intPair {
	getct := func(i int) elements.ChangeType { return elements.Normal }
	if ischange {
		getct = func(i int) elements.ChangeType { return bl.Element(i).ChangeType() }
	}
	getty := func(i int) elements.ElementType { return bl.Element(i).Type() }

	if bl == nil || bl.Len() == 0 {
		return nil
	}
	el := 3
	if ischange {
		el = bl.Len() / 5
	}

	ans := make([]intPair, 0, el)

	li := 0

	for li < bl.Len() {
		ct, ty := getct(li), getty(li)
		i := li + 1

		for i < bl.Len() && getct(i) == ct && getty(i) == ty {
			i++
		}
		ans = append(ans, intPair{li, i, ct})
		li = i
	}

	return ans
}

func dropNils(bl elements.Block) elements.Block {
	nb := make(elements.ByElementId, 0, bl.Len())
	hasn := false
	for i := 0; i < bl.Len(); i++ {
		e := bl.Element(i)
		if e == nil {
			hasn = true
			ble, ok := bl.(elements.ExtendedBlock)
			if ok {
				println("have null: ", ble.Idx(), " ", ble.Quadtree().String(), ": ", i, "/", bl.Len())
			} else {
				println("have null: ", i, "/", bl.Len())
			}
		} else {
			nb = append(nb, e)
		}
	}
	if hasn {
		return nb
	}
	return bl
}

func packBlock(bl elements.Block, stm map[string]int, ischange bool, writeExtra bool, qttup bool) (utils.PbfMsgSlice, error) {
	bl2 := dropNils(bl)

	ss := groupElements(bl2, ischange)
	ans := make(utils.PbfMsgSlice, 0, len(ss)+10)

	for i, s := range ss {
		pg, err := packGroup(bl2, stm, s.f, s.t, s.ct, writeExtra, qttup)
		if err != nil {
			return nil, err
		}
		ans = append(ans, utils.PbfMsg{2, pg, uint64(i)})
	}

	return ans, nil
}

func denseTags(tags elements.Tags, stm map[string]int, kvs []uint64) []uint64 {

	for i := 0; i < tags.Len(); i++ {
		k, _ := getString(tags.Key(i), stm)
		kvs = append(kvs, k)
		v, _ := getString(tags.Value(i), stm)
		kvs = append(kvs, v)
	}
	kvs = append(kvs, 0)
	return kvs
}

func packGroup(bl elements.Block, stm map[string]int, from int, to int, ct elements.ChangeType, writeExtra bool, qttup bool) ([]byte, error) {
	l := 1
	if bl.Element(from).Type() != elements.Node {
		l = bl.Len()
	}
	if ct != elements.Normal {
		l++
	}

	mm := make(utils.PbfMsgSlice, 0, l)

	if bl.Element(from).Type() == elements.Node {
		dd, err := packDense(bl, stm, from, to, writeExtra, qttup)
		if err != nil {
			return nil, err
		}
		mm = append(mm, utils.PbfMsg{2, dd, 0})
	} else {
		for i := from; i < to; i++ {
			e := bl.Element(i)
			t, pp, err := packElement(e, stm, writeExtra, qttup)
			if err != nil {
				return nil, err
			}
			if t != 0 {
				mm = append(mm, utils.PbfMsg{t, pp, uint64(i - from)})
			}
		}
	}

	if ct != elements.Normal {
		mm = append(mm, utils.PbfMsg{10, nil, uint64(ct)})
	}
	return mm.Pack(), nil
}
func getStringI(s string, stm map[string]int) int64 {
	a, _ := getString(s, stm)
	return int64(a)
}

func packDense(bl elements.Block, stm map[string]int, from int, to int, writeExtra bool, qttup bool) ([]byte, error) {

	mki := func() []int64 { return make([]int64, to-from) }
	mku := func() []uint64 { return make([]uint64, to-from) }

	ii, ln, lt, qt := mki(),  mki(), mki(), mki()
    
	kvs := make([]uint64, 0, (to-from)*5)
	i_vs, i_ts, i_cs, i_ui, i_us, i_vv := mku(), mki(), mki(), mki(), mki(), mku()
	all_visible := true
	if !writeExtra {
		qt = nil
        
	}

	for i, _ := range ii {
		e := bl.Element(from + i)

		if e.Type() != elements.Node {
			return nil, WrongTypeErr
		}

		ii[i] = int64(e.Id())
		if writeExtra && (i == 0 || qt != nil) {
			q, ok := e.(interface {
				Quadtree() quadtree.Quadtree
			})
			if ok {
                //qx[i],qy[i],qz[i] = q.Quadtree().Tuple()
				qt[i] = int64(q.Quadtree())
			} else {
				qt = nil
                //qx,qy,qz = nil,nil,nil
			}
		}
		if i == 0 || ln != nil {
			lnlt, ok := e.(elements.LonLat)
			if ok {
				ln[i] = lnlt.Lon()
				lt[i] = lnlt.Lat()
			} else {
				ln = nil
				lt = nil
			}
		}

		if i == 0 || i_vs != nil {
			fe, ok := e.(interface {
				Tags() elements.Tags
			})
			if ok && fe.Tags() != nil {
				kvs = denseTags(fe.Tags(), stm, kvs)
			} else {
				kvs = nil
			}
		}
		if i == 0 || i_vs != nil {
			fe, ok := e.(interface {
				Info() elements.Info
			})
			if ok && fe.Info() != nil && i_vs != nil {
				i_vs[i] = uint64(fe.Info().Version())
				i_ts[i] = int64(fe.Info().Timestamp())
				i_cs[i] = int64(fe.Info().Changeset())
				i_ui[i] = int64(fe.Info().Uid())
				i_us[i] = getStringI(fe.Info().User(), stm)
				if fe.Info().Visible() {
					i_vv[i] = 1
				} else {
					all_visible = false
				}
			} else {
				i_vs = nil
				i_ts = nil
				i_cs = nil
				i_ui = nil
				i_us = nil
				i_vv = nil
			}
		}
	}

	var info []byte
	if i_vs != nil {
		infi := make(utils.PbfMsgSlice, 5)
		for j, _ := range infi {
			infi[j].Tag = uint64(j + 1)
		}
		infi[0].Data, _ = utils.PackPackedList(i_vs)
		infi[1].Data, _ = utils.PackDeltaPackedList(i_ts)
		infi[2].Data, _ = utils.PackDeltaPackedList(i_cs)
		infi[3].Data, _ = utils.PackDeltaPackedList(i_ui)
		infi[4].Data, _ = utils.PackDeltaPackedList(i_us)
		if !all_visible {
			vvp, _ := utils.PackPackedList(i_vv)
			infi = append(infi, utils.PbfMsg{6, vvp, 0})
		}
		info = infi.Pack()
	}

	msgs := make(utils.PbfMsgSlice, 0, 6)
	iip, _ := utils.PackDeltaPackedList(ii)
	msgs = append(msgs, utils.PbfMsg{1, iip, 0})
	if info != nil {
		msgs = append(msgs, utils.PbfMsg{5, info, 0})
	}
	if ln == nil {
		lt = make([]int64, len(ii))
		ln = make([]int64, len(ii))
	}
	ltp, _ := utils.PackDeltaPackedList(lt)
	lnp, _ := utils.PackDeltaPackedList(ln)
	msgs = append(msgs, utils.PbfMsg{8, ltp, 0})
	msgs = append(msgs, utils.PbfMsg{9, lnp, 0})
	//}
	if kvs != nil {
		kvsp, _ := utils.PackPackedList(kvs)
		msgs = append(msgs, utils.PbfMsg{10, kvsp, 0})
	}
	if qt != nil {
        if qttup {
            qx := make([]int64,len(qt))
            qy := make([]int64,len(qt))
            qz := make([]int64,len(qt))
            for i,q:=range qt {
                qx[i],qy[i],qz[i] = quadtree.Quadtree(q).Tuple()
            }
            qtp, _ := utils.PackDeltaPackedList(qx)
            msgs = append(msgs, utils.PbfMsg{21, qtp, 0})
            
            qtp, _ = utils.PackDeltaPackedList(qy)
            msgs = append(msgs, utils.PbfMsg{22, qtp, 0})
            
            qtp, _ = utils.PackDeltaPackedList(qz)
            msgs = append(msgs, utils.PbfMsg{23, qtp, 0})
        } else {
            
            qtp, _ := utils.PackDeltaPackedList(qt)
            msgs = append(msgs, utils.PbfMsg{20, qtp, 0})
        }
	}
        
	return msgs.Pack(), nil
}

func packElement(e elements.Element, stm map[string]int, writeExtra bool, qttup bool) (uint64, []byte, error) {

	msgs := make(utils.PbfMsgSlice, 0, 8)
	msgs = append(msgs, utils.PbfMsg{1, nil, uint64(e.Id())})

	tt, ok := e.(interface {
		Tags() elements.Tags
	})
	if ok {
		kk, vv, err := packTags(tt.Tags(), stm)
		if err != nil {
			return 0, nil, err
		}

		msgs = append(msgs, utils.PbfMsg{2, kk, 0})
		msgs = append(msgs, utils.PbfMsg{3, vv, 0})

	}
	ei, ok := e.(interface {
		Info() elements.Info
	})
	if ok && ei.Info() != nil {

		ii := make(utils.PbfMsgSlice, 0, 6)
		ii = append(ii, utils.PbfMsg{1, nil, uint64(ei.Info().Version())}) // NOT zigzag encoded
		ii = append(ii, utils.PbfMsg{2, nil, uint64(ei.Info().Timestamp())})
		ii = append(ii, utils.PbfMsg{3, nil, uint64(ei.Info().Changeset())})
		ii = append(ii, utils.PbfMsg{4, nil, uint64(ei.Info().Uid())})
		us, _ := getString(ei.Info().User(), stm)
		ii = append(ii, utils.PbfMsg{5, nil, us})
		if !ei.Info().Visible() {
			ii = append(ii, utils.PbfMsg{6, nil, 0})
		}

		msgs = append(msgs, utils.PbfMsg{4, ii.Pack(), 0})
	}
	if writeExtra {
		qt, ok := e.(interface {
			Quadtree() quadtree.Quadtree
		})
		if ok && qt.Quadtree() != quadtree.Null {
            if qttup {
                msgs = append(msgs, utils.PbfMsg{21, packQuadtree(qt.Quadtree()), 0})
            } else {
                msgs = append(msgs, utils.PbfMsg{20, nil, utils.Zigzag(int64(qt.Quadtree()))})
            }
            
		}
	}

	switch e.Type() {
	case elements.Node:
		lnlt, ok := e.(elements.LonLat)
		if ok {
			msgs = append(msgs, utils.PbfMsg{8, nil, utils.Zigzag(lnlt.Lat())})
			msgs = append(msgs, utils.PbfMsg{9, nil, utils.Zigzag(lnlt.Lon())})
		}
		msgs.Sort()
		return 1, msgs.Pack(), nil
	case elements.Way:
		rr, ok := e.(elements.Refs)
		if ok {
			rrp := packRefs(rr)
			msgs = append(msgs, utils.PbfMsg{8, rrp, 0})
		}
		msgs.Sort()
		return 3, msgs.Pack(), nil
	case elements.Relation:
		mm, ok := e.(elements.Members)
		if ok {
			a, b, c := packMembers(mm, stm)
			if a != nil {
				msgs = append(msgs, utils.PbfMsg{8, a, 0})
			}
			msgs = append(msgs, utils.PbfMsg{9, b, 0})
			msgs = append(msgs, utils.PbfMsg{10, c, 0})
		}
		msgs.Sort()
		return 4, msgs.Pack(), nil
	case elements.Geometry:
		gg, ok := e.(elements.PackedGeometry)
		if ok {

			mm := utils.ReadPbfTagSlice(gg.GeometryData())
			if mm != nil {
				msgs = append(msgs, mm...)
			}
		}
		msgs.Sort()
		return 20, msgs.Pack(), nil
	}

	return 0, nil, WrongTypeErr
}

func packRefs(rr elements.Refs) []byte {
	ii := make([]int64, rr.Len())
	for i, _ := range ii {
		ii[i] = int64(rr.Ref(i))
	}
	pp, _ := utils.PackDeltaPackedList(ii)
	return pp
}

func packMembers(mm elements.Members, stm map[string]int) ([]byte, []byte, []byte) {
	ii := make([]int64, mm.Len())
	ty := make([]uint64, mm.Len())
	rl := make([]uint64, mm.Len())

	for i, _ := range ii {
		ii[i] = int64(mm.Ref(i)) //delta packed
		ty[i] = uint64(mm.MemberType(i))
		rl[i], _ = getString(mm.Role(i), stm)
	}
	b, _ := utils.PackDeltaPackedList(ii)
	c, _ := utils.PackPackedList(ty)
	a, _ := utils.PackPackedList(rl)
	return a, b, c
}
