// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package write

import (
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"
)

func packQuadtree(qt quadtree.Quadtree) []byte {
	x, y, z := qt.Tuple()
	msgs := make(utils.PbfMsgSlice, 3)
	msgs[0] = utils.PbfMsg{1, nil, uint64(x)}
	msgs[1] = utils.PbfMsg{2, nil, uint64(y)}
	msgs[2] = utils.PbfMsg{3, nil, uint64(z)}
	return msgs.Pack()
}

func packTags(tags elements.Tags, stm map[string]int) ([]byte, []byte, error) {
	kk, vv := make([]uint64, tags.Len()), make([]uint64, tags.Len())
	var err error
	for i, _ := range kk {
		kk[i], err = getString(tags.Key(i), stm)
		if err != nil {
			return nil, nil, err
		}
		vv[i], err = getString(tags.Value(i), stm)
		if err != nil {
			return nil, nil, err
		}
	}
	a, err := utils.PackPackedList(kk)
	if err != nil {
		return nil, nil, err
	}
	b, err := utils.PackPackedList(vv)
	if err != nil {
		return nil, nil, err
	}
	return a, b, nil
}

func getString(s string, stm map[string]int) (uint64, error) {
	p, ok := stm[s]
	if !ok {
		p = len(stm)
		stm[s] = p

	}
	return uint64(p), nil
}

func packStringTable(stm map[string]int) ([]byte, error) {
	mm := make(utils.PbfMsgSlice, len(stm))
	for k, v := range stm {
		if v == 0 {
			mm[0] = utils.PbfMsg{1, []byte(""), 0}
		} else {
			mm[v] = utils.PbfMsg{1, []byte(k), uint64(v)}
		}
	}
	return mm.Pack(), nil
}

//WriteExtendedBlock serializes the elements in block into a Pbf format
//primitive block. If ischange is true, write changetypes; if writeExtra
//is true write extra block attributes (quadtree, startdate, enddate and
//tags, if present), and also write element quadtrees.
func WriteExtendedBlock(block elements.ExtendedBlock, ischange bool, writeExtra bool, qttup bool) ([]byte, error) {

	stm := map[string]int{"!!!ZZtrt": 0} //nonsense value stringtable starts at 1
	//n.b. an empty string might be a real value

	msgs, err := packBlock(block, stm, ischange, writeExtra,qttup)

	if err != nil {
		return nil, err
	}

	if writeExtra { // extensions to standard pbf format: skip if not present
		if block.Quadtree() != quadtree.Null {
			msgs = append(msgs, utils.PbfMsg{31, packQuadtree(block.Quadtree()), 0})
		}
		if block.StartDate() != 0 {
			msgs = append(msgs, utils.PbfMsg{33, nil, uint64(block.StartDate())})
		}
		if block.EndDate() != 0 {
			msgs = append(msgs, utils.PbfMsg{34, nil, uint64(block.EndDate())})
		}
		if block.Tags() != nil {
			kk, vv, err := packTags(block.Tags(), stm)
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, utils.PbfMsg{35, kk, 0})
			msgs = append(msgs, utils.PbfMsg{36, vv, 0})
		}
	}

	st, err := packStringTable(stm)
	if err != nil {
		return nil, err
	}
	msgs = append(msgs, utils.PbfMsg{1, st, 0})

	msgs.Sort()
	return msgs.Pack(), nil
}
