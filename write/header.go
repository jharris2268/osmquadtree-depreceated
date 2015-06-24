// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package write

import (
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"
)

func packBbox(bbox *quadtree.Bbox) ([]byte, error) {
	msgs := make(utils.PbfMsgSlice, 4)
	msgs[0] = utils.PbfMsg{1, nil, utils.Zigzag(bbox.Minx * 100)}
	msgs[1] = utils.PbfMsg{2, nil, utils.Zigzag(bbox.Miny * 100)}
	msgs[2] = utils.PbfMsg{3, nil, utils.Zigzag(bbox.Maxx * 100)}
	msgs[3] = utils.PbfMsg{4, nil, utils.Zigzag(bbox.Maxy * 100)}
	return msgs.Pack(), nil
}
func packBlockIdx(bi BlockIdxWrite, i int) ([]byte, error) {
	msgs := make(utils.PbfMsgSlice, 3)
	q := packQuadtree(bi.Quadtree(i))
	ic := uint64(0)
	if bi.IsChange(i) {
		ic = 1
	}
	/*if err != nil {
		return nil, err
	}*/
	msgs[0] = utils.PbfMsg{1, q, 0}
	msgs[1] = utils.PbfMsg{2, nil, ic}
	msgs[2] = utils.PbfMsg{3, nil, utils.Zigzag(bi.BlockLen(i))}
	return msgs.Pack(), nil
}

type BlockIdxWrite interface {
	Len() int
	BlockLen(i int) int64
	Quadtree(i int) quadtree.Quadtree
	IsChange(i int) bool
}

func WriteHeaderBlock(bbox *quadtree.Bbox, idx BlockIdxWrite) ([]byte, error) {
	l := 3
	if bbox != nil {
		l += 1
	}
	if idx != nil {
		l += idx.Len()
	}
	msgs := make(utils.PbfMsgSlice, l)

	j := 0
	if bbox != nil {
		bb, err := packBbox(bbox)
		if err != nil {
			return nil, err
		}
		msgs[0] = utils.PbfMsg{1, bb, 0}
		j = 1
	}
	msgs[j] = utils.PbfMsg{4, []byte("OsmSchema-V0.6"), 0}
	msgs[j+1] = utils.PbfMsg{4, []byte("DenseNodes"), 0}
	msgs[j+2] = utils.PbfMsg{16, []byte("osmquadtree"), 0}
	j += 3
	if idx != nil {
		for i := 0; i < idx.Len(); i++ {

			p, err := packBlockIdx(idx, i)
			if err != nil {
				return nil, err
			}
			msgs[j+i] = utils.PbfMsg{22, p, 0}
		}
	}

	return msgs.Pack(), nil
}
