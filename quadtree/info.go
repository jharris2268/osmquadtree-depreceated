// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package quadtree

import (
	"github.com/jharris2268/osmquadtree/utils"
)

type QuadtreeInfo struct {
	Qt                     Quadtree
	X, Y, Z                int64   // tile representation
	Minx, Miny, Maxx, Maxy float64 //bounding box
	Str                    string
	Children               []*QuadtreeInfo //any child nodes
	Parent                 string          //parent node
}

func makeQuadtreeInfo(qt Quadtree, p string) *QuadtreeInfo {
	x, y, z := qt.Tuple()
	bb := qt.Bounds(0.0)
	return &QuadtreeInfo{qt, x, y, z, utils.AsFloat(bb.Minx), utils.AsFloat(bb.Miny), utils.AsFloat(bb.Maxx), utils.AsFloat(bb.Maxy), qt.String(), []*QuadtreeInfo{}, p}
}

func (qi *QuadtreeInfo) Add(qt Quadtree) {
	if qi.Qt == qt {

		return
	}
	for _, q := range qi.Children {
		if qt.Round(uint(q.Qt&31)) == q.Qt {
			q.Add(qt)
			return
		}
	}
	qi.Children = append(qi.Children, makeQuadtreeInfo(qt, qi.Str))

}

// MakeQuadtreeInfo produces a QuadtreeInfo tree structure. This is used
// by the osmquadtree-filter example to show the quadtree structure on a
// map.
func MakeQuadtreeInfo(qts QuadtreeSlice) *QuadtreeInfo {
	root := makeQuadtreeInfo(qts[0], "NONE")

	for _, q := range qts[1:] {
		root.Add(q)

	}

	return root
}
