// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geometry

import (
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
)

// AddNodeParent adds tags to nodes (filtered by having a tag with key
// nodetag) within input chan inc to show the value of the parent way
// tag waytag. The new tag has the key parenttag. As one node can have
// several parent values, the proccessParentValue is called to find the
// correct value. Ways, relations and unaffected nodes are immeadiatly
// written to the output channel. Affected nodes are grouped by quadtree
// and written to the output channel when no more ways can be found. Note
// that this means the output channel is not in strict quadtree block order,
// but this will not affect the behaviour of AddRelationRange.
// AddNodeParent can be used, for example, to add the parent way highway
// type for nodes with a highway tag. FindParentHighway should be used
// for proccesParentValue. This can then replace the geospatial join for
// rendering turning circles.
func AddNodeParent(inc <-chan elements.ExtendedBlock, processParentValue func([]string) string, nodetag, waytag, parenttag string) <-chan elements.ExtendedBlock {

	res := make(chan elements.ExtendedBlock)

	go func() {

		nodes := map[quadtree.Quadtree][]elements.FullNode{}
		ss := map[elements.Ref][]string{} //nodes waiting parent values
		idx := 0
		for bl := range inc {
			nn := make([]elements.FullNode, 0, 25)
			bq := bl.Quadtree()
			nb := make(elements.ByElementId, 0, bl.Len()) // block to pass on
			for i := 0; i < bl.Len(); i++ {
				e := bl.Element(i)
				switch e.Type() {
				case elements.Node:
					fn := e.(elements.FullNode)
					tt, ok := fn.Tags().(TagsEditable)
					if !ok {
						panic("unconverted tags")
					}
					if tt.Has(nodetag) {
						nn = append(nn, fn)
						ss[fn.Id()] = make([]string, 0, 5)
					} else {
						nb = append(nb, e) //pass on
					}

				case elements.Way:
					fw := e.(elements.FullWay)
					tt, ok := fw.Tags().(TagsEditable)
					if !ok {
						panic("unconverted tags")
					}
					if tt.Has(waytag) {
						h := tt.Get(waytag)
						for i := 0; i < fw.Len(); i++ {
							n := fw.Ref(i)
							if _, ok := ss[n]; ok {
								//add to pending map
								ss[n] = append(ss[n], h)
							}
						}
					}
					nb = append(nb, e)
				default:
					nb = append(nb, e)
				}
			}
			res <- elements.MakeExtendedBlock(idx, nb, bq, bl.StartDate(), bl.EndDate(), nil)
			idx++

			nodes[bq] = nn
			ds := make([]quadtree.Quadtree, 0, len(nodes))
			for k, v := range nodes {
				if k.Common(bq) != k {
					// we won't find any more parent ways, so we can
					// add the new tags and send the nodes block to the
					// output channel
					r := make(elements.ByElementId, len(v))
					for i, n := range v {
						hw := processParentValue(ss[n.Id()])
						if hw != "" {
							n.Tags().(TagsEditable).Put(parenttag, hw)
						}
						r[i] = n
						delete(ss, n.Id())
					}

					if len(v) > 0 {
						res <- elements.MakeExtendedBlock(idx, r, k, 0, 0, nil)
						idx++
					}
					ds = append(ds, k)
				}
			}
			for _, k := range ds {
				delete(nodes, k)
			}
		}

		for k, v := range nodes {
			r := make(elements.ByElementId, len(v))
			for i, n := range v {
				hw := processParentValue(ss[n.Id()])

				if hw != "" {
					n.Tags().(TagsEditable).Put(parenttag, hw)
				}
				r[i] = n
				delete(ss, n.Id())
			}
			if len(v) > 0 {
				res <- elements.MakeExtendedBlock(idx, r, k, 0, 0, nil)
			}

		}

		close(res)
	}()
	return res
}
