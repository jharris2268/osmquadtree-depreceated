// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package change

import (
	"github.com/jharris2268/osmquadtree/elements"
)

type origChangePair struct {
	idx  int
	orig elements.ExtendedBlock
	chg  elements.ExtendedBlock
}

func mergeOrigChange(idx int, orig elements.ExtendedBlock, chg elements.ExtendedBlock) elements.ExtendedBlock {

	if chg == nil || chg.Len() == 0 {
		// no change block: return orig
		return orig
	}

	if orig == nil || orig.Len() == 0 {
		// no orig block: drop deletes from change
		return elements.MakeExtendedBlock(idx, elements.AsNormalBlock(chg), chg.Quadtree(), chg.StartDate(), chg.EndDate(), chg.Tags())
	}

	if chg.Quadtree() != orig.Quadtree() {
		println("wtf", chg.Quadtree(), orig.Quadtree())
		panic(0)
	}

	objects := make(elements.ByElementId, 0, orig.Len()+chg.Len())

	ss := PairObjs(orig, chg)
	for s := ss(); s.Ok(); s = ss() {
		if s.B == nil {
			if s.A == nil {
				println("???")
			} else {
				// no change object: pick orig
				objects = append(objects, s.A)
			}
		} else if s.B.ChangeType() > 2 {
			// change object a move, modify or create, so pick thsi
			objects = append(objects, elements.AsNormal(s.B))
		} else {
			if s.B.ChangeType() == elements.Delete || s.B.ChangeType() == elements.Remove {
				// change object a delete or remove, so pick neither
			} else {
				// a normal object in the change block: shouldn't happen
				println("??", s.B.ChangeType() > 2, s.B.ChangeType(), s.B.String())
			}
		}
	}

	return elements.MakeExtendedBlock(idx, objects, orig.Quadtree(), 0, chg.EndDate(), orig.Tags())
}

// MergeOrigAndChange pairs input channels orig (eg. from a planet file)
// and cbs (change objects, perhaps the output of MergeChange) and applys
// change to orig. Returns ouput stream split in nc channels
func MergeOrigAndChange(orig <-chan elements.ExtendedBlock, cbs <-chan elements.ExtendedBlock, nc int) ([]chan elements.ExtendedBlock, error) {

	pp := make([]chan origChangePair, nc)
	for i, _ := range pp {
		pp[i] = make(chan origChangePair)
	}
	go func() {
		a, aok := <-orig
		b, bok := <-cbs

		i := 0
		for aok || bok {

			if !bok {
				// no changes left
				pp[i%nc] <- origChangePair{i, a, nil}
				a, aok = <-orig
			} else if !aok {
				// no origs left
				pp[i%nc] <- origChangePair{i, nil, b}
				b, bok = <-cbs
			} else if a.Quadtree() < b.Quadtree() {
				pp[i%nc] <- origChangePair{i, a, nil}
				a, aok = <-orig
			} else if a.Quadtree() > b.Quadtree() {
				pp[i%nc] <- origChangePair{i, nil, b}
				b, bok = <-cbs
			} else {
				pp[i%nc] <- origChangePair{i, a, b}
				a, aok = <-orig
				b, bok = <-cbs
			}
			i++
		}
		for _, p := range pp {
			close(p)
		}
	}()

	res := make([]chan elements.ExtendedBlock, nc)
	for i, _ := range res {
		res[i] = make(chan elements.ExtendedBlock)
		go func(i int) {
			for ocp := range pp[i] {
				rr := mergeOrigChange(ocp.idx, ocp.orig, ocp.chg)
				res[i] <- rr
			}
			close(res[i])
		}(i)
	}

	return res, nil
}
