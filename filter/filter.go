// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package filter

import (
	//"github.com/jharris2268/osmquadtree/readfile"
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"

	"log"
	"strings"

	//"sync"
)

func filterByQuadtree(inchan <-chan elements.ExtendedBlock, bbox quadtree.Bbox) (<-chan elements.ExtendedBlock, error) {

	res := make(chan elements.ExtendedBlock)
	go func() {
		for bl := range res {
			if bbox.Intersects(bl.Quadtree().Bounds(0.05)) {
				res <- bl
			}
		}
	}()
	return res, nil
}

func nodePresent(wn elements.Refs, ids IdSet) bool {
	for i := 0; i < wn.Len(); i++ {
		if ids.Contains(0, wn.Ref(i)) {
			return true
		}
	}
	return false
}

func memberPresent(mm elements.Members, ids IdSet) bool {
	for i := 0; i < mm.Len(); i++ {
		if ids.Contains(mm.MemberType(i), mm.Ref(i)) {
			return true
		}
	}
	return false
}

func addOthers(wn elements.Refs, ids IdSet, wns IdSet) {
	for i := 0; i < wn.Len(); i++ {
		r := wn.Ref(i)
		if !ids.Contains(0, r) {
			wns.Add(0, r)
		}
	}

}

type IdSet interface {
	Add(elements.ElementType, elements.Ref)
	Contains(elements.ElementType, elements.Ref) bool
	Len() int
}

//A LocTest is a spatial filter, such as a simple Bbox or a Polygon
type LocTest interface {
	//smallest rectangle covering given area
	Bbox() quadtree.Bbox

	//return True if node loc in area
	Contains(int64, int64) bool

	//return True if quadtree bounds entirely within given area
	ContainsQuadtree(quadtree.Quadtree) bool

	//return True if bbox intersects given area
	Intersects(bx quadtree.Bbox) bool

	//return True if quadtree bounds intersect given area
	IntersectsQuadtree(quadtree.Quadtree) bool
	String() string
}

func makekey(t elements.ElementType, i elements.Ref) int64 {
	return int64(t)<<59 | int64(i)
}

type idSetMap map[int64]bool

func (ids *idSetMap) Add(t elements.ElementType, i elements.Ref) {
	(*ids)[makekey(t, i)] = true
}

func (ids *idSetMap) Contains(t elements.ElementType, i elements.Ref) bool {
	_, ok := (*ids)[makekey(t, i)]
	return ok
}
func (ids *idSetMap) Len() int { return len(*ids) }

type idSetBitMap struct {
	mp map[int64]*[1024]uint64
	cc int
}

func (ids *idSetBitMap) Add(t elements.ElementType, i elements.Ref) {
	k := makekey(t, i)
	l := k / 65536
	if _, ok := ids.mp[l]; !ok {
		ids.mp[l] = &[1024]uint64{}
	}
	s := uint(k & 65535)

	a := s / 64
	b := uint64(1) << (s & 63)
	if (ids.mp[l][a] & b) == 0 {
		ids.cc++
	}
	ids.mp[l][a] |= b
}
func (ids *idSetBitMap) Len() int { return ids.cc }
func (ids *idSetBitMap) Contains(t elements.ElementType, i elements.Ref) bool {
	k := makekey(t, i)
	l := k / 65536
	if _, ok := ids.mp[l]; !ok {
		return false
	}
	s := uint(k & 65535)

	a := s / 64
	b := uint64(1) << (s & 63)
	return (ids.mp[l][a] & b) != 0
}

type locTestBbox quadtree.Bbox

func (bbox locTestBbox) Contains(x, y int64) bool {

	if bbox.Minx <= x && x <= bbox.Maxx {
		if bbox.Miny <= y && y <= bbox.Maxy {
			return true
		}
	}
	return false
}
func (bbox locTestBbox) ContainsQuadtree(qt quadtree.Quadtree) bool {
	bx := qt.Bounds(0.05)
	return quadtree.Bbox(bbox).Contains(bx)
}

func (bbox locTestBbox) Bbox() quadtree.Bbox {
	return quadtree.Bbox(bbox)
}
func (bbox locTestBbox) Intersects(other quadtree.Bbox) bool {
	return bbox.Bbox().Intersects(other)
}
func (bbox locTestBbox) IntersectsQuadtree(qt quadtree.Quadtree) bool {
	bx := qt.Bounds(0.05)
	return bbox.Intersects(bx)
}

func (bb locTestBbox) String() string {
	return "locTestBbox " + bb.Bbox().String()
}
func AsLocTest(bbox quadtree.Bbox) LocTest {
	return locTestBbox(bbox)
}

func readBbox(f string) *quadtree.Bbox {
	if f == "planet" {
		return quadtree.PlanetBbox()
	}
	t := strings.Split(f, ",")
	if len(t) != 4 {
		return quadtree.NullBbox()
	}

	mx, _, err := utils.ParseStringInt(t[0])
	if err != nil {
		return quadtree.NullBbox()
	}
	my, _, err := utils.ParseStringInt(t[1])
	if err != nil {
		return quadtree.NullBbox()
	}
	Mx, _, err := utils.ParseStringInt(t[2])
	if err != nil {
		return quadtree.NullBbox()
	}
	My, _, err := utils.ParseStringInt(t[3])
	if err != nil {
		return quadtree.NullBbox()
	}
	return &quadtree.Bbox{mx, my, Mx, My}

}

func MakeLocTest(f string) LocTest {
	if strings.HasSuffix(f, ".poly") {
		locTest, err := ReadPolyFile(f)
		if err != nil {
			panic(err.Error())
		}
		return locTest
	}

	if f != "" {
		fbx := readBbox(f)
		log.Println(fbx)
		return AsLocTest(*fbx)
	}
	fbx := quadtree.PlanetBbox()
	return AsLocTest(*fbx)
}

func MakeIdSet(bm bool) IdSet {
	if bm {
		return &idSetBitMap{map[int64]*[1024]uint64{}, 0}
	}
	return &idSetMap{}
}

//FindObjsFilter populates the given IdSet ids with the ids for elements
//in inblocks which pass the LocTest locTest. These elements are:
//1. Nodes within the the locTest
//2. Ways with at least one node within the locTest
//3. Other nodes belonging to Ways which are included (equilivant to osmosis' --complete-ways)
//4. Relations with at least on member within the locTest
func FindObjsFilter(inblocks <-chan elements.ExtendedBlock, locTest LocTest, ids IdSet) error {

	wns := &idSetMap{} //track other nodes: point 3 above

	rls := map[elements.Ref]elements.Members{}

	for bl := range inblocks {
		qq := locTest.ContainsQuadtree(bl.Quadtree())
		for i := 0; i < bl.Len(); i++ {

			o := bl.Element(i)
			if o.ChangeType() == 1 || o.ChangeType() == 2 {
				continue
			}
			switch o.Type() {
			case elements.Node:

				lc := o.(elements.LonLat)
				if qq || locTest.Contains(lc.Lon(), lc.Lat()) {
					ids.Add(0, o.Id())
				}
			case elements.Way:
				wn := o.(elements.Refs)
				if qq || nodePresent(wn, ids) {
					ids.Add(1, o.Id())
					addOthers(wn, ids, wns) //nodes not already included
				}
			case elements.Relation:
				mm := o.(elements.Members)
				if memberPresent(mm, ids) {
					ids.Add(2, o.Id())
				} else {

					rls[o.Id()] = mm //double check relations later
				}
			}
		}
	}
	println(len(rls), "pending rels,", len(*wns), "extra way nodes")
	for i := 0; i < 5; i++ {
		rl2 := map[elements.Ref]elements.Members{}

		for oi, mm := range rls {
			if memberPresent(mm, ids) {
				ids.Add(2, oi)
			} else {
				rl2[oi] = mm //check again next time round
			}
		}
		rls = rl2
	}

	for k, _ := range *wns {
		ids.Add(0, elements.Ref(k)) //add other nodes
	}

	return nil
}

func filterRelMembers(o elements.Element, ids IdSet) elements.Element {
	rel := o.(elements.FullElement)
	mm := rel.(elements.Members)
	if mm.Len() == 0 && o.ChangeType() == 0 {
		return nil
	}
	rr, tt, rl := make([]elements.Ref, 0, mm.Len()), make([]elements.ElementType, 0, mm.Len()), make([]string, 0, mm.Len())
	for i := 0; i < mm.Len(); i++ {
		if ids.Contains(mm.MemberType(i), mm.Ref(i)) { //only add if id within ids
			rr = append(rr, mm.Ref(i))
			tt = append(tt, mm.MemberType(i))
			rl = append(rl, mm.Role(i))
		}
	}
	if len(rr) == mm.Len() {
		return o //no change, so return input relation
	}
	if len(rr) == 0 && o.ChangeType() == 0 {
		return nil //no members, so pass nil (unless if in change block)
	}
	return elements.MakeRelation(
		rel.Id(),
		rel.Info(), rel.Tags(),
		tt, rr, rl,
		rel.Quadtree(), rel.ChangeType())
}

func filterBlock(bl elements.ExtendedBlock, ids IdSet) elements.ExtendedBlock {
	ee := make(elements.ByElementId, 0, bl.Len())
	for i := 0; i < bl.Len(); i++ {
		e := bl.Element(i)
		if ids.Contains(e.Type(), e.Id()) {
			if e.Type() == elements.Relation {
				e = filterRelMembers(e, ids)
			}
			if e != nil {
				ee = append(ee, e)
			}
		}
	}
	//return even if we have no elements
	return elements.MakeExtendedBlock(
		bl.Idx(), ee, bl.Quadtree(), bl.StartDate(), bl.EndDate(), bl.Tags())
}

//FilterObjs passes only the elements with ids in IdSet. See FindObjsFilter.
//Relations members are trimmed to only include objects to be passed on
//(equivilant to osmosis' --clip-incomplete-relations)
func FilterObjs(inblock []chan elements.ExtendedBlock, ids IdSet) ([]chan elements.ExtendedBlock, error) {
	out := make([]chan elements.ExtendedBlock, len(inblock))

	for i, _ := range inblock {
		out[i] = make(chan elements.ExtendedBlock)
		go func(i int) {
			for bl := range inblock[i] {
				out[i] <- filterBlock(bl, ids)
			}
			close(out[i])
		}(i)
	}
	return out, nil
}
