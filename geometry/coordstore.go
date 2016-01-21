// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geometry

import (
	"log"

	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"

	"errors"
	"fmt"
)

// A CoordStore stores node location coordinates.
type CoordStore interface {
	// Find returns (lon,lat,true) if ref is present, (0,0,false) if not.
	Find(ref elements.Ref) (int64, int64, bool)
	Len() int // Len() returns number of coordinates stored.
}

// CoordBlockStore stores the node location coordinates for blocks of elements,
// for as long as they are useful. Calling Add removes existing blocks
// with quadtree values which are not parents / antecendants of the new
// block, before adding the the node locations of this new block. This
// means we need only store 18 blocks of data at once, resulting in low
// memory usage.
type CoordBlockStore interface {
	CoordStore
	Add(elements.ExtendedBlock)
	NumBlocks() int
}

func getCoords(cs CoordStore, refs elements.Refs) ([]elements.Ref, []int64, []int64, error) {
	
    rfs := make([]elements.Ref, refs.Len())
	lons,lats := make([]int64, refs.Len()), make([]int64, refs.Len())
    
	for i := 0; i < refs.Len(); i++ {
		
		r := refs.Ref(i)
		ok := false
        rfs[i]=r
		lons[i],lats[i], ok = cs.Find(r)

		if !ok {
			return nil,nil, nil, errors.New(fmt.Sprintf("Missing node %d @ %d", r, i))
		}
		
	}
	return rfs,lons,lats, nil
}

type lonLat struct {
	lon, lat int64
}

type mapCoordStore map[elements.Ref]lonLat

func (mcs mapCoordStore) Find(ref elements.Ref) (int64, int64, bool) {
	ll, ok := mcs[ref]
	return ll.lon, ll.lat, ok // will return 0,0,false if ref not present
	// nb. 0,0,true would be a real answer
}

func (mcs mapCoordStore) Len() int { return len(mcs) }

type mapCoordBlockStore map[quadtree.Quadtree]CoordStore

func (mcbs mapCoordBlockStore) Find(ref elements.Ref) (int64, int64, bool) {
	for _, v := range mcbs { // try each tile in turn: can only ever be 18 tiles to look at
		a, b, c := v.Find(ref)
		if c { // found it, so return
			return a, b, c
		}
	}

	return 0, 0, false // not present in any tile
}

func (mcbs mapCoordBlockStore) Len() int {
	r := 0
	for _, v := range mcbs {
		r += v.Len()
	}
	return r
}

func (mcbs mapCoordBlockStore) Add(bl elements.ExtendedBlock) {
	// add each node location to a new mapCoordStore
	nt := mapCoordStore{}
	for i := 0; i < bl.Len(); i++ {
		e := bl.Element(i)
		if e.Type() != elements.Node {
			continue
		}
		ll, ok := e.(elements.LonLat)
		if !ok {
			continue
		}

		nt[e.Id()] = lonLat{ll.Lon(), ll.Lat()}
	}

	// ---
	// delete tiles we are finished with
	q := bl.Quadtree()
	tr := make(quadtree.QuadtreeSlice, 0, len(mcbs))
	for k, _ := range mcbs { // iterate over current tiles
		if k.Common(q) != k { // if not a parent of new tile
			tr = append(tr, k) // add to list
		}

	}

	for _, t := range tr {
		mcbs[t] = nil // help GC
		delete(mcbs, t)
	}
	// ----

	// add new tile to map
	mcbs[q] = nt

}

func (mcbs mapCoordBlockStore) NumBlocks() int { return len(mcbs) }

//extended way type, with added node locations. Satsifies elements.FullWay and elements.WayPoints
/*
type coordWay struct {
	id   elements.Ref
	info elements.Info
	tags elements.Tags
	qt   quadtree.Quadtree
	ct   elements.ChangeType

	refs []elements.Ref
    lons,lats []int64
}

func (tw *coordWay) Type() elements.ElementType      { return elements.Way }
func (tw *coordWay) Id() elements.Ref                { return tw.id }
func (tw *coordWay) Info() elements.Info             { return tw.info }
func (tw *coordWay) Tags() elements.Tags             { return tw.tags }
func (tw *coordWay) ChangeType() elements.ChangeType { return tw.ct }
func (tw *coordWay) Quadtree() quadtree.Quadtree     { return tw.qt }

func (tw *coordWay) SetChangeType(c elements.ChangeType) { tw.ct = c }
func (tw *coordWay) SetQuadtree(q quadtree.Quadtree)     { tw.qt = q }

func (tw *coordWay) Len() int               { return len(tw.refs) }
func (tw *coordWay) Ref(i int) elements.Ref { return tw.refs[i] }

//func (tw *coordWay) Coords() []Coord { return tw.cc }
func (tw *coordWay) LonLat(i int) (int64,int64) { return tw.lons[i],tw.lats[i] }

func (tw *coordWay) Pack() []byte {
	//will drop node coordinates, and unpack to a normal elements.FullWay
	return elements.PackFullElement(tw, elements.PackRefs(tw))
}
func (tw *coordWay) String() string {
	return fmt.Sprintf("CoordsWay: %8d [%4d refs] %-18s", tw.id, len(tw.refs), tw.qt)
}
*/

func makeCoordWay(cbs CoordBlockStore, e elements.Element) (elements.Element, *quadtree.Bbox, error) {
	

	fw, ok := e.(elements.FullWay)
	if !ok {
		return nil, nil, errors.New("Not a FullWay")
	}
	var err error
    
    wp,ok := fw.(elements.FullWayPoints)
    if ok {
        wp.SetTags(MakeTagsEditable(fw.Tags()))
        return wp, makeBboxLL(wp), nil
    }
    
    refs,lons,lats, err := getCoords(cbs, fw)
    if err != nil {
        return nil, nil, err
    }
    wp = elements.MakeWayPoints(
        fw.Id(),fw.Info(),MakeTagsEditable(fw.Tags()),
        refs,lons,lats,
        fw.Quadtree(), fw.ChangeType())
    
	return wp, makeBboxLL(wp), nil
}

// AddWayCoords converts the FullWay elements from the input chan inc into
// an extended way type with the node locations added to it. These ways
// also satisfy the elements.WayPoints interface type. It also filters out nodes
// without tags and relations objects without a type tag of
// "boundary","multipolygon", or "route". If bx is not null
// it also filters out nodes and ways not present within thay Bbox. The
// input channel inc must be in quadtree order.
func AddWayCoords(inc <-chan elements.ExtendedBlock, bx *quadtree.Bbox) <-chan elements.ExtendedBlock {
	ans := make(chan elements.ExtendedBlock)

	go func() {
		bs := mapCoordBlockStore{}
		idx := 0
		for bl := range inc {
			//add coords to BlockStore bs
			bs.Add(bl)
			nr := make(elements.ByElementId, 0, bl.Len())
			for i := 0; i < bl.Len(); i++ {
				e := bl.Element(i)
				switch e.Type() {
				case elements.Node:
					fn := e.(elements.FullNode)
					// skip if outside of bounding box
					if bx != nil && !bx.ContainsXY(fn.Lon(), fn.Lat()) {
						continue
					}

					// if we have tags, add to output
					if fn.Tags() != nil && fn.Tags().Len() > 0 {
						ne := elements.MakeNode(fn.Id(),
							fn.Info(), MakeTagsEditable(fn.Tags()),
							fn.Lon(), fn.Lat(), fn.Quadtree(),
							fn.ChangeType())

						nr = append(nr, ne)
					}
				case elements.Way:
					fw := e.(elements.FullWay)
					tw, wbx, err := makeCoordWay(bs, fw)

					if err != nil {
						log.Println(bl, fw, err.Error())
						//panic(err.Error())
						continue
					}
					// skip if entirely outside of bounding box
					if bx != nil && !bx.Intersects(*wbx) {
						continue
					}
					nr = append(nr, tw)
				case elements.Relation:
					fr := e.(elements.FullRelation)
					tt := MakeTagsEditable(fr.Tags())
					if tt.Has("type") {
						switch tt.Get("type") {
						case "boundary", "multipolygon", "route":

							nl := elements.MakeRelationCopy(
								fr.Id(), fr.Info(), tt, fr,
								fr.Quadtree(), fr.ChangeType())

							nr = append(nr, nl)
							//default:
							//  pass
						}
					}
				}
			}
			//skip empty blocks
			if len(nr) > 0 {
				ans <- elements.MakeExtendedBlock(idx, nr, bl.Quadtree(), bl.StartDate(), bl.EndDate(), nil)
				idx++
			}
		}
		close(ans)
	}()
	return ans
}
