// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package read

import (
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"

	"fmt"
)

//read element id, quadtree and data: location, nodes or members as appropiate

type nodeloc struct {
	ref     elements.Ref
    qt      quadtree.Quadtree
	lon, lat int64
}

type wayrefs struct {
	ref     elements.Ref
    qt      quadtree.Quadtree
	rr      []int64
}
type relmems struct {
	ref     elements.Ref
    qt      quadtree.Quadtree
	tt      []uint64
	rr      []int64
}

func (r *nodeloc) Type() elements.ElementType      { return elements.Node }
func (r *nodeloc) Id() elements.Ref                { return elements.Ref(r.ref) }
func (r *nodeloc) ChangeType() elements.ChangeType { return elements.Normal }
func (r *nodeloc) Pack() []byte {
	return elements.PackElement(r.Type(), 0, r.Id(), r.Quadtree(), elements.PackLonlat(r.lon, r.lat), nil, nil)
}
func (r *nodeloc) String() string {
	return fmt.Sprintf("Node loc %d %-18s [%d %d]", r.ref, r.Quadtree(), r.lon, r.lat)
}

func (r *nodeloc) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }
func (r *nodeloc) Lon() int64                  { return r.lon }
func (r *nodeloc) Lat() int64                  { return r.lat }

func (r *wayrefs) Type() elements.ElementType      { return elements.Way }
func (r *wayrefs) Id() elements.Ref                { return elements.Ref(r.ref) }
func (r *wayrefs) ChangeType() elements.ChangeType { return elements.Normal }
func (r *wayrefs) Pack() []byte {
	return elements.PackElement(r.Type(), 0, r.Id(), r.Quadtree(), elements.PackRefs(r), nil, nil)
}
func (r *wayrefs) String() string {
	return fmt.Sprintf("Way refs %d %-18s [%d refs]", r.ref, r.Quadtree(), len(r.rr))
}

func (r *wayrefs) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }
func (r *wayrefs) Len() int                    { return len(r.rr) }
func (r *wayrefs) Ref(i int) elements.Ref      { return elements.Ref(r.rr[i]) }

func (r *relmems) Type() elements.ElementType      { return elements.Relation }
func (r *relmems) Id() elements.Ref                { return elements.Ref(r.ref) }
func (r *relmems) ChangeType() elements.ChangeType { return elements.Normal }
func (r *relmems) Pack() []byte {
	return elements.PackElement(r.Type(), 0, r.Id(), r.Quadtree(), elements.PackMembers(r), nil, nil)
}
func (r *relmems) String() string {
	return fmt.Sprintf("Relation mems %d %-18s [%d mems]", r.ref, r.Quadtree(), len(r.rr))
}

func (r *relmems) Quadtree() quadtree.Quadtree           { return quadtree.Quadtree(r.qt) }
func (r *relmems) Len() int                              { return len(r.rr) }
func (r *relmems) Ref(i int) elements.Ref                { return elements.Ref(r.rr[i]) }
func (r *relmems) Role(i int) string                     { return "" }
func (r *relmems) MemberType(i int) elements.ElementType { return elements.ElementType(r.tt[i]) }

type readObjsData struct {
	nodes bool
	ways  bool
	rels  bool
}

func (rorq readObjsData) addType(e elements.ElementType) bool {
	switch e {
	case elements.Node:
		return rorq.nodes
	case elements.Way:
		return rorq.ways
	case elements.Relation:
		return rorq.rels
	}
	return false
}

func (readObjsData) node(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
	id,qt,ln,lt := elements.Ref(0), quadtree.Null, int64(0), int64(0)
    var err error
	pos, msg := utils.ReadPbfTag(buf, 0)
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		//ignore info and tags
		case 1:
			id = elements.Ref(msg.Value)
		case 8:
			lt = utils.UnZigzag(msg.Value)
		case 9:
			ln = utils.UnZigzag(msg.Value)
		case 20:
			qt = quadtree.Quadtree(utils.UnZigzag(msg.Value))
        case 21:
            qt,err = readQuadtree(msg.Data)
            if err!=nil { return nil,err }
		}
        
	}
	return &nodeloc{id,qt,ln,lt}, nil

}

func (readObjsData) way(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
	id, qt := elements.Ref(0), quadtree.Null
	var rfs []int64
    var err error
	pos, msg := utils.ReadPbfTag(buf, 0)
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		//ignore info and tags
		case 1:
			id = elements.Ref(msg.Value)
		case 8:
			rfs,err = utils.ReadDeltaPackedList(msg.Data)
            if err!=nil { return nil,err}
		case 20:
			qt = quadtree.Quadtree(utils.UnZigzag(msg.Value))
        case 21:
            qt,err = readQuadtree(msg.Data)
            if err!=nil { return nil,err }
		}
	}

	return &wayrefs{id,qt,rfs}, nil

}

func (readObjsData) relation(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
	id, qt := elements.Ref(0), quadtree.Null
	var mt []uint64
	var mr []int64
    var err error
	pos, msg := utils.ReadPbfTag(buf, 0)
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		//ignore info and tags
		case 1:
			id = elements.Ref(msg.Value)
		case 10:
			mt, err = utils.ReadPackedList(msg.Data)
            if err!=nil { return nil,err}
		case 9:
			mr, _ = utils.ReadDeltaPackedList(msg.Data)
            if err!=nil { return nil,err}
		case 20:
			qt = quadtree.Quadtree(utils.UnZigzag(msg.Value))
        case 21:
            qt,err = readQuadtree(msg.Data)
            if err!=nil { return nil,err }
		}
	}
	//println("ret rel",a,b,c,d)
	return &relmems{id,qt,mt,mr}, nil

}

func (readObjsData) dense(buf []byte, st []string, objs elements.ByElementId, ct elements.ChangeType) (elements.ByElementId, error) {
	var ii, qq, ln, lt []int64
    var qx,qy,qz []int64
	var err error
	pos, msg := utils.ReadPbfTag(buf, 0)
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		//ignore info and tags
		case 1:
			ii, err = utils.ReadDeltaPackedList(msg.Data)
		case 8:
			lt, err = utils.ReadDeltaPackedList(msg.Data)
		case 9:
			ln, err = utils.ReadDeltaPackedList(msg.Data)
		case 20:
			qq, err = utils.ReadDeltaPackedList(msg.Data)
        case 21:
			qx, err = utils.ReadDeltaPackedList(msg.Data)
        case 22:
			qy, err = utils.ReadDeltaPackedList(msg.Data)
        case 23:
			qz, err = utils.ReadDeltaPackedList(msg.Data)
		}

		if err != nil {
			return nil, err
		}

	}
    if len(qq)==0 && len(qx)>0 {
        qq,err = read_packed_quadtrees(qx,qy,qz)
        if err!=nil { return nil,err }
    }
    
    
	for i, id := range ii {
		q := int64(-1)
		if qq != nil && i < len(qq) {
			q = qq[i]
			//return nil,missingData
		}
		n, t := int64(0), int64(0)
		if ln != nil && lt != nil && i < len(ln) && i < len(lt) {
			//return nil,missingData
			n, t = ln[i], lt[i]
		}
		objs = append(objs, &nodeloc{elements.Ref(id), quadtree.Quadtree(q), n, t})

	}
	return objs, nil
}

func (readObjsData) geometry(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
	return nil, nil
}

// ReadObjsData returns an ExtendedBlock of elements with the core data
// (Type, Changetype and Ref), Quadtree, and the NodeLoc, Refs or Members
// (without roles) as appropiate. This can be significantly faster than
// reading the entire object. Setting nodes, ways and rels to false skips
// objects of this type.
func ReadObjsData(idx int, buf []byte, nodes, ways, rels bool) (elements.ExtendedBlock, error) {

	qt, bl, err := readPlain(buf, readObjsData{nodes, ways, rels}, false)
	if err != nil {
		return nil, err
	}
	return elements.MakeExtendedBlock(idx, bl, qt, 0, 0, nil), nil
}
