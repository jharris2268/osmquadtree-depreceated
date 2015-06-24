// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package read

import (
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"

	//"fmt"
	//"time"
	"errors"
)

func readQuadtree(buf []byte) (quadtree.Quadtree, error) {
	x, y, z := int64(0), int64(0), int64(0)
	pos, msg := utils.ReadPbfTag(buf, 0)

	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		case 1:
			x = int64(msg.Value)
		case 2:
			y = int64(msg.Value)
		case 3:
			z = int64(msg.Value)
		}
	}
	return quadtree.FromTuple(x, y, z)
}

func readStringtable(buf []byte) ([]string, error) {
	ans := make([]string, 0, len(buf)/10)
	pos, msg := utils.ReadPbfTag(buf, 0)

	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		if msg.Tag == 1 {
			ans = append(ans, string(msg.Data))
		}
	}
	return ans, nil
}

func makeTags(kk []uint64, vv []uint64, st []string) (elements.Tags, error) {
	keys, vals := make([]string, len(kk)), make([]string, len(kk))
	for i, k := range kk {
		keys[i] = st[k]
		vals[i] = st[vv[i]]
	}
	return elements.MakeTags(keys, vals), nil
}

func readPlain(buf []byte, readObjsImpl readObjs, change bool) (quadtree.Quadtree, elements.ByElementId, error) {
	//readPlain ignores the stringtable and some of the extra block attributes

	var objs elements.ByElementId
	var err error
	var qt quadtree.Quadtree
	pos, msg := utils.ReadPbfTag(buf, 0)
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		case 2:
			if change {
				// no stringtable:             nil strintable->
				objs, err = readPrimitiveGroupChange(msg.Data, nil, objs, readObjsImpl)
			} else {
				// no stringtable:        nil strintable->
				objs, err = readPrimitiveGroup(msg.Data, nil, objs, readObjsImpl, elements.Normal)
			}
		case 31:
			qt, err = readQuadtree(msg.Data)
		}
		if err != nil {
			return quadtree.Null, nil, err
		}
	}

	return qt, objs, nil
}

func readFull(idx int, buf []byte, readObjsImpl readObjs, change bool) (elements.ExtendedBlock, error) {
	stringtable := []string{}
	pgs := [][]byte{}

	var qt quadtree.Quadtree
	var sd, ed elements.Timestamp

	kk, vv := []uint64{}, []uint64{}

	pos, msg := utils.ReadPbfTag(buf, 0)
	var err error
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		case 1:
			stringtable, err = readStringtable(msg.Data)
		case 2:
			pgs = append(pgs, msg.Data) // primitive groups (i.e. element data): store for now
		case 31:
			qt, err = readQuadtree(msg.Data)

		case 33: //startdate
			if msg.Data == nil {
				sd = elements.Timestamp(msg.Value)
			} else {
				err = errors.New("expected varint for startdate")
			}
		case 34: //enddate
			if msg.Data == nil {
				ed = elements.Timestamp(msg.Value)
			} else {
				err = errors.New("expected varint for enddate")
			}

		case 35: //block tag keys: useful for some geometry table results
			kk, err = utils.ReadPackedList(msg.Data)
		case 36: //block tag values: useful for some geometry table results
			vv, err = utils.ReadPackedList(msg.Data)
		}

		if err != nil {
			return nil, err
		}
	}

	var tags elements.Tags
	if len(kk) > 0 {
		tags, err = makeTags(kk, vv, stringtable)
	}

	objs := make(elements.ByElementId, 0, 10000)
	for _, dd := range pgs { // a primitive block can contain any
		// number of primitive groups. In particular,
		// changefiles need a new group for every
		// change in changetype
		if change {
			objs, err = readPrimitiveGroupChange(dd, stringtable, objs, readObjsImpl)
		} else {
			objs, err = readPrimitiveGroup(dd, stringtable, objs, readObjsImpl, elements.Normal)
		}
		if err != nil {
			return nil, err
		}
	}

	return elements.MakeExtendedBlock(idx, objs, qt, sd, ed, tags), nil
}

type readObjs interface {
	//        data   stringtable   changetype
	node([]byte, []string, elements.ChangeType) (elements.Element, error)
	way([]byte, []string, elements.ChangeType) (elements.Element, error)
	relation([]byte, []string, elements.ChangeType) (elements.Element, error)
	geometry([]byte, []string, elements.ChangeType) (elements.Element, error)

	dense([]byte, []string, elements.ByElementId, elements.ChangeType) (elements.ByElementId, error)

	addType(elements.ElementType) bool //return true to read an object of given type
}

func readPrimitiveGroupChange(buf []byte, st []string, objs elements.ByElementId, readOb readObjs) (elements.ByElementId, error) {
	ct := elements.Normal

	pos, msg := utils.ReadPbfTag(buf, 0)
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		//only extract changetype value
		if msg.Tag == 10 {
			ct = elements.ChangeType(msg.Value)
			break
		}
	}
	// call readPrimitiveGroup with found value
	return readPrimitiveGroup(buf, st, objs, readOb, ct)
}

func readPrimitiveGroup(buf []byte, st []string, objs elements.ByElementId, readOb readObjs, ct elements.ChangeType) (elements.ByElementId, error) {

	var err error
	pos, msg := utils.ReadPbfTag(buf, 0)
	var o elements.Element

	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		case 1:
			if readOb.addType(elements.Node) {

				o, err = readOb.node(msg.Data, st, ct)
				if objs == nil {
					objs = make(elements.ByElementId, 0, 10000)
				}
				objs = append(objs, o)
			}
		case 2:
			if readOb.addType(elements.Node) {
				if objs == nil {
					objs = make(elements.ByElementId, 0, 10000)
				}
				objs, err = readOb.dense(msg.Data, st, objs, ct)
			}
		case 3:
			if readOb.addType(elements.Way) {
				o, err = readOb.way(msg.Data, st, ct)
				if objs == nil {
					objs = make(elements.ByElementId, 0, 10000)
				}
				objs = append(objs, o)
			}
		case 4:
			if readOb.addType(elements.Relation) {
				o, err = readOb.relation(msg.Data, st, ct)
				if objs == nil {
					objs = make(elements.ByElementId, 0, 10000)
				}
				objs = append(objs, o)
			}
		case 20:
			if readOb.addType(elements.Geometry) {
				o, err = readOb.geometry(msg.Data, st, ct)
				if objs == nil {
					objs = make(elements.ByElementId, 0, 10000)
				}
				objs = append(objs, o)
			}

		}
		if err != nil {
			return nil, err
		}
	}
	return objs, nil
}
