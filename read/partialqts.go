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

//only read element id, quadtree and changetype

type nodeqt struct {
	//ref, qt int64
    //ct      byte
    ref     elements.Ref
    qt      quadtree.Quadtree
    ct      elements.ChangeType
}

type wayqt struct {
	//ref, qt int64
    //ct      byte
    ref     elements.Ref
    qt      quadtree.Quadtree
    ct      elements.ChangeType
}

type relqt struct {
	//ref, qt int64
    //ct      byte
    ref     elements.Ref
    qt      quadtree.Quadtree
    ct      elements.ChangeType
}

type geomqt struct {
	//ref, qt int64
    //ct      byte
    ref     elements.Ref
    qt      quadtree.Quadtree
    ct      elements.ChangeType
}

func (r *nodeqt) Type() elements.ElementType      { return elements.Node }
func (r *nodeqt) Id() elements.Ref                { return elements.Ref(r.ref) }
func (r *nodeqt) ChangeType() elements.ChangeType { return elements.ChangeType(r.ct) }
func (r *nodeqt) Pack() []byte {
	return elements.PackElement(r.Type(), 0, r.Id(), r.Quadtree(), nil, nil, nil)
}
func (r *nodeqt) String() string { return fmt.Sprintf("Node refqt %d %-18s", r.ref, r.Quadtree()) }

func (r *nodeqt) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }

func (r *wayqt) Type() elements.ElementType      { return elements.Way }
func (r *wayqt) Id() elements.Ref                { return elements.Ref(r.ref) }
func (r *wayqt) ChangeType() elements.ChangeType { return elements.ChangeType(r.ct) }
func (r *wayqt) Pack() []byte {
	return elements.PackElement(r.Type(), 0, r.Id(), r.Quadtree(), nil, nil, nil)
}
func (r *wayqt) String() string { return fmt.Sprintf("Way refqt %d %-18s", r.ref, r.Quadtree()) }

func (r *wayqt) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }

func (r *relqt) Type() elements.ElementType      { return elements.Relation }
func (r *relqt) Id() elements.Ref                { return elements.Ref(r.ref) }
func (r *relqt) ChangeType() elements.ChangeType { return elements.ChangeType(r.ct) }
func (r *relqt) Pack() []byte {
	return elements.PackElement(r.Type(), 0, r.Id(), r.Quadtree(), nil, nil, nil)
}
func (r *relqt) String() string { return fmt.Sprintf("Relation refqt %d %-18s", r.ref, r.Quadtree()) }

func (r *relqt) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }

func (r *geomqt) Type() elements.ElementType      { return elements.Geometry }
func (r *geomqt) Id() elements.Ref                { return elements.Ref(r.ref) }
func (r *geomqt) ChangeType() elements.ChangeType { return elements.ChangeType(r.ct) }
func (r *geomqt) Pack() []byte {
	return elements.PackElement(r.Type(), 0, r.Id(), r.Quadtree(), nil, nil, nil)
}
func (r *geomqt) String() string { return fmt.Sprintf("Geometry refqt %d %-18s", r.ref, r.Quadtree()) }

func (r *geomqt) Quadtree() quadtree.Quadtree { return quadtree.Quadtree(r.qt) }

func MakeObjQt(ty elements.ElementType, ref elements.Ref, qt quadtree.Quadtree) elements.Element {
	switch ty {
	case elements.Node:
		return &nodeqt{ref,qt, 0}
	case elements.Way:
		return &wayqt{ref,qt, 0}
	case elements.Relation:
		return &relqt{ref,qt, 0}
	case elements.Geometry:
		return &geomqt{ref,qt, 0}
	}
	return nil
}

type readObjsRefqt struct{}

func (rorq readObjsRefqt) addType(e elements.ElementType) bool { return true }

func find_id_and_qt(buf []byte) (elements.Ref, quadtree.Quadtree, error) {
    var id elements.Ref
    var qt quadtree.Quadtree
    idok,qtok := false,false
    var err error
    pos, msg := utils.ReadPbfTag(buf, 0)
    for ; ((msg.Tag > 0) && (!idok || !qtok)); pos, msg = utils.ReadPbfTag(buf, pos) {
        switch msg.Tag {
            case 1:
                id = elements.Ref(msg.Value)
                idok = true
            case 20:
                qt = quadtree.Quadtree(utils.UnZigzag(msg.Value))
                qtok = true
            case 21:
                qt,err = readQuadtree(msg.Data)
                
                if err!=nil {
                    return id,qt,err
                }
                qtok = true
                
        }
    }
    if !idok || !qtok {
        fmt.Println("??",idok,qtok,id,qt,"qt=",len(buf))
        return id,qt,missingData
    }
    return id,qt,nil
}

func (readObjsRefqt) node(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    id,qt,err := find_id_and_qt(buf)
    if err!=nil { return nil, err }
    return &nodeqt{id,qt,ct}, nil
    
    /*
	a, ok := getV(buf, 1)
	if !ok {
		return nil, missingData
	}
	b, ok := getV(buf, 20)
    

	return &nodeqt{int64(a), utils.UnZigzag(b), byte(ct)}, nil*/
}

func (readObjsRefqt) way(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    id,qt,err := find_id_and_qt(buf)
    if err!=nil { return nil, err }
    return &wayqt{id,qt,ct}, nil
    /*
	a, ok := getV(buf, 1)
	if !ok {
		return nil, missingData
	}
	b, ok := getV(buf, 20)

	return &wayqt{int64(a), utils.UnZigzag(b), byte(ct)}, nil*/
}

func (readObjsRefqt) relation(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    id,qt,err := find_id_and_qt(buf)
    if err!=nil { return nil, err }
    return &relqt{id,qt,ct}, nil
    /*
	a, ok := getV(buf, 1)
	if !ok {
		return nil, missingData
	}
	b, ok := getV(buf, 20)

	return &relqt{int64(a), utils.UnZigzag(b), byte(ct)}, nil*/
}

func (readObjsRefqt) geometry(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    id,qt,err := find_id_and_qt(buf)
    if err!=nil { return nil, err }
    return &geomqt{id,qt,ct}, nil
	
    /*a, ok := getV(buf, 1)
	if !ok {
		return nil, missingData
	}
	b, ok := getV(buf, 20)

	return &geomqt{int64(a), utils.UnZigzag(b), byte(ct)}, nil*/
}

func read_packed_quadtrees(qx, qy, qz []int64) ([]int64, error) {
    
    if (len(qx)!=len(qy) || len(qx)!=len(qz)) {
        return nil, missingData
    }
    qq := make([]int64, len(qx))
    for i,x := range qx {
        qt,err := quadtree.FromTuple(x,qy[i],qz[i])
        if err!=nil { return nil,err }
        qq[i] = int64(qt)
    }
    /*q:=quadtree.Quadtree(qq[0])
    s,t,u:=q.Tuple()
    fmt.Println(qx[0],qy[0],qz[0],len(qx),len(qq),s,t,u,q)*/
    return qq,nil
}

func (readObjsRefqt) dense(buf []byte, st []string, objs elements.ByElementId, ct elements.ChangeType) (elements.ByElementId, error) {
	var ii, qq []int64
    var qx,qy,qz []int64

	var err error
	pos, msg := utils.ReadPbfTag(buf, 0)
	for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(buf, pos) {
		switch msg.Tag {
		case 1:
			ii, err = utils.ReadDeltaPackedList(msg.Data)
		case 20:
			qq, err = utils.ReadDeltaPackedList(msg.Data) //depreceated
		
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
    if (len(qq) == 0) && (len(qx)>0) {
        qq,err = read_packed_quadtrees(qx,qy,qz)
        if err!=nil { return nil, err }
    }
    
	for i, id := range ii {
		if i >= len(qq) {
			return nil, missingData
		}
		objs = append(objs, &nodeqt{elements.Ref(id), quadtree.Quadtree(qq[i]), ct})

	}
	return objs, nil
}

// ReadQts reutrns an ExtendedBlock of elements consiting of the Type,
// Ref and Quadtree only. This is used by the calcqts.FindGroups function.
func ReadQts(idx int, buf []byte, isc bool) (elements.ExtendedBlock, error) {
    //fmt.Println("ReadQts",idx,len(buf))
	qt, bl, err := readPlain(buf, readObjsRefqt{}, isc)
    
	if err != nil {
        fmt.Println("ReadQts",idx,len(buf),err.Error())
		return nil, err
	}
    //fmt.Println(qt,bl)
	return elements.MakeExtendedBlock(idx, bl, qt, 0, 0, nil), nil
}
