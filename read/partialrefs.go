// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package read


import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    
    
    "fmt"
    "errors"
)

var missingData = errors.New("Missing Data")

type noderef int64
type wayref int64
type relref int64
type geomref int64

func (r noderef) Type() elements.ElementType { return elements.Node }
func (r noderef) Id() elements.Ref { return elements.Ref(r) }
func (r noderef) ChangeType() elements.ChangeType { return elements.Normal }
func (r noderef) Pack() []byte { return elements.PackElement(r.Type(),0,r.Id(),0,nil,nil,nil) }
func (r noderef) String() string { return fmt.Sprintf("Node ref %d", r ) }


func (r wayref) Type() elements.ElementType { return elements.Way }
func (r wayref) Id() elements.Ref { return elements.Ref(r) }
func (r wayref) ChangeType() elements.ChangeType { return elements.Normal }
func (r wayref) Pack() []byte { return elements.PackElement(r.Type(),0,r.Id(),0,nil,nil,nil) }
func (r wayref) String() string { return fmt.Sprintf("Way ref %d", r ) }


func (r relref) Type() elements.ElementType { return elements.Relation }
func (r relref) Id() elements.Ref { return elements.Ref(r) }
func (r relref) ChangeType() elements.ChangeType { return elements.Normal }
func (r relref) Pack() []byte { return elements.PackElement(r.Type(),0,r.Id(),0,nil,nil,nil) }
func (r relref) String() string { return fmt.Sprintf("Relation ref %d", r ) }


func (r geomref) Type() elements.ElementType { return elements.Geometry }
func (r geomref) Id() elements.Ref { return elements.Ref(r) }
func (r geomref) ChangeType() elements.ChangeType { return elements.Normal }
func (r geomref) Pack() []byte { return elements.PackElement(r.Type(),0,r.Id(),0,nil,nil,nil) }
func (r geomref) String() string { return fmt.Sprintf("Geometry ref %d", r ) }

type readObjsRef struct{}
func (rorq readObjsRef) addType(e elements.ElementType) bool { return true }


func (readObjsRef) node(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    a,ok := getV(buf,1)
    if !ok { return nil,missingData }
    
    return noderef(a),nil
}

func (readObjsRef) way(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    a,ok := getV(buf,1)
    if !ok { return nil,missingData }
    
    return wayref(a),nil
}

func (readObjsRef) relation(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    a,ok := getV(buf,1)
    if !ok { return nil,missingData }
    
    return relref(a),nil
}
func (readObjsRef) geometry(buf []byte, st []string, ct elements.ChangeType) (elements.Element, error) {
    a,ok := getV(buf,1)
    if !ok { return nil,missingData }
    
    return geomref(a),nil
}

func (readObjsRef) dense(buf []byte, st []string, objs elements.ByElementId, ct elements.ChangeType) (elements.ByElementId, error) {
    ii := []int64{}
    var err error
    pos,msg:=utils.ReadPbfTag(buf,0)
    for ; msg.Tag>0; pos,msg=utils.ReadPbfTag(buf,pos) {
        if msg.Tag==1 {
            ii,err = utils.ReadDeltaPackedList(msg.Data)
        }
        if err!=nil {
            return nil,err
        }
        
    }
    for _,i := range ii {
        objs=append(objs, noderef(i))
    }
    return objs, nil
}

func getV(buf []byte, tg uint64) (uint64,bool) {
    pos,msg:=utils.ReadPbfTag(buf,0)
    for ; msg.Tag>0; pos,msg=utils.ReadPbfTag(buf,pos) {
        if msg.Tag==tg {
            return msg.Value,true
        }
    }
    return 0,false
}

func ReadRefs(buf []byte) (elements.Block, error) {
    return readPlain(buf, readObjsRef{})
}
