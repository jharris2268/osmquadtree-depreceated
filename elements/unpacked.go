// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package elements

import (
    "github.com/jharris2268/osmquadtree/quadtree"
    "fmt"
    //"time"
    "strings"
    
)

func exStr(qt quadtree.Quadtree, ct ChangeType) string {
    s:=""
    if qt!=0 {
        s += fmt.Sprintf(" %-18s",qt)
    }
    if ct!=Normal {
        s += fmt.Sprintf(" %-5s", ChangeTypeString(ct))
    }
    return s
}
    

type fullNode struct {
    id      Ref
    ct      ChangeType
    qt      quadtree.Quadtree
    lon,lat int64
    
    info    Info
    tags    Tags
}

func (fn *fullNode) Type() ElementType {
    return Node
}
func (fn *fullNode) ChangeType() ChangeType {
    return fn.ct
}

func (fn *fullNode) Id() Ref {
    return fn.id
}
func (fn *fullNode) Quadtree() quadtree.Quadtree {
    return fn.qt
}
func (fn *fullNode) Info() Info {
    return fn.info
}

func (fn *fullNode) Tags() Tags {
    return fn.tags
}


func (fn *fullNode) Lon() int64 {
    return fn.lon
}

func (fn *fullNode) Lat() int64 {
    return fn.lat
}

func (fn *fullNode) String() string {
    return fmt.Sprintf("FullNode: %-10d [%-10d, %-10d]%s", fn.id,fn.lon,fn.lat, exStr(fn.qt, fn.ct))
}


func MakeNode(id Ref, info Info, tags Tags,
    lon int64, lat int64,
    qt quadtree.Quadtree, change ChangeType) FullNode {
    
    return &fullNode{id,change,qt,lon,lat,info,tags}
}


func (fn *fullNode) Pack() []byte {
    return PackFullElement(fn, PackLonlat(fn.lon,fn.lat))
    //return PackElement(Node,fn.ct,fn.id,fn.qt,PackLonlat(fn.lon,fn.lat), fn.info.Pack(), fn.tags.Pack())
}

    
type fullWay struct {
    
    id      Ref
    ct      ChangeType
    qt      quadtree.Quadtree
    refs    []Ref
    
    info    Info
    tags    Tags
}

func (fn *fullWay) Type() ElementType {
    return Way
}
func (fn *fullWay) ChangeType() ChangeType {
    return fn.ct
}

func (fn *fullWay) Id() Ref {
    return fn.id
}
func (fn *fullWay) Quadtree() quadtree.Quadtree {
    return fn.qt
}
func (fn *fullWay) Info() Info {
    return fn.info
}

func (fn *fullWay) Tags() Tags {
    return fn.tags
}

func (fn *fullWay) Len() int {
    return len(fn.refs)
}

func (fn *fullWay) Ref(i int) Ref {
    return fn.refs[i]
}

func MakeWay(id Ref, info Info, tags Tags,
    refs []Ref,
    qt quadtree.Quadtree, change ChangeType) FullWay {
    
    return &fullWay{id,change,qt,refs,info,tags}
}


func (fn *fullWay) Pack() []byte {
    return PackFullElement(fn, packRefs(fn.refs))
    //return PackElement(Way,fn.ct,fn.id,fn.qt,packRefs(fn.refs), fn.info.Pack(), fn.tags.Pack())
}

func (fn *fullWay) String() string {
    return fmt.Sprintf("FullWay: %-10d [%-10d refs]%s", fn.id, len(fn.refs), exStr(fn.qt, fn.ct))
}

type relMember struct {
    memType ElementType
    ref     Ref
    role    string
}

type fullRelation struct {
    
    id      Ref
    ct      ChangeType
    qt      quadtree.Quadtree
    mems    []relMember
    
    info    Info
    tags    Tags
}

func (fn *fullRelation) Type() ElementType {
    return Relation
}
func (fn *fullRelation) ChangeType() ChangeType {
    return fn.ct
}

func (fn *fullRelation) Id() Ref {
    return fn.id
}

func (fn *fullRelation) Quadtree() quadtree.Quadtree {
    return fn.qt
}

func (fn *fullRelation) Info() Info {
    return fn.info
}

func (fn *fullRelation) Tags() Tags {
    return fn.tags
}

func (fn *fullRelation) Len() int {
    return len(fn.mems)
}

func (fn *fullRelation) MemberType(i int) ElementType {
    return fn.mems[i].memType
}

func (fn *fullRelation) Ref(i int) Ref {
    return fn.mems[i].ref
}

func (fn *fullRelation) Role(i int) string {
    return fn.mems[i].role
}



func MakeRelation(id Ref, info Info, tags Tags,
    tys []ElementType, refs []Ref, roles []string,
    qt quadtree.Quadtree, change ChangeType) FullRelation {
    
    
    mems := make([]relMember, len(tys))
    for i,t:=range tys {
        mems[i].memType = t
        mems[i].ref = refs[i]
        if len(roles)>0 {
            mems[i].role = roles[i]
        }
    }
    return &fullRelation{id,change,qt,mems,info,tags}
}

func MakeRelationCopy(id Ref, info Info, tags Tags,
    origmems Members,
    qt quadtree.Quadtree, change ChangeType) FullRelation {
    
    
    mems := make([]relMember,origmems.Len())
    for i,_:=range mems {
        mems[i].memType = origmems.MemberType(i)
        mems[i].ref = origmems.Ref(i)
        mems[i].role = origmems.Role(i)
    }
    return &fullRelation{id,change,qt,mems,info,tags}
}

func (fn *fullRelation) Pack() []byte {
    return PackFullElement(fn, packMembers(fn.mems))
    //return PackElement(Relation,fn.ct,fn.id,fn.qt,packMembers(fn.mems), fn.info.Pack(), fn.tags.Pack())
}

func (fn *fullRelation) String() string {
    return fmt.Sprintf("FullRelation: %-10d [%-10d mems]%s", fn.id, len(fn.mems), exStr(fn.qt, fn.ct))
}

type unpackedTags struct {
    keys []string
    vals []string
}

func MakeTags(keys, vals []string) Tags {
    return &unpackedTags{keys, vals}
}

func (upt *unpackedTags) Len() int {
    return len(upt.keys)
}

func (upt *unpackedTags) Key(i int) string {
    return upt.keys[i]
}
func (upt *unpackedTags) Value(i int) string {
    return upt.vals[i]
}

func (upt *unpackedTags) Pack() []byte {
    return packTags(upt.keys,upt.vals)
}

func TagsString(tags Tags) string {
    s:=make([]string,tags.Len())
    for i,_:=range s {
        s[i] = fmt.Sprintf("%s:%q",tags.Key(i),tags.Value(i))
    }
    return fmt.Sprintf("{%s}", strings.Join(s,", "))
}
    



type unpackedInfo struct {
    vs   int64
    ts   Timestamp
    cs   Ref
    ui   int64
    user string
}

func (upi *unpackedInfo) Version() int64 { return upi.vs }
func (upi *unpackedInfo) Timestamp() Timestamp { return upi.ts }
func (upi *unpackedInfo) Changeset() Ref { return upi.cs }
func (upi *unpackedInfo) Uid() int64 { return upi.ui }
func (upi *unpackedInfo) User() string { return upi.user }

func MakeInfo(vs int64, ts Timestamp, cs Ref, ui int64, user string) Info {
    return &unpackedInfo{vs,ts,cs,ui,user}
}

func (upt *unpackedInfo) Pack() []byte {
    return packInfo(upt.vs, upt.ts, upt.cs, upt.ui, upt.user)
}


type packedGeometry struct {
    
    id      Ref
    ct      ChangeType
    qt      quadtree.Quadtree
    data    []byte
    
    info    Info
    tags    Tags
}

func (fn *packedGeometry) Type() ElementType {
    return Geometry
}
func (fn *packedGeometry) ChangeType() ChangeType {
    return fn.ct
}

func (fn *packedGeometry) Id() Ref {
    return fn.id
}

func (fn *packedGeometry) Quadtree() quadtree.Quadtree {
    return fn.qt
}

func (fn *packedGeometry) Info() Info {
    return fn.info
}

func (fn *packedGeometry) Tags() Tags {
    return fn.tags
}

func (fn *packedGeometry) GeometryData() []byte {
    return fn.data
}


func MakeGeometry(id Ref, info Info, tags Tags,
    data []byte,
    qt quadtree.Quadtree, change ChangeType) PackedGeometry {
    
    return &packedGeometry{id,change,qt,data,info,tags}
}

func (fn *packedGeometry) Pack() []byte {
    return PackFullElement(fn, fn.data)
    
}

func (fn *packedGeometry) String() string {
    return fmt.Sprintf("PackedGeometry: %-10d [%-10d bytes]%s", fn.id,len(fn.data), exStr(fn.qt, fn.ct))
}


func (fn *fullNode) SetQuadtree(q quadtree.Quadtree) {
    fn.qt = q
}
func (fn *fullWay) SetQuadtree(q quadtree.Quadtree) {
    fn.qt = q
}

func (fn *fullRelation) SetQuadtree(q quadtree.Quadtree) {
    fn.qt = q
}

func (fn *packedGeometry) SetQuadtree(q quadtree.Quadtree) {
    fn.qt = q
}

func (fn *fullNode) SetChangeType(ct ChangeType) {
    fn.ct = ct
}
func (fn *fullWay) SetChangeType(ct ChangeType) {
    fn.ct = ct
}

func (fn *fullRelation) SetChangeType(ct ChangeType) {
    fn.ct = ct
}

func (fn *packedGeometry) SetChangeType(ct ChangeType) {
    fn.ct = ct
}

func (fn *packedGeometry) SetTags(tags Tags) {
    fn.tags=tags
}
