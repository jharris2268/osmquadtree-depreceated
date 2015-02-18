package elements

import (
    "time"
    "github.com/jharris2268/osmquadtree/quadtree"
)

type ElementType int
const (
    Node ElementType = iota
    Way
    Relation
    Geometry
)

func (et ElementType) String() string {
    switch et {
        case Node: return "Node"
        case Way: return "Way"
        case Relation: return "Relation"
        case Geometry: return "Geometry"
    }
    return "??"
}

type ChangeType int
const (
    Normal  ChangeType = iota
    Delete
    Remove
    Unchanged
    Modify
    Create
)

func ChangeTypeString(ct ChangeType) string {
    switch ct {
        case Normal: return "Normal"
        case Delete: return "Delete"
        case Remove: return "Remove"
        case Unchanged: return "Unchgd"
        case Modify: return "Modify"
        case Create: return "Create"
    }
    return "??"
}
func (ct ChangeType) String() string {
    return ChangeTypeString(ct)
}

type Ref int64
type Timestamp int64

func (t Timestamp) String() string {
    return time.Unix(int64(t),0).UTC().Format("2006-01-02T15:04:05")
}
func (t Timestamp) DateString() string {
    return time.Unix(int64(t),0).UTC().Format("20060102")
}

func ReadDateString(s string) (Timestamp,error) {
    a,err := time.Parse("2006-01-02T15:04:05", s)
    if err==nil {
        return Timestamp(a.Unix()),nil
    }
    a,err = time.Parse("20060102", s)
    if err!=nil {
        return 0,err
    }
    return Timestamp(a.Unix()),nil
}
    
        

type Element interface {
    Type()      ElementType
    Id()        Ref
    ChangeType() ChangeType
    Pack()      []byte
    String()    string
}

    
type Tags interface {
    Len()       int
    Key(int)    string
    Value(int)  string
    
    Pack()      []byte
}

type Info interface {
    Version()   int64
    Timestamp() Timestamp
    Changeset() Ref
    Uid()       int64
    User()      string
    
    Pack()      []byte
}

type LonLat interface {
    Lon()       int64
    Lat()       int64
}

type Refs  interface {
    Len()       int
    Ref(int)    Ref
}

type Members interface {
    Len()       int
    MemberType(int)   ElementType
    Ref(int)    Ref
    Role(int)   string
}
type Quadtreer interface {
    Quadtree()  quadtree.Quadtree
}

type FullElement interface {
    Element    
    Tags()      Tags
    Info()      Info
    Quadtreer
    
    SetQuadtree(quadtree.Quadtree)
    SetChangeType(ChangeType)
}

type FullNode interface {
    FullElement
    LonLat
}

type FullWay interface {
    FullElement
    Refs
}

type FullRelation interface {
    FullElement
    Members
}
    
type PackedGeometry interface {
    FullElement
    GeometryData()  []byte
}
