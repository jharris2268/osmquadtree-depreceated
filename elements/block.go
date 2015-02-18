package elements

import (
    "github.com/jharris2268/osmquadtree/quadtree"
    "fmt"
    "sort"
)

type Block interface {
    Len()           int
    Element(i int)  Element
    String()        string
}

type ExtendedBlock interface {
    Block
    Quadtree()      quadtree.Quadtree
    StartDate()     Timestamp
    EndDate()       Timestamp
    Tags()          Tags
    Idx()           int
    SetIdx(int)
}

type ByElementId []Element

func (bo ByElementId) Len() int {
    return len(bo)
}
func (bo ByElementId) Element(i int) Element {
    return bo[i]
}

func (bo ByElementId) Swap(i,j int) {
    bo[i],bo[j] = bo[j], bo[i]
}

func (bo ByElementId) Less(i,j int) bool {
    return Less(bo[i],bo[j])
}

func Less(l Element, r Element) bool {
    if l.Type()==r.Type() {
        return l.Id() < r.Id()
    }
    return l.Type()< r.Type()
}

func (bo ByElementId) Sort() {
    sort.Sort(bo)
}

func (bo ByElementId) String() string {
    b0s := "NULL"
    b1s := "NULL"
    if len(bo)>0 {
        b0s = bo[0].String()
        if len(bo)>1 {
            b1s = bo[len(bo)-1].String()
        }
    }
    
    return fmt.Sprintf("Block %-5d: %s => %s", bo.Len(), b0s,b1s)
}

type extendedBlock struct {
    idx         int
    elements    ByElementId
    qt          quadtree.Quadtree
    startDate   Timestamp
    endDate     Timestamp
    tags        Tags
}

func MakeExtendedBlock(idx int, elements Block,
    qt quadtree.Quadtree,
    startDate Timestamp, endDate Timestamp,
    tags Tags) ExtendedBlock {
    
    return &extendedBlock{idx,Elements(elements),qt,startDate,endDate,tags}
}

func (e *extendedBlock) SetIdx(i int) {
    e.idx=i
}

func (e *extendedBlock) Idx() int { return e.idx }


func (e *extendedBlock) Len() int { return e.elements.Len() }
func (e *extendedBlock) Element(i int) Element { return e.elements.Element(i) }

func (e *extendedBlock) Quadtree()  quadtree.Quadtree { return e.qt }
func (e *extendedBlock) StartDate() Timestamp { return e.startDate }
func (e *extendedBlock) EndDate()   Timestamp { return e.endDate }
func (e *extendedBlock) Tags()      Tags      { return e.tags }

func (bo *extendedBlock) String() string {
    var a,b Element
    if bo.Len()>0 {
        a=bo.Element(0)
        b=bo.Element(bo.Len()-1)
    }
    f:=""
    if bo.Quadtree()!=quadtree.Null {
        f=fmt.Sprintf("[%-18s %d=>%d] ",bo.Quadtree(), bo.StartDate(), bo.EndDate())
    }
    return fmt.Sprintf("ExtendedBlock %-5d: %s%s => %s", bo.Len(), f,a,b)
}


func Elements(block Block) ByElementId {
    if block==nil {
        return nil
    }
    
    bi,ok := block.(ByElementId)
    if ok { return bi }
    
    e,ok := block.(*extendedBlock)
    if ok {
        return e.elements
    }
    
    ans:=make(ByElementId, block.Len())
    for i,_ := range ans {
        ans[i] = block.Element(i)
    }
    return ans
}

func AsNormalBlock(block Block) Block {
    
    oo := make(ByElementId, block.Len())
    for i,_ := range oo {
        oo[i] = AsNormal(block.Element(i))
    }
    return oo
}

func AsNormal(element Element) Element {
    if element.ChangeType()==0 {
        return element
    }
    
    switch element.(type) {
        case *fullNode:
            fn := element.(*fullNode)
            fn.ct = 0
            return fn
        case *fullWay:
            fn := element.(*fullWay)
            fn.ct = 0
            return fn
        case *fullRelation:
            fn := element.(*fullRelation)
            fn.ct = 0
            return fn
        case *packedGeometry:
            fn := element.(*packedGeometry)
            fn.ct = 0
            return fn
        case PackedElement:
            fn := element.(PackedElement)
            fn[1] = 0
            return fn
    }
    
    e := UnpackElement(element.Pack())
    return AsNormal(e)
}
    
        
            
        
    