// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package elements

import (
	"fmt"
	"sort"

	"github.com/jharris2268/osmquadtree/quadtree"
)

// A list of Element values
type Block interface {
	Len() int
	Element(i int) Element
	String() string
}

// A list of Element values, with added metadata
type ExtendedBlock interface {
	Block
	Quadtree() quadtree.Quadtree
	StartDate() Timestamp
	EndDate() Timestamp
	Tags() Tags
	Idx() int
	SetIdx(int)
}

// Slice of Element values, implementing Block interface
type ByElementId []Element

func (bo ByElementId) Len() int {
	return len(bo)
}
func (bo ByElementId) Element(i int) Element {
	return bo[i]
}

func (bo ByElementId) Swap(i, j int) {
	bo[i], bo[j] = bo[j], bo[i]
}

func (bo ByElementId) Less(i, j int) bool {
	return Less(bo[i], bo[j])
}

func compGeom(l Element, r Element) bool {
    lfg,ok := l.(interface{ OriginalType() ElementType })
    if ok {
        rfg,ok := r.(interface{ OriginalType() ElementType })
        if ok {
            if lfg.OriginalType()!=rfg.OriginalType() {
                return lfg.OriginalType() < rfg.OriginalType()
            }
        }
    }
    if l.Id() != r.Id() {
        return l.Id() < r.Id()
    }
    
    lfe,ok := l.(FullElement)
    if ok {
        
        
        rfe,ok := r.(FullElement)
        
        if ok {
            return compTags(lfe.Tags(), rfe.Tags())
        }
    }
    return string(l.Pack()) < string(r.Pack())
}

func compTags(lt Tags, rt Tags) bool {
    mm := lt.Len()
    if rt.Len() < mm { mm = rt.Len() }
    for i:=0; i < mm; i++ {
        if lt.Key(i) != rt.Key(i) {
            return lt.Key(i) < rt.Key(i)
        }
        if lt.Value(i) != rt.Value(i) {
            return lt.Value(i) < rt.Value(i)
        }
    }
    return false
}

func Less(l Element, r Element) bool {
	if l.Type() == r.Type() {
        if l.Type() == Geometry {
            return compGeom(l, r)
        }
		return l.Id() < r.Id()
	}
	return l.Type() < r.Type()
}

func (bo ByElementId) Sort() {
	sort.Sort(bo)
}

func (bo ByElementId) String() string {
	b0s := "NULL"
	b1s := "NULL"
	if len(bo) > 0 {
		b0s = bo[0].String()
		if len(bo) > 1 {
			b1s = bo[len(bo)-1].String()
		}
	}

	return fmt.Sprintf("Block %-5d: %s => %s", bo.Len(), b0s, b1s)
}

type extendedBlock struct {
	idx       int
	elements  ByElementId
	qt        quadtree.Quadtree
	startDate Timestamp
	endDate   Timestamp
	tags      Tags
}

// MakeExtendedBlock adds metadata to elemtnts Block
func MakeExtendedBlock(idx int, elements Block,
	qt quadtree.Quadtree,
	startDate Timestamp, endDate Timestamp,
	tags Tags) ExtendedBlock {

	return &extendedBlock{idx, Elements(elements), qt, startDate, endDate, tags}
}

func (e *extendedBlock) SetIdx(i int) {
	e.idx = i
}

func (e *extendedBlock) Idx() int { return e.idx }

func (e *extendedBlock) Len() int              { return e.elements.Len() }
func (e *extendedBlock) Element(i int) Element { return e.elements.Element(i) }

func (e *extendedBlock) Quadtree() quadtree.Quadtree { return e.qt }
func (e *extendedBlock) StartDate() Timestamp        { return e.startDate }
func (e *extendedBlock) EndDate() Timestamp          { return e.endDate }
func (e *extendedBlock) Tags() Tags                  { return e.tags }

func (bo *extendedBlock) String() string {
	var a, b Element
	if bo.Len() > 0 {
		a = bo.Element(0)
		b = bo.Element(bo.Len() - 1)
	}
	f := ""
	if bo.Quadtree() != quadtree.Null {
		f = fmt.Sprintf("[%-18s %d=>%d] ", bo.Quadtree(), bo.StartDate(), bo.EndDate())
	}
	return fmt.Sprintf("ExtendedBlock %-5d: %s%s => %s", bo.Len(), f, a, b)
}

// Elements converts block to slice ByElementId
func Elements(block Block) ByElementId {
	if block == nil {
		return nil
	}

	bi, ok := block.(ByElementId)
	if ok {
		return bi
	}

	e, ok := block.(*extendedBlock)
	if ok {
		return e.elements
	}

	ans := make(ByElementId, block.Len())
	for i, _ := range ans {
		ans[i] = block.Element(i)
	}
	return ans
}

// AsNormalBlock calls AsNormal on each element of block
func AsNormalBlock(block Block) Block {

	oo := make(ByElementId, 0, block.Len())
	for i, _ := range oo {
		e := block.Element(i)
		switch e.ChangeType() {
		case Normal:
			oo = append(oo, e)
		case Delete, Remove:
			//pass
		case Unchanged, Modify, Create:
			oo = append(oo, AsNormal(e))
		}
	}
	return oo
}

func AsNormal(element Element) Element {
	if element.ChangeType() == Normal {
		return element
	}
	
	setChangeType,ok := element.(interface{
		SetChangeType(ChangeType)
	})
	if ok {
		setChangeType.SetChangeType(Normal)
		return element
	}
	
	
	switch element.(type) {
	/*case *fullNode:
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
		return fn*/
		
	case PackedElement:
		fn := element.(PackedElement)
		fn[1] = 0
		return fn
	}
	fmt.Println("??",element)
	e := UnpackElement(element.Pack())
	return AsNormal(e)
}
