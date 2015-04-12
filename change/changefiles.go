// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package change

import (
	//"github.com/jharris2268/osmquadtree/read"
	"github.com/jharris2268/osmquadtree/elements"
	"sort"
	
)


type byQuadtree []elements.ExtendedBlock

func (sob byQuadtree) Len() int           { return len(sob) }
func (sob byQuadtree) Less(i, j int) bool { return sob[i].Quadtree() < sob[j].Quadtree() }
func (sob byQuadtree) Swap(i, j int)      { sob[j], sob[i] = sob[i], sob[j] }

type byQuadtreeAndStartDate []elements.ExtendedBlock

func (sob byQuadtreeAndStartDate) Len() int { return len(sob) }
func (sob byQuadtreeAndStartDate) Less(i, j int) bool {
	if sob[i].Quadtree() == sob[j].Quadtree() {
		return sob[i].StartDate() < sob[j].StartDate()
	}
	return sob[i].Quadtree() < sob[j].Quadtree()
}
func (sob byQuadtreeAndStartDate) Swap(i, j int) { sob[j], sob[i] = sob[i], sob[j] }


type ElementPair struct {
    A elements.Element
	B elements.Element
}

// Ok() returns false if both A and B are nil
func (ep ElementPair) Ok() bool {
    return ep.A != nil || ep.B !=nil
}

func objCmp(a elements.Element, b elements.Element) int {
    
	if a == nil && b == nil {
		return 0
	}
    
    //an entity is less than a nil
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}
    
    // if same type, compare Id
	if a.Type() == b.Type() {
		if a.Id() == b.Id() {
			return 0
		} else if a.Id() < b.Id() {
			return -1
		} else {
			return 1
		}
	}
	if a.Type() < b.Type() {
		return -1
	}
	return 1
}



// PairObjs pairs the lhs and rhs blocks by element type and element id. If
// an element is not present in either the lhs or rhs return nil. Returns
// an iterator function
func PairObjs(lhs elements.Block, rhs elements.Block) func() ElementPair {
	
    ai := 0
    bi := 0
    
    return func() ElementPair {
        var a,b elements.Element
		if ai<lhs.Len() { a = lhs.Element(ai) }
        if bi<rhs.Len() { b = rhs.Element(bi) }
        
        if a==nil && b == nil {
            return ElementPair{nil,nil}
        }
        switch objCmp(a,b) {
            case 0:
                ai++
                bi++
                return ElementPair{a,b}
            case -1:
                ai++
                return ElementPair{a,nil}
            case 1:
                bi++
                return ElementPair{nil,b}
        }
        return ElementPair{nil,nil}
    }
}

// PairObjsChan pairs the lhs and rhs chans by element type and element
// id. If an element is not present in either the lhs or rhs return nil.
// Returns a channel of ElementPair
func PairObjsChan(lhs <-chan elements.Element, rhs <-chan elements.Element) <-chan ElementPair {
	
    res:=make(chan ElementPair)
    
    
    go func() {
        
        a,aok := <-lhs 
        b,bok := <-rhs 
        
        for aok || bok {
            if !aok {
                // no rhs left
                res <- ElementPair{nil,b}
                b,bok = <-rhs
                continue
            }
            if !bok {
                // no lhs left
                res <- ElementPair{a,nil}
                a,aok = <-lhs
                continue
            }
            
            switch objCmp(a,b) {
                case 0:
                    // same obj, return both
                    res <- ElementPair{a,b}
                    a,aok = <-lhs
                    b,bok = <-rhs
                    
                case -1:
                    // same lhs lower than rhs
                    res <- ElementPair{a,nil}
                    a,aok = <-lhs
                case 1:
                    // same lhs higher than rhs
                    res <- ElementPair{nil,b}
                    b,bok = <-rhs
            }
        }
        close(res)
    }()
    return res
}

func mergeTwoChangeObjBlocks(lhs elements.Block, rhs elements.Block) elements.ByElementId {
	aa := make(elements.ByElementId, 0, lhs.Len()+rhs.Len())
    ss := PairObjs(lhs,rhs)
	for s := ss(); s.Ok(); s=ss() {
		if s.B == nil {
			if s.A == nil {
				//println("pass")
			} else {
				aa = append(aa, s.A)
			}
		} else {
			aa = append(aa, s.B)
		}
		s.A = nil
		s.B = nil
	}
	lhs = nil
	rhs = nil
	return aa
}

type blockList []elements.ExtendedBlock
func (bl blockList) Len() int { return len(bl) }
func (bl blockList) Block(i int) elements.Block { return bl[i] }

func mergeManyChangeObjBlocks(blocks blockList) elements.Block {

	l := blocks.Len()
	if l == 1 {
		return blocks.Block(0)
	} else if l == 2 {
		return mergeTwoChangeObjBlocks(blocks.Block(0), blocks.Block(1))
	}
    // call mergeManyChangeObjBlocks on each half, then call
    
	hl := l / 2
	return mergeTwoChangeObjBlocks(mergeManyChangeObjBlocks(blocks[:hl]), mergeManyChangeObjBlocks(blocks[hl:]))
}

// MergeChangeBlock combines multiple input blocks into a single output
// block. inBlocks must have the same quadtree value and be sorted in
// ascending date order. The output block with contain the latest entity
// for each type and ref.
func MergeChangeBlock(idx int, inBlocks []elements.ExtendedBlock) elements.ExtendedBlock {

	if len(inBlocks) == 0 {
		return nil
	}
	

	sort.Sort(byQuadtreeAndStartDate(inBlocks))

	qt := inBlocks[0].Quadtree()
	sd := inBlocks[0].StartDate() //startdate from first block
	ed := inBlocks[len(inBlocks)-1].EndDate() //enddate from last
    objects := mergeManyChangeObjBlocks(inBlocks)
    
	//println("make",len(inBlocks),idx,qt.String(),sd,ed,objects.Len())
	return elements.MakeExtendedBlock(idx, objects, qt, sd, ed,nil)
}

// MergeChange calls MergeChangeBlock for each group of inBlocks (see
// readfile.ReadPbfFileFullParallel to produce appropiate input chan).
// Returns output chan of result
func MergeChange(inBlocks <-chan []elements.ExtendedBlock) (<-chan elements.ExtendedBlock, error) {

    
	res := make(chan elements.ExtendedBlock)
	go func() {
        i:=0
		for bl := range inBlocks {
            rr := MergeChangeBlock(i,bl)
			res <- rr
            i++
        }
		close(res)
	}()
	return res, nil
}

