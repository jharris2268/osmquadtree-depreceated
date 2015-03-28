// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package readfile

import (
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/change"
    "github.com/jharris2268/osmquadtree/elements"
    "strings"
)


func lowestQt(vals []elements.ExtendedBlock) quadtree.Quadtree {
    lq := quadtree.Null
    for _,v := range vals {
        if v==nil {
            continue
        }
        if v.Quadtree()==quadtree.Null{
            panic("wtf")
        }
        if lq==quadtree.Null || v.Quadtree()<lq {
            lq=v.Quadtree()
        }
    }
    return lq
}

func getQt( vals    []elements.ExtendedBlock,
            incs    []<-chan elements.ExtendedBlock,
            qt      quadtree.Quadtree ) []elements.ExtendedBlock {
    
    ans:=make([]elements.ExtendedBlock, 0, len(vals))
    for i,v := range vals {
        if v!=nil {
            var ok bool
            if v.Quadtree() == qt {
                ans=append(ans, v)
                vals[i],ok = <-incs[i]
                if !ok {
                    vals[i] = nil
                }
            }
        }
    }
    return ans
}
            
                

func getNext(incs []<-chan elements.ExtendedBlock) func() []elements.ExtendedBlock {
    vals := make([]elements.ExtendedBlock, len(incs))
    for i,inc := range incs {
        v,ok := <- inc
        for ok && v.Quadtree()<0 {
            v,ok = <- inc
        }
        vals[i] = v
    }
    
    return func() []elements.ExtendedBlock {
        //println("getNext...")
        lowest := lowestQt(vals)
        //println("lowest=",lowest.String())
        if lowest == quadtree.Null { return nil }
        return getQt(vals, incs, lowest)
        
        
    }
}
    


func ReadPbfFileFullParallel(fns []string, iterFunc func(string) <-chan elements.ExtendedBlock) (<-chan []elements.ExtendedBlock,error) {
    
    
    bls := make([]<-chan elements.ExtendedBlock, len(fns))
    for i,f := range fns {
        //var err error
        bls[i] = iterFunc(f)
        //println(f)
        /*if err!=nil {
            return nil, err
        }*/
        
    }
    
    res := make(chan []elements.ExtendedBlock)
    go func() {
        nn := getNext(bls)
        for nbs := nn(); nbs!=nil; nbs=nn() {
            //println(len(nbs),nbs[0].Quadtree().String())
            res <- nbs
        }
        close(res)
    }()
    
    return res,nil
}


func ReadExtendedBlockMultiMerge(origfn string, chgfns []string, nc int) ([]chan elements.ExtendedBlock, error) {
    
    
    
    orig,err := ReadExtendedBlockMultiSorted(origfn, nc)
    if err!=nil { return nil, err } 
    
    iterFunc := func(s string) <-chan elements.ExtendedBlock {
        a,_ := ReadExtendedBlock(s)
        return a
    }
    
    chgs,err := ReadPbfFileFullParallel(chgfns, iterFunc)
    if err!=nil { return nil, err }
    
    merged,err:= change.MergeChange(chgs)
    if err!=nil { return nil, err }
    
    return change.MergeOrigAndChange(orig,merged,nc)
        
}

func ReadExtendedBlockMultiMergeQtsSingleFile(origfn string, nc int, passQt func(quadtree.Quadtree) bool) ([]chan elements.ExtendedBlock, error) {
    
    
    orig := make(chan elements.ExtendedBlock,20)
    chgs := make(chan []elements.ExtendedBlock)
    
    go func() {
        defer close(orig)
        defer close(chgs)
        aa := make([]elements.ExtendedBlock, 0, 30)
        blcks,_ := ReadExtendedBlockMultiSortedQts(origfn, nc, passQt)
        
        b0 := <- blcks
        if b0==nil {
            return
        }
        aa = append(aa, b0)
        ni:=0
        for bl := range blcks {
            if bl.Quadtree()!=aa[0].Quadtree() {
                
                aa[0].SetIdx(ni)
                
                orig <- aa[0]
                if len(aa)>1 {
                    chgs <- aa[1:]
                } else {
                    chgs <- []elements.ExtendedBlock{elements.MakeExtendedBlock(ni,nil,aa[0].Quadtree(),0,0,nil)}
                }
                ni++
                
                aa=make([]elements.ExtendedBlock,0,30)
            }
            aa=append(aa, bl)
        }
        orig <- aa[0]
        if len(aa)>1 {
            chgs <- aa[1:]
        }
    }()
    merged,err:= change.MergeChange(chgs)
    if err!=nil { return nil, err }
    
    return change.MergeOrigAndChange(orig,merged,nc)
        
}
            
    


func ReadExtendedBlockMultiMergeQts(origfn string, chgfns []string, nc int, passQt func(quadtree.Quadtree) bool) ([]chan elements.ExtendedBlock, error) {
    
    if len(chgfns)==0 {
        return ReadExtendedBlockMultiMergeQtsSingleFile(origfn, nc, passQt)
    }
    
    
    getBlocks := func(s string) <-chan elements.ExtendedBlock {
        nc:=1
        if strings.HasSuffix(s,"pbf") {
            nc=4
        }
        a,_ := ReadExtendedBlockMultiSortedQts(s, nc, passQt)
        return a
    }
    
    orig := getBlocks(origfn)
    //origs := SortExtendedBlockChan(orig)
    
    chgs,err := ReadPbfFileFullParallel(chgfns, getBlocks)
    if err!=nil { return nil, err }
    
    merged,err:= change.MergeChange(chgs)
    if err!=nil { return nil, err }
    
    return change.MergeOrigAndChange(orig,merged,nc)
        
}




func MakePassQt(qts map[quadtree.Quadtree]bool) func(quadtree.Quadtree) bool {
    return func(q quadtree.Quadtree) bool {
        _,ok:=qts[q]
        return ok
    }
}


func ReadMergeChangePbfSorted(origfn string, chgfns []string, nc int, passQt func(quadtree.Quadtree) bool) (<-chan elements.ExtendedBlock,error) {
    
    dd,err := ReadExtendedBlockMultiMergeQts(origfn,chgfns,nc,passQt)
    if err!=nil { return nil,err }
    return CollectExtendedBlockChans(dd,false),nil
}

func ReadExtendedBlockMultiQtsUnmerged(origfn string, chgfns []string, nc int, passQt func(quadtree.Quadtree) bool, nout int) ([]chan elements.ExtendedBlock,error) {
    
    getBlocks := func(s string) <-chan elements.ExtendedBlock {
        nc:=1
        if strings.HasSuffix(s,"pbf") {
            nc=4
        }
        a,_ := ReadExtendedBlockMultiSortedQts(s, nc, passQt)
        return a
    }
    
    
    
    cc,err := ReadPbfFileFullParallel(append([]string{origfn},chgfns...), getBlocks)
    if err!=nil {
        return nil,err
    }
    
    
    out:=make([]chan elements.ExtendedBlock, nout)
    for i,_ := range out {
        out[i]=make(chan elements.ExtendedBlock)
    }
    
    go func() {
        x:=0
        for c:=range cc {
            for _,b:=range c {
                b.SetIdx(x)
                out[x%nout] <- b
                x++
            }
        }
        for _,o:=range out {
            close(o)
        }
    }()
    return out,nil
}
