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


func ProcessPbfFileFullMerge(origfn string, chgfns []string, proc func(int, elements.ExtendedBlock) error, nc int) (error) {
    
    
    
    orig,err := ReadFileBlocksFullSorted(origfn, nc)
    if err!=nil { return err } 
    
    iterFunc := func(s string) <-chan elements.ExtendedBlock {
        a,_ := ReadFileBlocksFull(s)
        return a
    }
    
    chgs,err := ReadPbfFileFullParallel(chgfns, iterFunc)
    if err!=nil { return err }
    
    merged,err:= change.MergeChange(chgs)
    if err!=nil { return err }
    
    return change.MergeOrigAndChange(orig,merged,nc,proc)
        
}

func ProcessPbfFileFullMergeQts(origfn string, chgfns []string, passQt func(quadtree.Quadtree) bool, proc func(int, elements.ExtendedBlock) error, nc int) (error) {
    
    getBlocks := func(s string) <-chan elements.ExtendedBlock {
        nc:=1
        if strings.HasSuffix(s,"pbf") {
            nc=4
        }
        a,_ := ReadFileBlocksSorted(s, nc, MakeProcessFileBlocksPartialQts(passQt) )
        return a
    }
    
    orig := getBlocks(origfn)
    //origs := SortExtendedBlockChan(orig)
    
    chgs,err := ReadPbfFileFullParallel(chgfns, getBlocks)
    if err!=nil { return err }
    
    merged,err:= change.MergeChange(chgs)
    if err!=nil { return err }
    
    return change.MergeOrigAndChange(orig,merged,nc,proc)
        
}



func MakeProcessPbfFile(origfn string, nc int) func(func(int, elements.ExtendedBlock) error) error {
    return func(proc func(int, elements.ExtendedBlock) error) error {
        return ProcessFileBlocksFullMulti(origfn, proc, nc)
    }
}

func MakeProcessMergeChangePbfFile(origfn string, chgfns []string, nc int) func(func(int, elements.ExtendedBlock) error) error {
    
    
    return func(proc func(int, elements.ExtendedBlock) error) error {
        
        if chgfns == nil {
            return ProcessFileBlocksFullMulti(origfn, proc, nc)
        }
        return ProcessPbfFileFullMerge(origfn, chgfns, proc, nc)
    }
}


func MakePassQt(qts map[quadtree.Quadtree]bool) func(quadtree.Quadtree) bool {
    return func(q quadtree.Quadtree) bool {
        _,ok:=qts[q]
        return ok
    }
}

func MakeProcessPbfFileFullMergeQts(origfn string, chgfns []string, passQt func(quadtree.Quadtree) bool, nc int) func(func(int, elements.ExtendedBlock) error) error {
    return func(proc func(int, elements.ExtendedBlock) error) error {
        return ProcessPbfFileFullMergeQts(origfn, chgfns, passQt, proc, nc)
    }
}


func ReadMergeChangePbfSorted(origfn string, chgfns []string, passQt func(quadtree.Quadtree) bool, nc int) (<-chan elements.ExtendedBlock,error) {
    
    pf := func(o string, proc func(int, elements.ExtendedBlock) error, n int) error {
        return ProcessPbfFileFullMergeQts(origfn, chgfns, passQt, proc, nc)
    }
    return ReadFileBlocksSorted(origfn,nc,pf)
}

func ReadChangePbfSortedUnmerged(origfn string, chgfns []string, passQt func(quadtree.Quadtree) bool, nc int) (<-chan elements.ExtendedBlock,error) {
    getBlocks := func(s string) <-chan elements.ExtendedBlock {
        nc:=1
        if strings.HasSuffix(s,"pbf") {
            nc=4
        }
        a,_ := ReadFileBlocksSorted(s, nc, MakeProcessFileBlocksPartialQts(passQt) )
        return a
    }
    
    out:=make(chan elements.ExtendedBlock)
    cc,err := ReadPbfFileFullParallel(append([]string{origfn},chgfns...), getBlocks)
    if err!=nil {
        return nil,err
    }
    go func() {
        x:=0
        for c:=range cc {
            for _,b:=range c {
                b.SetIdx(x)
                out <- b
                x++
            }
        }
        close(out)
    }()
    return out,nil
}
