package blocksort

import (
    "github.com/jharris2268/osmquadtree/elements"
    "sort"
    "sync"
)

type objsIdx struct {
    k int
    v elements.ByElementId
}



func SortInMem(
    inChans []chan elements.ExtendedBlock,
    alloc Allocater,
    nc int,
    makeBlock func(int, int, elements.Block) (elements.ExtendedBlock, error) ) ([]chan elements.ExtendedBlock,error) {
    
    
    cc := make(chan objsIdx)
    go func() {
        wg:=sync.WaitGroup{}
        for _,inc:=range inChans {
            go func(inc chan elements.ExtendedBlock) {
                for b:= range inc {
                    ll := map[int]elements.ByElementId{}
                    for i:=0; i < b.Len(); i++ {
                        e:=b.Element(i)
                        ii := alloc(e)
                        ll[ii] = append(ll[ii],e)
                    }
                    for k,v := range ll {
                        cc <- objsIdx{k,v}
                    }
                }
                wg.Done()
            }(inc)
            
        }
        wg.Wait()
        close(cc)
    }()
    
    
    tt := map[int][]elements.ByElementId{}
    mx := -1
    for c:=range cc {
        tt[c.k] = append(tt[c.k], c.v)
        if c.k > mx {
            mx=c.k
        }
    }
    
    kk := make([]int, 0, len(tt))
    for k,_ := range tt {
        kk=append(kk,k)
    }
    sort.Ints(kk)
    
    if mx<0 {
        return nil,nil
    }
    //j:=0
    
    res := make([]chan elements.ExtendedBlock, nc)
    for i,_:=range res {
        res[i]=make(chan elements.ExtendedBlock)
    }
    
    
    go func() {
        for j,i := range kk {
            vv,ok := tt[i]
            if !ok {
                //makeBlock(i%nc, i, nil)
                continue
            }
            tl:=0
            for _,v := range vv {
                tl+=len(v)
            }
            tb:=make(elements.ByElementId, tl)
            pp:=0
            for _,v := range vv {
                copy(tb[pp:],v)
                pp+=len(v)
            }
            
            tb.Sort()
            bl,_:=makeBlock(j,i,tb)
            res[j%nc] <- bl
            delete(tt,i)
            //j++
        }
        for _,r:=range res {
            close(r)
        }
    }()
        
        
    return res, nil
}
    
        
    
    
    
