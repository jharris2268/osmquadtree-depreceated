package blocksort

import (
    "github.com/jharris2268/osmquadtree/elements"
    "sort"
)

type objsIdx struct {
    k int
    v elements.ByElementId
}



func SortInMem(
    dataFunc func(func(int,elements.ExtendedBlock) error) error,
    alloc Allocater,
    nc int,
    makeBlock func(int, int, int, elements.Block) error) error {
    
    
    cc := make(chan objsIdx)
    
    addobjs := func(j int, b elements.ExtendedBlock) error {
        ll := map[int]elements.ByElementId{}
        for i:=0; i < b.Len(); i++ {
            e:=b.Element(i)
            ii := alloc(e)
            ll[ii] = append(ll[ii],e)
        }
        for k,v := range ll {
            cc <- objsIdx{k,v}
        }
        return nil
    }
    
    go func() {
        dataFunc(addobjs)
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
        return nil
    }
    //j:=0
    
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
        makeBlock(j%nc,j,i,tb)
        delete(tt,i)
        //j++
    }
    return nil
}
    
        
    
    
    
