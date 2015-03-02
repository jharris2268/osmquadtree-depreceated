// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package blocksort

import (
    "github.com/jharris2268/osmquadtree/elements"
    "sort"
    "sync"
    "fmt"
)

type objsIdx struct {
    k  int
    vv []byte
    ii []int
    
}



func SortInMem(
    inChans []chan elements.ExtendedBlock,
    alloc Allocater,
    nc int,
    makeBlock func(int, int, elements.Block) (elements.ExtendedBlock, error) ) ([]chan elements.ExtendedBlock,error) {
    
    
    cc := make(chan objsIdx)
    go func() {
        wg:=sync.WaitGroup{}
        wg.Add(len(inChans))
        for _,inc:=range inChans {
            go func(inc chan elements.ExtendedBlock) {
                for b:= range inc {
                    ll := map[int][][]byte{}
                    for i:=0; i < b.Len(); i++ {
                        e:=b.Element(i)
                        ii := alloc(e)
                        
                        
                        ll[ii] = append(ll[ii],e.Pack())
                    }
                    for k,v := range ll {
                        ss:=0
                        for _,vi:=range v {
                            ss+=len(vi)
                        }
                        bb := make([]byte,ss)
                        ii := make([]int, len(v)+1)
                        p:=0
                        for i,vi := range v {
                            ii[i] = p
                            copy(bb[p:], vi)
                            p+=len(vi)
                        }
                        ii[len(v)]=ss
                        cc <- objsIdx{k,bb,ii}
                    }
                }
                wg.Done()
            }(inc)
            
        }
        wg.Wait()
        close(cc)
    }()
    
    
    tt := map[int][]objsIdx{}
    mx := -1
    for c:=range cc {
        tt[c.k] = append(tt[c.k], c)
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
    
        go func(i int) {
            for j:=i; j < len(kk); j+=nc {
                k:=kk[j]
            
                vv,ok := tt[k]
                if !ok {
                    fmt.Println("WTF",j,k)
                    continue
                }
                tl:=0
                for _,v := range vv {
                    tl+=(len(v.ii)-1)
                }
                tb:=make(elements.ByElementId, 0, tl)
                pp:=0
                for _,v := range vv {
                    for j,p := range v.ii[:len(v.ii)-1] {
                        q:=v.ii[j+1]
                        e := elements.UnpackElement(v.vv[p:q])
                        tb=append(tb,e)
                        pp++
                    }
                }
                
                tb.Sort()
                
                bl,_:=makeBlock(j,k,tb)
                //fmt.Println(j,bl)
                res[j%nc] <- bl
                
            }
            close(res[i])
            
            
        }(i)
        
    }
    
        
    return res, nil
}
    
        
    
    
    
