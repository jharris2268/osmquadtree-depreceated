package geometry

import (
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/utils"
    "strconv"
    "fmt"
    "sort"
    "strings"
)



type pendingTags struct {
    q quadtree.Quadtree
    s []string
}

func AdminLevels(tt TagsEditable, ss []string) bool {
    ssi := make(utils.Int64Slice, 0, len(ss))
    for _,s:=range ss {
        ii,err := strconv.ParseInt(s,10,64)
        if err==nil {
            ssi = append(ssi,ii)
        }
    }
    if len(ssi)==0 {
        return false
    }
    ssi.Sort()
    
    
    
    tt.Put("min_admin_level", fmt.Sprintf("%d",ssi[0]))
    tt.Put("max_admin_level", fmt.Sprintf("%d",ssi[len(ssi)-1]))
    return true
}

func BusRouteList(tt TagsEditable, ss []string) bool {
    if ss==nil || len(ss) == 0 {
        return false
    }
    sm := map[string]bool{}
    for _,s:=range ss {
        sm[s]=true
    }
    ss=make([]string,0,len(sm))
    for s,_ := range sm {
        ss=append(ss,s)
    }
    
    sort.Strings(ss)
    
    tt.Put("bus_routes", strings.Join(ss, ";"))
    return true
}


func AddRelationRange(inc <-chan elements.ExtendedBlock, testRel func(TagsEditable) bool, srctag string, proc func(TagsEditable,[]string) bool) <-chan elements.ExtendedBlock {
    
    res := make(chan elements.ExtendedBlock)
    
    go func() {
        
        ss := map[elements.Ref]*pendingTags{}
        
        idx:=0
        
        for bl := range inc {
            
            bq := bl.Quadtree()
            
            for i:=0; i < bl.Len(); i++ {
                e:=bl.Element(i)
                switch e.Type() {
                    case elements.Relation:
                        fr := e.(elements.FullRelation)
                        tt,ok := fr.Tags().(TagsEditable)
                        
                        if !ok {
                            panic("unconverted tags")
                        }
                        
                        
                        if !testRel(tt) {
                            
                            continue
                        }
                        
                        if tt.Has(srctag) {
                            v:=tt.Get(srctag)
                            for j:=0; j < fr.Len(); j++ { 
                                if fr.MemberType(j) == elements.Way {
                                    w:=fr.Ref(j)
                                    p,ok := ss[w]
                                    if !ok {
                                        p = &pendingTags{bq,[]string{}}
                                    } else {
                                        p.q = p.q.Common(bq)
                                    }
                                    p.s=append(p.s,v)
                                    ss[w]=p
                                }
                            }
                            
                        }
                }
            }
            nb := make(elements.ByElementId, 0, bl.Len())
            for i:=0; i < bl.Len(); i++ {
                e:=bl.Element(i)
                switch e.Type() { 
                
                    case elements.Way:
                        p,ok := ss[e.Id()]
                        if ok {
                            fw := e.(elements.FullWay)
                            
                            tt,ok := fw.Tags().(TagsEditable)
                            if !ok {
                                panic("unconverted tags")
                            }
                            proc(tt,p.s)
                            delete(ss,e.Id())
                            
                            nb=append(nb,fw)
                            
                            
                        } else {
                            nb=append(nb,e)
                        }
                    default:
                        nb=append(nb,e)
                }
            }
            res <- elements.MakeExtendedBlock(idx, nb, bq, bl.StartDate(), bl.EndDate(), nil)
            idx++
            
            
            ds:=make([]elements.Ref,0,len(ss))
            for k,v := range ss {
                if bq.Common(v.q)!=v.q {
                    ds=append(ds,k)
                }
            }
            for _,k:=range ds {
                delete(ss,k)
            }
        }
        
        
        
        close(res)
    }()
    return res
}