package change

import (
	//"fmt"

	//"github.com/jharris2268/osmquadtree/read"
	//"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/elements"
    "sync"
    //"fmt"
)

type origChangePair struct {
	idx  int
	orig elements.ExtendedBlock
	chg  elements.ExtendedBlock
}

func mergeOrigChange(idx int, orig elements.ExtendedBlock, chg elements.ExtendedBlock) elements.ExtendedBlock {
	
	if (orig==nil || orig.Len() == 0) && (chg==nil || chg.Len() == 0) {
		return elements.MakeExtendedBlock(idx,nil,0,0,0,nil)
	}
	if (chg==nil || chg.Len() == 0) {
		return orig
	}

	if (orig==nil || orig.Len() == 0) {
		return elements.MakeExtendedBlock(idx, elements.AsNormalBlock(chg), chg.Quadtree(), chg.StartDate(), chg.EndDate(), chg.Tags())
	}
	
    if chg.Quadtree() != orig.Quadtree() {
		println("wtf", chg.Quadtree(), orig.Quadtree())
		panic(0)
	}

	objects := make(elements.ByElementId, 0, orig.Len()+chg.Len())

    ss := PairObjs(orig, chg)
    for s:=ss(); s.Ok(); s=ss() {
		if s.B == nil {
			if s.A == nil {
				println("???")
			} else {
				objects = append(objects, s.A)
			}
		} else if s.B.ChangeType() > 2 {
            //println(s.b.String())
            objects=append(objects, elements.AsNormal(s.B))
		} else {
            if s.B.ChangeType()==elements.Delete || s.B.ChangeType()==elements.Remove {
                //pass
            } else {
                println("??",s.B.ChangeType()>2,s.B.ChangeType(),s.B.String())
            }
        }
	}
    //fmt.Println(orig.Len(),"+",chg.Len(),"=",objects.Len())
	return elements.MakeExtendedBlock(idx, objects, orig.Quadtree(), 0, chg.EndDate(), orig.Tags())
}

func MergeOrigAndChange(orig <-chan elements.ExtendedBlock, cbs <-chan elements.ExtendedBlock,  nc int, proc func(int, elements.ExtendedBlock) error) error {

    pp := make([]chan origChangePair, nc)
    for i,_:=range pp {
        pp[i] = make(chan origChangePair)
    }
	go func() {
		a, aok := <-orig
		b, bok := <-cbs

		i := 0
		for aok || bok {
            
			if !bok {
                //println(i,a.Quadtree().String(),a.Len(),"nil")
				pp[i%nc] <- origChangePair{i, a, nil}
                
				a, aok = <-orig
			} else if !aok {
                //println(i,b.Quadtree().String(),"nil",b.Len())
				pp[i%nc] <- origChangePair{i, nil, b}
				b, bok = <-cbs
			} else if a.Quadtree() < b.Quadtree() {
				//println(i,a.Quadtree().String(),a.Len(),"nil")
                pp[i%nc] <- origChangePair{i, a, nil}
				a, aok = <-orig
			} else if a.Quadtree() > b.Quadtree() {
                //println(i,b.Quadtree().String(),"nil",b.Len())
				pp[i%nc] <- origChangePair{i, nil, b}
				b, bok = <-cbs
			} else {
                //println(i,b.Quadtree().String(),a.Len(),b.Len())
				pp[i%nc] <- origChangePair{i, a, b}
				a, aok = <-orig
				b, bok = <-cbs
			}
			i++
		}
        for _,p:=range pp {
            close(p)
        }
	}()

	//merged := make(chan elements.ExtendedBlock)
	//go func() {
    wg:=sync.WaitGroup{}
    for i:=0; i < nc; i++ {
        wg.Add(1)
        go func(i int) {
            for ocp := range pp[i] {
                
                //fmt.Println(ocp.idx,ocp.orig,ocp.chg)
                rr:=mergeOrigChange(ocp.idx,ocp.orig, ocp.chg)
                //fmt.Println(ocp.idx,rr)
                proc(i,rr)
            }
            wg.Done()
        }(i)
    }
    wg.Wait()
	return nil
}
