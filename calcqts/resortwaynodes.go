package calcqts

import (
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/blocksort"
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/read"
	
	"fmt"
	"time"
    "errors"
    "sort"
    "runtime/debug"
    "sync"
)

func readWayNodes(infn string, nc int) (blocksort.AllocBlockStore, elements.Block, error) {
    
    rels := make([]elements.ByElementId, nc)
    for i,_:=range rels {
        rels[i] = make(elements.ByElementId, 0, 1000000)
    }
    
    inChans, err := readfile.ReadSomeElementsMulti(infn,nc,false,true,true)
    if err!=nil { return nil,nil,err}
    
    addFunc  := func(block elements.ExtendedBlock, res chan blocksort.IdPacked) error {
        
        if block==nil { return nil }
        
        if (block.Idx()%1371)==0  {
            fmt.Printf("%8d %s\n",block.Idx(), block.String())
        }
        
        cc := block.Idx() % nc  
        
        if block.Len()==0 { return nil }
 
        rp := map[int]nodeWaySlice{}            
        
        for i:=0; i < block.Len(); i++ {
            e:=block.Element(i)
            switch e.Type() {
                case elements.Node:
                    continue
                case elements.Way:
                    ei := e.Id()
                    rf,ok := e.(elements.Refs)
                    if !ok {
                        return errors.New("NO REFS")
                    }
                    for j:=0; j < rf.Len(); j++ {
                        r:=rf.Ref(j)
                        k:=int(r>>20)
                        rp[k] = append(rp[k], nodeWay{r,ei})
                        
                    }
                case elements.Relation:
                    rels[cc] = append(rels[cc], e)
            }
        }
        
        for k,v := range rp {
            res <- blocksort.IdPacked{k, v.Pack()}
        }
        return nil
    }
            
                
    //abs:= blocksort.MakeAllocBlockStore("tempfilesplit")
    abs := blocksort.MakeAllocBlockStore("tempfileslim")
    err =  blocksort.AddData(abs,inChans,addFunc)
    
    if err!=nil {
        return nil,nil,err
    }
    
    nr := 0
    for _,r:= range rels {
        nr+=len(r)
    }
    
    relsf := make(elements.ByElementId, nr)
    rr:=0
    for _,r:= range rels {
        copy(relsf[rr:], r)
        rr+=len(r)
    }
    relsf.Sort()
    fmt.Println("have",nr,len(relsf),"rels")
    return abs,relsf,nil
}

type nodeWay struct {
	node,way elements.Ref
}

type nodeWaySlice []nodeWay

func (pcs nodeWaySlice) Len() int { return len(pcs) }
func (pcs nodeWaySlice) Swap(i,j int) { pcs[i],pcs[j] = pcs[j],pcs[i] }
func (pcs nodeWaySlice) Less(i,j int) bool {
    if pcs[i].node == pcs[j].node {
        return pcs[i].way < pcs[j].way
    }
    return pcs[i].node < pcs[j].node
}
func (pcs nodeWaySlice) Sort() {
    sort.Sort(pcs)
}

func (pcs nodeWaySlice) Pack() []byte {
    pcs.Sort()

	pk := make([]byte, 20*len(pcs))
	lnode,lway := elements.Ref(0), elements.Ref(0)

	p := utils.WriteVarint(pk, 0, int64(len(pcs)))
	for i := 0; i < len(pcs); i++ {
		p = utils.WriteVarint(pk, p, int64(pcs[i].node-lnode))
		p = utils.WriteVarint(pk, p, int64(pcs[i].way-lway))
		lnode = pcs[i].node
		lway = pcs[i].way
	}
    
	return pk[:p]
}

func readParentChildSlice(bl []byte, mn elements.Ref, mx elements.Ref) nodeWaySlice {
    ln, ps := utils.ReadVarint(bl, 0)
    t := make(nodeWaySlice, 0, ln)
    lnode, lway := elements.Ref(0), elements.Ref(0)
    node,way := int64(0), int64(0)
    
    for i:=0; i < int(ln); i++ {
        node, ps = utils.ReadVarint(bl, ps)
        way, ps = utils.ReadVarint(bl, ps)
        
        lnode += elements.Ref(node)
        lway += elements.Ref(way)
        
        if lway >= mn && lway < mx {
            t=append(t, nodeWay{lnode,lway})
        }

    }
    return t
}

type pcsIdx struct {
    idx int
    pcs nodeWaySlice
}

func mergeParentChildSlice(ttp []nodeWaySlice) nodeWaySlice {
    tl:=0
    for _,t:=range ttp {
        tl+=len(t)
    }
    ans:=make(nodeWaySlice, tl)
    pp:=0
    
    for _,t:=range ttp {
        copy(ans[pp:],t)
        pp+=len(t)
    }
    ans.Sort()
    return ans
}
func (pi *pcsIdx) Idx() int { return pi.idx }

func readParentChildSliceBlockSort(abs blocksort.AllocBlockStore, mn elements.Ref, mx elements.Ref) <-chan nodeWaySlice {
    
    resp:=make(chan utils.Idxer)
    
    add:=func(i int, idx int, al int, pp blocksort.IdPackedList) error {
        rr := make([]nodeWaySlice, pp.Len())
        for i:=0; i < pp.Len(); i++ {
            rr[i] = readParentChildSlice(pp.At(i).Data,mn,mx)
        }
        resp <- &pcsIdx{idx,mergeParentChildSlice(rr)}
        return nil
    }
    
    go func() {
        blocksort.ReadData(abs,4,add)
        close(resp)
    }()
    
    sorted := utils.SortIdxerChan(resp)
    res := make(chan nodeWaySlice)
    go func() {
        for s:=range sorted {
            q := s.(*pcsIdx)
            if len(q.pcs)>0 {
                res <- q.pcs
            }
        }
        close(res)
    }()
    return res
}

type nodeAndWays struct {
	id     elements.Ref
	ln, lt int64
	ways   []elements.Ref
}

type nwi struct {
    idx int
    nws []nodeAndWays
}

func (n *nwi) Idx() int { return n.idx }

func iterNodes(infn string) <-chan []nodeAndWays {
    res:=make(chan utils.Idxer)
    
    
        
        
    
    add := func(i int, bl elements.ExtendedBlock) error {
        //println(bl.String())
        if bl==nil {
            res <- &nwi{bl.Idx(),nil}
            return nil
        }
            
        n := nwi{bl.Idx(), make([]nodeAndWays,0,bl.Len())}
        
        for j:=0; j < bl.Len(); j++ {
            e:=bl.Element(j)
            if e.Type()==elements.Node {
                
                nn:=nodeAndWays{e.Id(),0,0,nil}
                ln,ok := e.(elements.LonLat)
                if !ok {
                    continue
                }
                
                nn.ln = ln.Lon()
                nn.lt = ln.Lat()
                n.nws=append(n.nws,nn)
            }
        }
        res <- &n
        return nil
    }
    
    blcks, err := readfile.ReadSomeElementsMulti(infn, 4, true, false, false)
    if err!=nil { return nil }
    
    
    go func() {
        wg:=sync.WaitGroup{}
        wg.Add(len(blcks))
        for i,bl:=range blcks {
            go func(i int, bl chan elements.ExtendedBlock) {
                for b:=range bl {
                    add(i,b)
                }
                wg.Done()
            }(i,bl)
        }
        wg.Wait()
        close(res)
    }()
    
    sorted:=utils.SortIdxerChan(res)
    
    output:=make(chan []nodeAndWays)
    go func() {
        for s:=range sorted {
            n:=s.(*nwi)
            if len(n.nws)>0 {
                /*if (n.idx % 1381)==0 {
                    fmt.Printf("\rnodes %-8d: %-5d %10d %10d %10d",n.idx,len(n.nws),n.nws[0].id,n.nws[0].ln,n.nws[0].lt)
                }*/
                output <- n.nws
            }
        }
        close(output)
    }()
    return output
}


func mergeNodeAndWayNodes(nodes <-chan []nodeAndWays, wayNodes <-chan nodeWaySlice) <-chan []nodeAndWays {
	res := make(chan []nodeAndWays)

	go func() {
		missingnodes := 0

		wayNodeBlock, wayNodesOk := <-wayNodes
        for wayNodesOk && len(wayNodeBlock)==0 {
            wayNodeBlock, wayNodesOk = <-wayNodes
        }
        if !wayNodesOk {
            panic("no way nodes")
        }
        //println(wayNodesOk, len(wayNodeBlock),wayNodeBlock[0].node,wayNodeBlock[0].way)
		wn := 0

		

		for bl := range nodes {
            
			for i,n := range bl {
                
                tn := make([]elements.Ref, 0, 10)
                
				for wayNodesOk && wayNodeBlock[wn].node < n.id {
					println("?? missing node", wayNodeBlock[wn].node, "for", wayNodeBlock[wn].way, "[@", n.id,n.ln,n.lt, "]")
					missingnodes++
					if missingnodes == 10 {
						panic("10 missings")
					}

					wn++

					for wayNodesOk && wn == len(wayNodeBlock) {
						wayNodeBlock, wayNodesOk = <-wayNodes
						wn = 0
					}

				}

				for wayNodesOk && wayNodeBlock[wn].node == n.id {
					tn = append(tn, wayNodeBlock[wn].way)
                    wn++
                    for wayNodesOk && wn == len(wayNodeBlock) {
						wayNodeBlock, wayNodesOk = <-wayNodes
						wn = 0
					
                        /*if wayNodesOk {
                            println(wayNodesOk, len(wayNodeBlock),wayNodeBlock[0].node,wayNodeBlock[0].way)
                        }*/
                    }
                }
                if len(tn)>0 {
                    bl[i].ways = tn
                }
			}

			res <- bl
		}
		close(res)
	}()
	return res
}

func expandWayBoxes(infn string, abs blocksort.AllocBlockStore, mw, Mw elements.Ref) wayBbox {
	ans := newWayBbox()

	st := time.Now()
	numNode, numWN, nextP := 0, 0, 0
    nds := iterNodes(infn)
    wayNodes:=readParentChildSliceBlockSort(abs,mw,Mw)
    //cc:=0
	for nwpb := range mergeNodeAndWayNodes(nds, wayNodes) {
		for _, nwp := range nwpb {
			if numNode == nextP {
                debug.FreeOSMemory()
				fmt.Printf("\rexpandWayBoxes: %-8d // %-8d [%-10d [%-9d %-9d] w/ %-4d] %d tiles %s", numNode, numWN, nwp.id, nwp.ln, nwp.lt, len(nwp.ways),ans.NumTiles(),utils.MemstatsStr())
                //utils.WriteMemProfile()
                
				nextP += 1952373
			}
			numNode++
			numWN += len(nwp.ways)
			for _, w := range nwp.ways {
                //println(w,nwp.id,nwp.ln,nwp.lt)
				ans.Expand(w, nwp.ln, nwp.lt)
                //cc++
			}
            
            //if cc>100 {
            //    panic("")
            //}
		}
        nwpb = nil

	}
	fmt.Printf("\rexpandWayBoxes: %-8d // %-8d: %-8.1fs  %d tiles %s %-50s\n", numNode, numWN, time.Since(st).Seconds(),ans.NumTiles(),utils.MemstatsStr(), "")
	fmt.Printf("%-10d wayboxes [between %-10d and %-10d]\n", ans.Len(), mw, Mw)
	return ans
}


func calcWayQts(infn string, abs blocksort.AllocBlockStore) (objQt, error) {

	
	qts := newObjQtImpl()

    /*
    for i:=0; i < 4; i++ {
        qts = expandWayBoxes(infn, abs, elements.Ref(i)<<26, elements.Ref(i+1)<<26).Qts(qts, 18, 0.05)
        debug.FreeOSMemory()
        utils.WriteMemProfile()
        
    }
    qts = expandWayBoxes(infn, abs, 4<<26, 1<<45).Qts(qts, 18, 0.05)
    */
    mp := elements.Ref(10000)<<14
    qts = expandWayBoxes(infn, abs, 0, mp).Qts(qts, 18, 0.05)
    debug.FreeOSMemory()
    qts = expandWayBoxes(infn, abs, mp, 1<<45).Qts(qts, 18, 0.05)
    debug.FreeOSMemory()
    //utils.WriteMemProfile()
    
	return qts, nil
}

type qtMap map[elements.Ref]quadtree.Quadtree

func findNodeQts(
    infn string,
	abs blocksort.AllocBlockStore,
	rels elements.Block,
	wayQts objQt,
    res chan elements.ExtendedBlock) (qtMap, int, error) {

	relnds := qtMap{}
        
	for i:=0; i < rels.Len();i++ {
        mm:=rels.Element(i).(elements.Members)
		for j:=0; j < mm.Len(); j++ {
            if mm.MemberType(j)==elements.Node {
                relnds[mm.Ref(j)] = quadtree.Null            
            }
        }
	}

	nds := iterNodes(infn)
    wayNodes:=readParentChildSliceBlockSort(abs,0,1<<61)
    
	k := 0
	st := time.Now()
	numNode, numWN, nextP := 0, 0, 0
	
    
    for nwp := range mergeNodeAndWayNodes(nds, wayNodes) {
		
        rl := make(elements.ByElementId, 0, len(nwp))
		for _, nw := range nwp {
			if nw.id == 0 {
				continue
			}
			if numNode == nextP {
				fmt.Printf("\rcalcNodeQts: %-8d // %-8d [%-10d [%-9d %-9d] w/ %-4d] %s", numNode, numWN, nw.id, nw.ln, nw.lt, len(nw.ways),utils.MemstatsStr())
				nextP += 19523717
			}
			numNode++
			numWN += len(nw.ways)

			q := quadtree.Null
			if len(nw.ways) > 0 {
				for _, w := range nw.ways {
					q = q.Common(wayQts.Get(w))
				}
			} else {
                var err error
				q,err = quadtree.Calculate(quadtree.Bbox{nw.ln, nw.lt, nw.ln+1, nw.lt+1}, 0.05, 18)
                if err!=nil { panic(err.Error()) }
			}
			if q==quadtree.Null {
                ww:=make([]quadtree.Quadtree,len(nw.ways))
                for i,w:=range nw.ways {
                    ww[i]=wayQts.Get(w)
                }
                panic(fmt.Sprintf("wtf %d %d %d %s %s",nw.id,nw.ln,nw.lt,nw.ways,ww))
            }
            if _, ok := relnds[nw.id]; ok {
				relnds[nw.id] = q
			}
			
            rl = append(rl, read.MakeObjQt(elements.Node,nw.id, q))
		}

		if len(rl) > 0 {
			res <- elements.MakeExtendedBlock(k, rl, quadtree.Null, 0, 0, nil)
			k++
		}

	}
	fmt.Printf("\rcalcNodeQts: %-8d // %-8d: %-8.1fs %s%-100s \n", numNode, numWN, time.Since(st).Seconds(),utils.MemstatsStr(), "")
    debug.FreeOSMemory()
    //utils.WriteMemProfile()
    
	return relnds, k, nil

}

func writeWayQts(wayQts objQt,
	rels elements.Block,
	k int,
	res chan elements.ExtendedBlock) (qtMap, int, error) {

    relwys := qtMap{}
        
	for i:=0; i < rels.Len();i++ {
        mm:=rels.Element(i).(elements.Members)
		for j:=0; j < mm.Len(); j++ {
            if mm.MemberType(j)==elements.Way {
                relwys[mm.Ref(j)] = quadtree.Null            
            }
        }
	}
        

	for bl := range wayQts.ObjsIter(1, 8000) {
		for _, b := range bl {
			i := b.Id()
            
			if _, ok := relwys[i]; ok {
                relwys[i] = b.(interface {Quadtree() quadtree.Quadtree }).Quadtree()
			}
		}
		res <- elements.MakeExtendedBlock(k, bl, quadtree.Null, 0, 0, nil)
		k++
	}
	return relwys, k, nil

}

func writeRelQts(
    nn qtMap, ww qtMap,
	rels elements.Block,
	k int,
	res chan elements.ExtendedBlock) (int, error) {

	rls := newObjQtImpl()
	rr := make(nodeWaySlice, 0, 10000)
	
    for i:=0; i < rels.Len();i++ {
        e:=rels.Element(i)
        ei:=e.Id()
        mm := e.(elements.Members)
        
        if mm.Len()==0 {
            rls.Set(ei, 0)
        }
		
        for j:=0; j < mm.Len(); j++ {
            switch mm.MemberType(j) {
                case elements.Node:
                    rls.Expand(ei, nn[mm.Ref(j)])
                case elements.Way:
                    rls.Expand(ei, ww[mm.Ref(j)])
                case elements.Relation:
                    
                    rr=append(rr,nodeWay{ei,mm.Ref(j)})
                
            }
        }
	}
    
	for i := 0; i < 5; i++ {
		for _, p := range rr {
            if i==0 && p.node==p.way {
                fmt.Printf("circular rel: %d: curr val %s\n",p.node,rls.Get(p.node))
                if rls.Get(p.node)==quadtree.Null {
                    rls.Set(p.node, 0)
                }
            }
            oq:=rls.Get(p.way)
            if oq!=quadtree.Null {
                rls.Expand(p.node, oq)
            }
		}
	}

	for bl := range rls.ObjsIter(elements.Relation, 8000) {
		res <- elements.MakeExtendedBlock(k, bl, quadtree.Null, 0, 0, nil)
		k++
	}

	return k, nil
}

func CalcObjectQts(infn string) (<-chan elements.ExtendedBlock, error) {
	
    abs, rels, err := readWayNodes(infn,4)
	if err != nil {
		return nil, err
	}
	
    debug.FreeOSMemory()
    
    
    /*
    wmn, wmx := wayNodes.MinMax()
	fmt.Printf("WayNodes: %-10d %-12d => %-12d\n", wayNodes.Len(), wmn, wmx)
	rmn, rmx := relMems.MinMax()
	fmt.Printf("RelMembs: %-10d %d %-10d => %d %-10d\n", relMems.Len(), rmn>>61, rmn&0x1fffffffffffffff, rmx>>61, rmx&0x1fffffffffffffff)
	fmt.Printf("have %d rels\n", rls.Len())
    */
    
    
	wayQts, err := calcWayQts(infn, abs)
	if err != nil {
		return nil, err
	}
	//println("have", wayQts.Len(), "way qts")
	res := make(chan elements.ExtendedBlock)
	go func() {
		nn, k, err := findNodeQts(infn, abs, rels, wayQts, res)
		if err != nil {
			panic(err.Error())
		}
		ww, k, err := writeWayQts(wayQts, rels, k, res)
		if err != nil {
			panic(err.Error())
		}
        abs.Finish()
        
		k, err = writeRelQts(nn, ww, rels, k, res)
		
        if err != nil {
			panic(err.Error())
		}
        close(res)
        		
	}()
	return res, nil
}
