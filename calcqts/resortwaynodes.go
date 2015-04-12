// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

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
    //"sync"
)

type psp struct {
    idx, nn, mm int
    bls string
}

func readWayNodes(infn string, nc int) (blocksort.AllocBlockStore, int, elements.Block, error) {
    
    rels := make([]elements.ByElementId, nc)
    for i,_:=range rels {
        rels[i] = make(elements.ByElementId, 0, 1000000)
    }
    
    inChans, err := readfile.ReadSomeElementsMulti(infn,nc,false,true,true)
    if err!=nil { return nil,0,nil,err}
    
    nws := make([]int,nc)
    
    
    prog := make(chan psp)
    
    
    
    addFunc  := func(block elements.ExtendedBlock, res chan blocksort.IdPacked) error {
        
        if block==nil { return nil }
                
        cc := block.Idx() % nc  
        
        if block.Len()==0 { return nil }
 
        
        rp := map[int]nodeWaySlice{}            
        
        for i:=0; i < block.Len(); i++ {
            e:=block.Element(i)
            switch e.Type() {
                case elements.Node:
                    continue
                case elements.Way:
                    nws[cc]++
                    ei := e.Id()
                    rf,ok := e.(elements.Refs)
                    if !ok {
                        return errors.New("NO REFS")
                    }
                    for j:=0; j < rf.Len(); j++ {
                        r:=rf.Ref(j)
                        k:=int(r>>20) //split nodes into groups of 1<<20 ids
                        rp[k] = append(rp[k], nodeWay{r,ei})
                        
                    }
                    //add rels to relation block
                case elements.Relation:
                    rels[cc] = append(rels[cc], e)
            }
        }
        
        
        
        nn,mm:=0,0
        for k,v := range rp {
            res <- blocksort.IdPacked{k, v.Pack()}
            nn += len(v)
            mm += 1
        }
        //send progress message
        prog <- psp{block.Idx(),nn,mm,block.String()}
                
        return nil
    }
    
    abs := blocksort.MakeAllocBlockStore("tempfileslim")
    
    
    go func() {
        st:=time.Now()
        progstep := 1874215
        nn,mm,nb,ps,idx := 0,0,1,"",0
        
        for p := range prog {
            nn += p.nn
            mm += p.mm
            idx= p.idx
            ps = p.bls
            if nn > nb { //write progress message about once per second
                fmt.Printf("\r%8.1fs %10d [%10d %10d] %s", time.Since(st).Seconds(),idx,nn,mm,ps)
                nb += progstep 
            }
        }
        fmt.Printf("\r%8.1fs %10d [%10d %10d] %s\n", time.Since(st).Seconds(),idx,nn,mm,ps)
        fmt.Printf("have %d objs in %d blocks\n", abs.TotalLen(), abs.NumBlocks())
    }()
    
                
    
    err =  blocksort.AddData(abs,inChans,addFunc)
    
    if err!=nil {
        return nil,0,nil,err
    }
    close(prog)
    
    
    // merge and sort the relation blocks
    
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
    
    nw:=0
    for _,n:=range nws { nw+=n }
    fmt.Println("have",nw,"ways")
    fmt.Println("have",len(relsf),"rels")
    return abs,nw,relsf,nil
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


func readParentChildSliceBlockSort(abs blocksort.AllocBlockStore, mn elements.Ref, mx elements.Ref) <-chan nodeWaySlice {
    
    // split reading from BlockStoreAlloc into four parallel chans
    resp:=make([]chan nodeWaySlice, 4)
    for i,_:=range resp {
        resp[i] = make(chan nodeWaySlice)
    }
    
    add:=func(i int, blob blocksort.BlockStoreAllocPair) error {
        all := blob.Block.All()
        rr := make([]nodeWaySlice, all.Len())
        for j,_ := range rr {
            rr[j] = readParentChildSlice(all.At(j).Data,mn,mx)
        }
        //write to each channel in turn
        resp[blob.Idx%4] <- mergeParentChildSlice(rr)
        return nil
    }
    
    go func() {
        blocksort.ReadData(abs,4,add)
        for _,r:=range resp {
            close(r)
        }
    }()
    
    res := make(chan nodeWaySlice)
    go func() {
        //merge 4 channels into one, preserving order
        rem:=4
        ii:=0
        for rem>0 { //number of remaining channels
            rr,ok := <-resp[ii%4]
            if ok {
                res <- rr
            } else {
                rem -= 1
            }
            ii++
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

func iterNodes(infn string) <-chan []nodeAndWays {
    
    blcks, err := readfile.ReadSomeElementsMulti(infn, 4, true, false, false)
    if err!=nil { return nil }
    
    
    output := make(chan []nodeAndWays)
    go func() {
        for bl:=range readfile.CollectExtendedBlockChans(blcks,false) {
            if (bl.Idx()%10000)==0 {
                debug.FreeOSMemory()
            }
            
            if bl.Len()==0 {
                continue
            }
            r:=make([]nodeAndWays,0,bl.Len())
        
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
                    r=append(r,nn)
                }
                
            }
            if len(r)>0 {
                output <- r
            }
        }
        close(output)
    }()
 
    return output
}
func nextWN(wns nodeWaySlice, l int) (int, elements.Ref, []elements.Ref) {
    //group nodeWaySlice by common node id
    n:=wns[l].node
    for i:=l; i < len(wns); i++ {
        if wns[i].node != n {
            ww:=make([]elements.Ref,0,i-l)
            for j:=l; j < i; j++ {
                ww = append(ww, wns[j].way)
            }
            return i, n, ww
        }
    }
    
    ww:=make([]elements.Ref,0,len(wns)-l)
    for j:=l; j < len(wns); j++ {
        ww = append(ww, wns[j].way)
    }
    return len(wns), n, ww
}


func mergeNodeAndWayNodes(nodes <-chan []nodeAndWays, wayNodes <-chan nodeWaySlice) <-chan []nodeAndWays {

    res := make(chan []nodeAndWays)
    go func() {
        wns,ok := <- wayNodes
        if !ok {
            panic("NO WAYNODES")
        }
        wnsi,n,ww := nextWN(wns,0)
        
        for bl := range nodes {
            
            for i,nn := range bl {
                
                for ok && (n <= nn.id) {
                    if n<nn.id {
                        println("MISSING NODE", n, ww)
                    } else {
                        //add ways to nodeAndWays
                        bl[i].ways=ww
                    }
                    
                    if wnsi == len(wns) {
                        //at end, fetch next waynode block
                        for ok && wnsi == len(wns) {
                            wns,ok = <-wayNodes
                            wnsi=0
                        }
                    }
                    
                    //fetch next ways for next node
                    if ok {
                        wnsi,n,ww = nextWN(wns,wnsi)
                    }
                    
                }
                //for nodes without ways, do nothing
            }
            
            res <- bl
        }
        close(res)
    }()
    return res

}

func expandWayBoxes(infn string, abs blocksort.AllocBlockStore, mw elements.Ref, Mw elements.Ref, useDense bool) wayBbox {
	ans := newWayBbox(useDense)

	st := time.Now()
	numNode, numWN, nextP := 0, 0, 0
    nds := iterNodes(infn)
    wayNodes:=readParentChildSliceBlockSort(abs,mw,Mw)
    
    for nwpb := range mergeNodeAndWayNodes(nds, wayNodes) {
		for _, nwp := range nwpb {
			if numNode == nextP {
                fmt.Printf("\rexpandWayBoxes: %-8d // %-8d [%-10d [%-9d %-9d] w/ %-4d] %d tiles %s", numNode, numWN, nwp.id, nwp.ln, nwp.lt, len(nwp.ways),ans.NumTiles(),utils.MemstatsStr())
                nextP += 1952373
			}
			numNode++
			numWN += len(nwp.ways)
			for _, w := range nwp.ways {
                ans.Expand(w, nwp.ln, nwp.lt)
			}
            nwp.ways=nil
		}
        nwpb = nil

	}
	fmt.Printf("\rexpandWayBoxes: %-8d // %-8d: %-8.1fs  %d tiles %s %-50s\n", numNode, numWN, time.Since(st).Seconds(),ans.NumTiles(),utils.MemstatsStr(), "")
	fmt.Printf("%-10d wayboxes [between %-10d and %-10d]\n", ans.Len(), mw, Mw)
	return ans
}


func calcWayQts(infn string, abs blocksort.AllocBlockStore, useDense bool) (objQt, error) {

	
	qts := newObjQt(useDense)
    if !useDense {
        qts = expandWayBoxes(infn, abs, 0, 1<<45,false).Qts(qts, 18, 0.05)
    
    } else {
        //split into three parts: memory use gets too high overwise
        mp := elements.Ref(500 * tileLen)
        qts = expandWayBoxes(infn, abs, 0, mp,useDense).Qts(qts, 18, 0.05)
        debug.FreeOSMemory()
        qts = expandWayBoxes(infn, abs, mp, 2*mp,useDense).Qts(qts, 18, 0.05)
        debug.FreeOSMemory()
        qts = expandWayBoxes(infn, abs, 2*mp, 1<<45,useDense).Qts(qts, 18, 0.05)
    }
    debug.FreeOSMemory()
    
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
    
    //find which nodes are members of relations: we will fill this in
    //as we calculate the node qt values
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
				nextP += 1952371
			}
			numNode++
			numWN += len(nw.ways)

			q := quadtree.Null
			if len(nw.ways) > 0 { //if a node has way members, 
                //the qt value is the highest common value for the ways
				for _, w := range nw.ways {
					q = q.Common(wayQts.Get(w))
				}
			} else {
                //otherwise it base on the node location
                var err error
				q,err = quadtree.Calculate(quadtree.Bbox{nw.ln, nw.lt, nw.ln+1, nw.lt+1}, 0.05, 18)
                if err!=nil { panic(err.Error()) }
			}
            
			if q==quadtree.Null {
                //something has gone very wrong
                ww:=make([]quadtree.Quadtree,len(nw.ways))
                for i,w:=range nw.ways {
                    ww[i]=wayQts.Get(w)
                }
                panic(fmt.Sprintf("wtf %d %d %d %s %s",nw.id,nw.ln,nw.lt,nw.ways,ww))
            }
            
            
            if _, ok := relnds[nw.id]; ok { //is it a member of a relation?
				relnds[nw.id] = q //store qt if it is
			}
			
            
            rl = append(rl, read.MakeObjQt(elements.Node,nw.id, q))
            
            nw.ways=nil
            
		}
        nwp=nil

		if len(rl) > 0 {
            //pack into a block
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
    //find which ways are members of relations
	for i:=0; i < rels.Len();i++ {
        mm:=rels.Element(i).(elements.Members)
		for j:=0; j < mm.Len(); j++ {
            if mm.MemberType(j)==elements.Way {
                relwys[mm.Ref(j)] = quadtree.Null            
            }
        }
	}
        

    //iter over way qts in blocks of 8000
	for bl := range wayQts.ObjsIter(1, 8000) {
		for _, b := range bl {
			i := b.Id()
            //if a member of a relation, store qt value
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

	rls := newObjQt(false)
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
                //expand node and way members
                case elements.Node:
                    rls.Expand(ei, nn[mm.Ref(j)])
                case elements.Way:
                    rls.Expand(ei, ww[mm.Ref(j)])
                case elements.Relation:
                    
                    rr=append(rr,nodeWay{ei,mm.Ref(j)})
                
            }
        }
	}
    //for relation members, 
	for i := 0; i < 5; i++ {
		for _, p := range rr {
            if i==0 && p.node==p.way {
                fmt.Printf("circular rel: %d: curr val %s\n",p.node,rls.Get(p.node))
                if rls.Get(p.node)==quadtree.Null {
                    rls.Set(p.node, 0)
                }
            }
            //expand if child relation has a qt value yet
            oq:=rls.Get(p.way)
            if oq!=quadtree.Null {
                rls.Expand(p.node, oq)
            }
		}
	}//repeating 5 times ensures nested qt heirarchies all have a value


    //iter over relations in groups of 8000
	for bl := range rls.ObjsIter(elements.Relation, 8000) {
		res <- elements.MakeExtendedBlock(k, bl, quadtree.Null, 0, 0, nil)
		k++
	}

	return k, nil
}


// Calculate a quadtree value for each entity in infn.
func CalcObjectQts(infn string) (<-chan elements.ExtendedBlock, error) {
	
    //rearrange way nodes, store relations
    abs, numWays, rels, err := readWayNodes(infn,4)
	if err != nil {
		return nil, err
	}
	
    debug.FreeOSMemory()
    
    
    useDense := numWays > 40000000
    
    fmt.Printf("%d ways, %d rels: useDense? %t %s\n", numWays, rels.Len(),useDense,utils.MemstatsStr())
	wayQts, err := calcWayQts(infn, abs, useDense)
	if err != nil {
		return nil, err
	}
	
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
