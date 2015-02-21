package update

import (
	"github.com/jharris2268/osmquadtree/calcqts"
	"github.com/jharris2268/osmquadtree/locationscache"
	"github.com/jharris2268/osmquadtree/readfile"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/read"
    "github.com/jharris2268/osmquadtree/xmlread"

	"fmt"
	"sort"
	//"time"

	//"runtime/debug"
)

type tempObjStore struct {
	c []elements.ByElementId
}

func newTempObjStore() *tempObjStore {
	return &tempObjStore{make([]elements.ByElementId, 0, 100)}
}

func (tos *tempObjStore) add(o elements.Element) {
	ct := len(tos.c) - 1
	if ct < 0 || len(tos.c[ct]) == 8000 {
		tos.c = append(tos.c, make(elements.ByElementId, 0, 8000))
		ct++
	}
	tos.c[ct] = append(tos.c[ct], elements.PackedElement(o.Pack()))
}

func (tos *tempObjStore) get(i int) elements.Element {
	return tos.c[i/8000][i%8000]
}
func (tos *tempObjStore) Len() int {
	ct := len(tos.c) - 1
	if ct < 0 {
		return 0
	}
	return ct*8000 + len(tos.c[ct])
}

func (tos *tempObjStore) Swap(i, j int) {
	tos.c[i/8000][i%8000], tos.c[j/8000][j%8000] = tos.c[j/8000][j%8000], tos.c[i/8000][i%8000]
}
func (tos *tempObjStore) Less(i, j int) bool {
	oi := tos.get(i)
	oj := tos.get(j)
    return elements.Less(oi,oj)
}

func filterLastObj(inc <-chan elements.Element) <-chan elements.Element {

	res := make(chan elements.Element)

	go func() {
		createAndDel := 0
		cobs := make([]elements.Element, 0, 10)
		for o := range inc {

			if len(cobs) > 0 && (cobs[0].Type() != o.Type() || cobs[0].Id() != o.Id()) {
				c0 := cobs[0]
				c1 := cobs[len(cobs)-1]
				if c0.ChangeType() == elements.Create && c1.ChangeType() == elements.Delete {
					createAndDel++
				} // else {
				res <- c1
				//}
				cobs = make([]elements.Element, 0, 10)
			}
			cobs = append(cobs, o)
		}
		if len(cobs) > 0 {
			c0 := cobs[0]
			c1 := cobs[len(cobs)-1]
			if c0.ChangeType() == elements.Create && c1.ChangeType() == elements.Delete {
				createAndDel++
			} // else {
			res <- c1
			//}
		}
		println("passing", createAndDel, "create and deletes")
		close(res)
	}()
	return res
}
type nodeLoc struct {
    lon,lat int64
}
type nodeLocMap map[elements.Ref]nodeLoc
type wayNodeSet map[elements.Ref]bool
type objQtMap   map[elements.Ref]quadtree.Quadtree

func packId(e elements.Element) elements.Ref {
    return (elements.Ref(e.Type()) << 59) | e.Id()
}
func packMemAt(mm elements.Members, i int) elements.Ref {
    return (elements.Ref(mm.MemberType(i)) << 59) | mm.Ref(i)
}

func fetchChangeObjs(xmlfn string) (*tempObjStore, nodeLocMap, objQtMap, wayNodeSet, elements.Timestamp, error) {

	changeobjs := newTempObjStore()

	xmlc, err := xmlread.ReadOsmXmlFile(xmlfn)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	z := 0

	nodelocs := nodeLocMap{}
	wayNodes := wayNodeSet{}
	objQts := objQtMap{}

	maxts := elements.Timestamp(0)
    
	for e := range filterLastObj(xmlc) {
		if (z % 132877) == 0 {
			fmt.Printf("%-10d: %-90s %-3d %-4d\n", z, e, len(changeobjs.c), changeobjs.Len())
		}
		z++
		changeobjs.add(e)

		oi := packId(e)
		objQts[oi] = -1
		if e.ChangeType() != elements.Delete {
			ts := e.(interface{Info() elements.Info}).Info().Timestamp()
			if ts > maxts {
				maxts = ts
			}
            
            switch e.Type() {
                case elements.Node:
                    ll := e.(elements.LonLat)
                    nodelocs[oi] = nodeLoc{ll.Lon(),ll.Lat()}
                case elements.Way:
                    nn := e.(elements.Refs)
                    for i := 0; i < nn.Len(); i++ {
                        wayNodes[nn.Ref(i)] = true
                    }
                case elements.Relation:
                    rr := e.(elements.Members)
                    objQts[packId(e)]=quadtree.Null
                    for i := 0; i < rr.Len(); i++ {
                        objQts[packMemAt(rr,i)]=quadtree.Null
                        
                    }
			}

		}
	}

	o := changeobjs.get(changeobjs.Len() - 1)
	fmt.Printf("%-10d: %-90s %-3d %-4d\n", z, o.String(), len(changeobjs.c), changeobjs.Len())
	return changeobjs, nodelocs, objQts, wayNodes, maxts, nil
}

type int64slice []int64

func (ii int64slice) Len() int           { return len(ii) }
func (ii int64slice) Swap(i, j int)      { ii[i], ii[j] = ii[j], ii[i] }
func (ii int64slice) Less(i, j int) bool { return ii[i] < ii[j] }

func iterObjIds(objQts objQtMap, wayNodes wayNodeSet) chan int64 {

	rr := make(int64slice, 0, len(objQts)+len(wayNodes))
	for o, _ := range objQts {
		rr = append(rr, int64(o))
	}
	for o, _ := range wayNodes {
		if _, ok := objQts[o]; !ok {
			rr = append(rr, int64(o))
		}
	}
	sort.Sort(rr)

	outc := make(chan int64)
	go func() {
		for _, o := range rr {
			outc <- o
		}
		close(outc)
	}()
	return outc
}

func findExistingObjs(ts map[int64]int, nfs map[int64]srcBlock,
	objQts objQtMap, nodeLocs nodeLocMap,
	wayNodes wayNodeSet, tsp map[elements.Ref]int64) (*tempObjStore, error) {

	mxS := int64(0)
	for i, _ := range nfs {
		if i > mxS {
			mxS = i
		}
	}

	tempobjs := newTempObjStore()
	type blii struct {
		ii int64
		bl elements.Block
	}
	incc := make(chan blii)
	go func() {
		for i := int64(0); i <= mxS; i++ {
			ss, ok := nfs[i]
			if !ok {
				continue
			}

			fps := make(int64slice, 0, ss.idx.Len())

			for t, _ := range ts {
				if (t >> 32) == i {
					//println(t,i,t&0xffffffff,len(ss.idx))
					fp := ss.idx.Filepos(int(t & 0xffffffff))
					fps = append(fps, fp)

				}
			}
			sort.Sort(fps)

			fmt.Printf("load %d tiles from %s\n", len(fps), ss.fn)
			
            
            bll,err := readfile.ReadExtendedBlockMultiSortedPartial(ss.fn,4,fps)
            
            if err != nil {
				panic(err.Error())
			}
            
            for bl := range bll {
                incc <- blii{i,bl}
            }
            
			
		}
		close(incc)
	}()

	toc := make(chan elements.Element)

	go func() {
		for blk := range incc {
			bl := blk.bl
			for j := 0; j < bl.Len(); j++ {
				o := bl.Element(j)

				if o.ChangeType() != elements.Delete && o.ChangeType() != elements.Remove {

					oi := packId(o)
					t, ok := tsp[oi]
                    
                    oq := o.(elements.Quadtreer).Quadtree()
					
					if ok && (t>>32 == blk.ii) {

						if _, ok := objQts[oi]; ok {
							objQts[oi] = oq
						}
						if o.Type() == elements.Node {
							objQts[oi] = oq
							toc <- o
						}
					}
				}
			}
		}
		close(toc)

	}()
    
    println("have",len(nodeLocs),"nodeLocs")
    
	tos := map[elements.Ref]int{}
	cc := 0
	for o := range toc {
		oi := o.Id()
		if _, ok := wayNodes[oi]; ok {
			if _, ok := nodeLocs[oi]; !ok {
				pp, ok := tos[oi]
				if ok { //&& ot.Info().Version()>=o.Info().Version() {
					panic(fmt.Sprintf("[%d] all ready have added obj %s @ %d", cc, o.String(), pp))
				} else {
					//op := osmread.SimpleObjPacked(o.Pack())
                    switch oi {
                        case 16485399,966008526,959485761,2189869964:
                            println(o.String())
                    }
					tempobjs.add(o)
                    ll := o.(elements.LonLat)
                    
					nodeLocs[oi] = nodeLoc{ll.Lon(),ll.Lat()}
					
					tos[oi] = cc
					cc++
				}
			}
		}
	}
    println("have",len(nodeLocs),"nodeLocs")
	
	sort.Sort(tempobjs)
	
	return tempobjs, nil
}

type srcBlock struct {
	ts  elements.Timestamp
	fn  string
	idx read.BlockIdx
}

func newChangeEle(e elements.Element, ct elements.ChangeType, q quadtree.Quadtree) elements.Element {
    ee := elements.UnpackElement(e.Pack())
    
    ee.SetChangeType(ct)
    ee.SetQuadtree(q)
    return ee
}


func CalcUpdateTiles(prfx string, xmlfn string, enddate elements.Timestamp, newfn string, state int64) (<-chan elements.ExtendedBlock, quadtree.QuadtreeSlice, error) {
	changeobjs, nodelocs, objQts, wayNodes, maxts, err := fetchChangeObjs(xmlfn)
	
    fmt.Printf("prfx=%s; %d tiles, %d nodelocs, %d objQts, %d waynodes\n",
		prfx, changeobjs.Len(), len(nodelocs), len(objQts), len(wayNodes))
	
    fmt.Printf("given ts: %-10d [%-15s]; max ts: %-10d [%-15s] [%-6d]\n",
		enddate, enddate,
		maxts, maxts,
		maxts-enddate)

	specs, qts,err := locationscache.GetCacheSpecs(prfx)
    if err!=nil { return nil,nil,err}

	startdate := specs[len(specs)-1].Timestamp

	if err != nil {
		return nil, nil,err
	}

	lcs := locationscache.GetTiles(prfx, iterObjIds(objQts, wayNodes))

	ts := map[int64]int{}
	tsp := map[elements.Ref]int64{}

	nvs, ivs := 0, 0
	for c := range iterObjIds(objQts, wayNodes) {
		k := c / 32
		t, ok := lcs[k]
		if ok {
			if t[c%32] == 0 {
				ivs += 1
			} else {
				ts[t[c%32]-1]++
				tsp[elements.Ref(c)] = t[c%32] - 1
			}

		} else {
			nvs += 1
		}
	}

	nfs := map[int64]srcBlock{}
	for k, _ := range ts {
		i := k >> 32
		if _, ok := nfs[i]; !ok {
			ss := srcBlock{}
			ss.ts = specs[i].Timestamp

			_, hb, err := readfile.GetHeaderBlock(prfx + specs[i].Filename)
			if err != nil {
				return nil, nil,err
			}

			ss.fn = prfx + specs[i].Filename

			ss.idx = hb.Index
			nfs[i] = ss
		}

	}

	fmt.Printf("%d loctiles fetched. %d new vals, %d not present and %d tiles to load\n", len(lcs), nvs, ivs, len(ts))
	fmt.Printf("%d files present; %d qts\n", len(nfs), len(qts))
	/*for k,s := range nfs {
	      println(k, s.ts, s.fn, len(s.idx))
	  }
	  println("have",len(tsp),"tsp")*/
	tempobjs, err := findExistingObjs(ts, nfs, objQts, nodelocs, wayNodes, tsp)
	tsp = nil
	if err != nil {
		return nil, nil,err
	}
	println(tempobjs.Len(), len(nodelocs))

	nqts := objQtMap{}
	qss := 0
	for i := 0; i < changeobjs.Len(); i++ {
		o := changeobjs.get(i)
		if o.ChangeType() != elements.Delete && o.Type() == elements.Way {
			
            bx := quadtree.NullBbox()
                        
            
            a,b,c,_,wn := elements.UnpackQtRefs(o.Pack())
            if c!=o.Id() { panic(fmt.Sprintf("?? %s %s %s %d", o, a,b,c)) }
            
			for k := 0; k < wn.Len(); k++ {
				n, ok := nodelocs[wn.Ref(k)]

				if !ok {
					println("missing node", wn.Ref(k), "from", o.String(), "@", k)
					panic("")
                    continue
				}
				//wbx.Expand(wid,n.Lon(),n.Lat())
                bx.ExpandXY(n.lon,n.lat)
			}
			q,_ := quadtree.Calculate(*bx, 0.05, 18)
			
            objQts[packId(o)] = q
			qss += 1
			for k := 0; k < wn.Len(); k++ {
				n := wn.Ref(k)
				oq, ok := nqts[n]
				if ok {
					oq = oq.Common(q)
				} else {
					oq = q
				}
				nqts[n] = oq
			}

		}
	}
	fmt.Printf("way qts set=%d; len(nqts)=%d", qss, len(nqts))

	for oi, q := range nqts {
		if oq, ok := objQts[oi]; ok {
			q = q.Common(oq)
		}
		objQts[oi] = q

	}

	nn0 := 0
	no0 := 0
	for n, q := range nqts {
		if q == 0 {
			nn0 += 1
		}
		if oq, ok := objQts[n]; ok {
			if oq == 0 {
				no0 += 1
			}
		}
	}
	fmt.Printf("%d nqts =0, o/w %d objQts = 0 \n", nn0, no0)

	for i := 0; i < changeobjs.Len(); i++ {
		o := changeobjs.get(i)
		if o.ChangeType() != 1 && o.Type() == 0 {
			n, ok := nqts[o.Id()]
			if !ok {
				a := nodelocs[o.Id()]
                
				n,_ = quadtree.Calculate(quadtree.Bbox{a.lon, a.lat, a.lon+1, a.lat+1},0.05, 18)
				oq, ok := objQts[o.Id()]
				if ok {
					n = n.Common(oq)
				}
				objQts[o.Id()] = n
			}
		}
	}

	rr := map[elements.Ref][]elements.Ref{}
	for i := 0; i < changeobjs.Len(); i++ {
		o := changeobjs.get(i)
		if o.ChangeType() != 1 && o.Type() == 2 {
			oi := packId(o)
            
            
            a,b,c,_,mm := elements.UnpackQtRefs(o.Pack())
            if c!=o.Id() { panic(fmt.Sprintf("?? %s %s %s %d", o, a,b,c)) }
            
            //ounp := elements.UnpackElement(o.Pack())
            
			//mm := ounp.(elements.Members)
			q,ok := objQts[oi]//quadtree.Null
            if !ok {
                q = quadtree.Null
            }
            
            
			if mm.Len() > 0 {
				for j := 0; j < mm.Len(); j++ {
					//mi := packMemAt(mm,j)
                    mi := mm.Ref(j)
					//if mm.MemberType(j) == elements.Relation {
                    if mi>>59 == 2 {
						rr[oi] = append(rr[oi], mi)
					} else {
                        
						q = q.Common(objQts[mi])
					}
				}
			}
            if o.Id()==5881 {
                println(o.String(),oi,q,q.String(),mm.Len())
                if mm.Len()>0 {
                    a:=mm.Ref(0)
                    b:=mm.Ref(mm.Len()-1)
                    println(a>>59,a&0xffffffff,b>>59,b&0xffffffff)
                }
            }
			objQts[oi] = q
		}
	}
	for i := 0; i < 5; i++ {
		for k, vv := range rr {
			for _, v := range vv {
				t, ok := objQts[v]
				if ok {
					objQts[k] = objQts[k].Common(t)
				}
			}
		}
	}

	println(len(objQts))
	qttree := calcqts.MakeQtTree(qts)

	qtsl := map[int64]quadtree.Quadtree{}
	for k, v := range nfs {
		//for i,q := range v.idx {
		for i := 0; i < v.idx.Len(); i++ {
			qq := (k << 32) | int64(i)
			qtsl[qq] = v.idx.Quadtree(i)
		}
	}
	println("qttree.Len()=", qttree.Len(), "len(qtsl)=", len(qtsl))
    zzzz:=0
	nodel, nomod := 0, 0
	createexisting := 0
	allocs := map[quadtree.Quadtree]elements.ByElementId{}
	for i := 0; i < changeobjs.Len(); i++ {
		o := changeobjs.get(i)
		oi := packId(o)

		oa := int64(-1)

		k := int64(oi / 32)
		t, ok := lcs[k]
		if ok && len(t) == 32 && t[oi%32] != 0 {
			oa = t[oi%32] - 1
		}
		oq := quadtree.Null
		if oa >= 0 {
			oq, ok = qtsl[oa]
			if !ok {
				panic(fmt.Sprintf("wtf %s %d %d %d", o.String(), k, oa, len(qts)))
			}

		}
		nqt, nqtok := objQts[oi]
        
        if nqt<0 && o.ChangeType() != 5 {
            if zzzz<25 {
                println("??", oi>>59,oi&0xffffffffff,nqt, o.ChangeType().String())
                zzzz++
            }
        }
        
		nq := quadtree.Null
		if !nqtok {
			if o.ChangeType() == 4 || o.ChangeType() == 5 {
				panic("no qt for " + o.String())
			}
		} else {
			nqi := qttree.Find(nqt)
			nq = qttree.At(nqi).Quadtree
		}

		
		switch o.ChangeType() {
		case 1:
			if oq >= 0 {

				allocs[oq] = append(allocs[oq], newChangeEle(o,1,0))
			} else {
				nodel++
			}
		case 4:

			if oq >= 0 && nq != oq {
				allocs[oq] = append(allocs[oq], newChangeEle(o, 2, 0))
			} else if oq < 0 {
				nomod++
			}
			allocs[nq] = append(allocs[nq], newChangeEle(o, 4, nqt))
		case 5:
			if oq >= 0 {
				//println("create existing obj???", o.String(),oq,oa)
				createexisting++
				if nq != oq {
					allocs[oq] = append(allocs[oq], newChangeEle(o, 2, 0))
				}
			}
			allocs[nq] = append(allocs[nq], newChangeEle(o, 5, nqt))
		}

	}
	println("have", nodel, "delete not present,", nomod, "modify not present")
	println("have", createexisting, "create existings")
	for i := 0; i < tempobjs.Len(); i++ {
		o := tempobjs.get(i)
		oi := packId(o)

		oa := int64(-1)

		k := int64(oi / 32)
		t, ok := lcs[k]
		if ok && len(t) == 32 && t[oi%32] != 0 {
			oa = (t[oi%32] - 1)
		}
		oq := quadtree.Null
		if oa >= 0 {
			oq, ok = qtsl[oa]
			if !ok {
				panic(fmt.Sprintf("wtf %s %d %d %d", o.String(), k, oa, len(qts)))
			}
		}
		nqt, nqtok := objQts[oi]
		if !nqtok {
			panic("no qt for " + o.String())

		}

        oqt := elements.UnpackElement(o.Pack()).(elements.Quadtreer).Quadtree()
		
        if nqt != oqt {
			nqi := qttree.Find(nqt)
			nq := qttree.At(nqi).Quadtree

			/*if o.ObjectId() == 2250146319 || o.ObjectId() == 2250146328 || o.ObjectId() == 2177704274 {
				fmt.Printf("\nfound object %s: k=%d, t[oi%%32]=%d,oqt=%s oa=%d, oq=%s, nqt=%s, nq=%s\n", o.String(), k, t[oi%32], quadtree.QuadtreeString(o.Quadtree()), oa, quadtree.QuadtreeString(oq), quadtree.QuadtreeString(nqt), quadtree.QuadtreeString(nq))
			}*/
			if nq != oq {
				allocs[oq] = append(allocs[oq], newChangeEle(o, 2, 0))
			}
			allocs[nq] = append(allocs[nq], newChangeEle(o, 3, nqt))
		} 
	}
	println("have", len(allocs), "allocs")

	ks := make(quadtree.QuadtreeSlice, 0, len(allocs))
	for k, _ := range allocs {
		ks = append(ks, k)
	}
	sort.Sort(ks)

	nlocs := locationscache.LocsMap{}
	if newfn != "" {

		iio := int64(len(specs)) << 32

		for i, k := range ks {

			ii := int64(i) | iio
			for _, v := range allocs[k] {
				oi := int64(packId(v))
                
				oa, ob := oi/32, oi%32
				if _, ok := nlocs[oa]; !ok {
					or, ok := lcs[oa]
					if ok && len(or) == 32 {
						nlocs[oa] = or[:]
					} else {
						nlocs[oa] = make([]int64, 32)
					}
				}
				switch v.ChangeType() {
				case 1:
					nlocs[oa][ob] = 0
				case 3, 4, 5:
					nlocs[oa][ob] = ii + 1
				}
			}
		}
		naz := 0
		for _, v := range nlocs {
			naz++
			for _, vi := range v {
				if vi != 0 {
					naz--
					break
				}
			}
		}
		println("have", len(nlocs), "new locations, with", naz, "all zero??")
		println("AddNewEntries", prfx, len(nlocs), len(specs), enddate, newfn, state)
		locationscache.AddNewEntries(prfx, nlocs, len(specs), enddate, newfn, state)
	}

	res := make(chan elements.ExtendedBlock)
	go func() {
		for i, k := range ks {
            vv := allocs[k]
            vv.Sort()
            
            /*if k==0 {
                for _,o:=range vv {
                    fmt.Println(o)
                }
            }*/
            
			res <- elements.MakeExtendedBlock(i, vv, k, startdate, enddate,nil )
		}
		close(res)
	}()
	return res, ks, nil
}
