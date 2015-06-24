// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package calcqts

import (
	"github.com/jharris2268/osmquadtree/blocksort"
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/readfile"

	"github.com/jharris2268/osmquadtree/read"
	"github.com/jharris2268/osmquadtree/utils"

    "fmt"
	"errors"
	"log"
	"runtime"
	"runtime/debug"
	"sort"
	"time"
)

type psp struct {
	idx, nn, mm int
	bls         string
}

func splitNWS(in *nodeWaySlice, sp uint) map[int]*nodeWaySlice {
	in.Sort()
	ll := map[int]int{}
	for _, nw := range in.nw {
		ll[int(nw.node>>sp)]++
	}
	out := map[int]*nodeWaySlice{}
	for k, v := range ll {
		out[k] = &nodeWaySlice{make([]nodeWay, 0, v)}
	}
	for _, nw := range in.nw {
		out[int(nw.node>>sp)].Add(nw.node, nw.way)
	}
	return out
}

func readWayNodes(infn string, nc int, tfs string, sp uint) (blocksort.AllocBlockStore, int, elements.Ref, elements.Block, error) {

	rels := make([]elements.ByElementId, nc)
	for i, _ := range rels {
		rels[i] = make(elements.ByElementId, 0, 1000000)
	}

	inChans, err := readfile.ReadSomeElementsMulti(infn, nc, false, true, true)
	if err != nil {
		return nil, 0, 0, nil, err
	}

	nws := make([]int, nc)
	mxw := make([]elements.Ref, nc)

	prog := make(chan psp)

	addFunc := func(block elements.ExtendedBlock, res chan blocksort.IdPacked) error {

		if block == nil {
			return nil
		}

		cc := block.Idx() % nc

		if block.Len() == 0 {
			return nil
		}

		rp := &nodeWaySlice{}

		for i := 0; i < block.Len(); i++ {
			e := block.Element(i)
			switch e.Type() {
			case elements.Node:
				continue
			case elements.Way:
				nws[cc]++
				ei := e.Id()
				if ei > mxw[cc] {
					mxw[cc] = ei
				}
				rf, ok := e.(elements.Refs)
				if !ok {
					return errors.New("NO REFS")
				}
				for j := 0; j < rf.Len(); j++ {
					r := rf.Ref(j)
					rp.Add(r, ei)

				}
				//add rels to relation block
			case elements.Relation:
				rels[cc] = append(rels[cc], elements.PackedElement(e.Pack()))
			}
		}

		rps := splitNWS(rp, sp)

		nn, mm := 0, 0
		for k, v := range rps {
			res <- blocksort.IdPacked{k, v.Pack()}
			nn += v.Len()
			v = nil
			mm += 1
		}
		//send progress message
		prog <- psp{block.Idx(), nn, mm, block.String()}
		rp = nil
		rps = nil
		return nil
	}

	abs := blocksort.MakeAllocBlockStore(tfs)

	go func() {
		st := time.Now()
		progstep := 1874215
		nn, mm, nb, ps, idx := 0, 0, 1, "", 0

		for p := range prog {
			nn += p.nn
			mm += p.mm
			idx = p.idx
			ps = p.bls
			if nn > nb { //write progress message about once per second
				fmt.Printf("\r%8.1fs %10d [%10d %10d] %s", time.Since(st).Seconds(), idx, nn, mm, ps)
				nb += progstep
			}
		}
		log.Printf("\r%8.1fs %10d [%10d %10d] %s\n", time.Since(st).Seconds(), idx, nn, mm, ps)
		log.Printf("have %d objs in %d blocks\n", abs.TotalLen(), abs.NumBlocks())
	}()

	err = blocksort.AddData(abs, inChans, addFunc)

	if err != nil {
		return nil, 0, 0, nil, err
	}
	close(prog)

	nr := 0
	for _, r := range rels {
		nr += len(r)
	}

	relsf := make(elements.ByElementId, nr)
	rr := 0
	for _, r := range rels {
		copy(relsf[rr:], r)
		rr += len(r)
	}
	relsf.Sort()

	nw := 0
	for _, n := range nws {
		nw += n
	}
	mx := elements.Ref(0)
	for _, w := range mxw {
		if w > mx {
			mx = w
		}
	}
	log.Println("have", nw, "ways", "max=", mx)
	log.Println("have", len(relsf), "rels")
	return abs, nw, mx, relsf, nil
}

type nodeWay struct {
	node, way elements.Ref
}

type nodeWaySlice struct {
	nw []nodeWay
}

func (pcs *nodeWaySlice) Len() int      { return len(pcs.nw) }
func (pcs *nodeWaySlice) Swap(i, j int) { pcs.nw[i], pcs.nw[j] = pcs.nw[j], pcs.nw[i] }
func (pcs *nodeWaySlice) Less(i, j int) bool {
	if pcs.nw[i].node == pcs.nw[j].node {
		return pcs.nw[i].way < pcs.nw[j].way
	}
	return pcs.nw[i].node < pcs.nw[j].node
}

func (pcs *nodeWaySlice) Add(n, w elements.Ref) {
	pcs.nw = append(pcs.nw, nodeWay{n, w})
}

func (pcs *nodeWaySlice) Sort() {
	sort.Sort(pcs)
}

func (pcs *nodeWaySlice) Pack() []byte {
	pcs.Sort()

	pk := make([]byte, 20*len(pcs.nw))
	lnode, lway := elements.Ref(0), elements.Ref(0)

	p := utils.WriteVarint(pk, 0, int64(len(pcs.nw)))
	for i := 0; i < len(pcs.nw); i++ {
		p = utils.WriteVarint(pk, p, int64(pcs.nw[i].node-lnode))
		p = utils.WriteVarint(pk, p, int64(pcs.nw[i].way-lway))
		lnode = pcs.nw[i].node
		lway = pcs.nw[i].way
	}

	return pk[:p]
}

func readParentChildSliceBlockSort(abs blocksort.AllocBlockStore, mn elements.Ref, mx elements.Ref) <-chan *nodeWaySlice {

	// split reading from BlockStoreAlloc into four parallel chans
	resp := make([]chan *nodeWaySlice, 4)
	for i, _ := range resp {
		resp[i] = make(chan *nodeWaySlice)
	}

	add := func(i int, blob blocksort.BlockStoreAllocPair) error {
		all := blob.Block.All()
		tl := 0
		for i := 0; i < all.Len(); i++ {
			tl += numWNS2(all.At(i).Data)
		}
		res := &nodeWaySlice{make([]nodeWay, 0, tl)}
		for i := 0; i < all.Len(); i++ {
			readWNS2(res, all.At(i).Data, mn, mx)
		}
		res.Sort()
		resp[blob.Idx%4] <- res
		blob.Block = nil

		return nil
	}

	go func() {
		blocksort.ReadData(abs, 4, add)
		for _, r := range resp {
			close(r)
		}
	}()

	res := make(chan *nodeWaySlice)
	go func() {
		//merge 4 channels into one, preserving order
		rem := 4
		ii := 0
		for rem > 0 { //number of remaining channels
			rr, ok := <-resp[ii%4]
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

func numWNS2(ss []byte) int {
	v, _ := utils.ReadVarint(ss, 0)
	return int(v)
}

func readWNS2(nws *nodeWaySlice, ss []byte, mn elements.Ref, mx elements.Ref) {
	v, p := utils.ReadVarint(ss, 0)
	x, y := elements.Ref(0), elements.Ref(0)
	for p < len(ss) {
		v, p = utils.ReadVarint(ss, p)
		x += elements.Ref(v)
		v, p = utils.ReadVarint(ss, p)
		y += elements.Ref(v)
		if y >= mn && y < mx {
			nws.nw = append(nws.nw, nodeWay{x, y})
		}
	}
}

type nodeWayInter interface {
	Len() int
	Id(int) elements.Ref
	Lon(int) int64
	Lat(int) int64
	NumWays(int) int
	Way(int, int) elements.Ref
	Clear()
}

type nn struct {
	id       elements.Ref
	lon, lat int64
	pl       int
}

type nodeWayBlock struct {
	nodes []nn
	ways  []elements.Ref
}

func (nwb *nodeWayBlock) Len() int              { return len(nwb.nodes) }
func (nwb *nodeWayBlock) Id(i int) elements.Ref { return nwb.nodes[i].id }
func (nwb *nodeWayBlock) Lon(i int) int64       { return nwb.nodes[i].lon }
func (nwb *nodeWayBlock) Lat(i int) int64       { return nwb.nodes[i].lat }
func (nwb *nodeWayBlock) NumWays(i int) int {
	a := nwb.nodes[i].pl
	if (i + 1) < len(nwb.nodes) {
		return nwb.nodes[i+1].pl - a
	}
	return len(nwb.ways) - a
}
func (nwb *nodeWayBlock) Way(i int, j int) elements.Ref {
	return nwb.ways[nwb.nodes[i].pl+j]
}

func (nwb *nodeWayBlock) Clear() {
	nwb.nodes = nil
	nwb.ways = nil
}

func mergeNodeAndWayNodesBlock(infn string, wayNodes <-chan *nodeWaySlice) <-chan nodeWayInter {

	res := make(chan nodeWayInter)
	go func() {
		wns, ok := <-wayNodes

		wnsi := 0
		for ok && wnsi == len(wns.nw) {
			wns, ok = <-wayNodes
			wnsi = 0
		}

		if !ok {
			panic("NO WAYNODES")
		}

		mnn := 0
		blcks, err := readfile.ReadSomeElementsMulti(infn, 4, true, false, false)
		if err != nil {
			panic(err.Error())
		}

		for bl := range readfile.CollectExtendedBlockChans(blcks) {

			if bl.Len() == 0 {
				continue
			}

			nwb := &nodeWayBlock{}
			nwb.nodes = make([]nn, bl.Len())
			ln := bl.Element(bl.Len() - 1).Id()
			if ok && wns != nil && len(wns.nw) > 1 && wns.nw[len(wns.nw)-1].node > ln {
				j := wnsi
				for wns.nw[j].node <= ln {
					j++
				}
				nwb.ways = make([]elements.Ref, 0, j-wnsi)
			} else {
				nwb.ways = make([]elements.Ref, 0, 4*bl.Len())
			}

			for i := 0; i < bl.Len(); i++ {
				nn, nok := bl.Element(i).(interface {
					Id() elements.Ref
					Lon() int64
					Lat() int64
				})
				if !nok {
					panic("??")
				}
				nwb.nodes[i].id = nn.Id()
				nwb.nodes[i].lon = nn.Lon()
				nwb.nodes[i].lat = nn.Lat()
				nwb.nodes[i].pl = len(nwb.ways)

				if ok && (wns == nil || wnsi >= len(wns.nw)) {
					lw := 0
					if wns != nil {
						lw = len(wns.nw)
					}
					println("WTF", mnn, wnsi, lw, nn.Id(), nn.Lon(), nn.Lat())
					for ok && (wns == nil || wnsi == len(wns.nw)) {
						wns = nil
						wns, ok = <-wayNodes
						log.Println(wns == nil, ok)
						wnsi = 0
					}
				}

				if ok && wns != nil && (wns.nw[wnsi].node <= nn.Id()) {
					if wns.nw[wnsi].node < nn.Id() {
						println("MISSING NODE", mnn, wns.nw[wnsi].node, wns.nw[wnsi].way, nn.Id(), nn.Lon(), nn.Lat())
						mnn++
						if mnn > 25 {
							panic("missing nodes")
						}
					} else {
						//add ways to nodeAndWays

						for ok && wns.nw[wnsi].node == nn.Id() {
							nwb.ways = append(nwb.ways, wns.nw[wnsi].way)

							wnsi += 1

							if wnsi == len(wns.nw) {
								//at end, fetch next waynode block
								for ok && wnsi == len(wns.nw) {
									wns = nil
									wns, ok = <-wayNodes
									wnsi = 0
								}
							}

						}
					}
				}
			}

			res <- nwb
		}

		close(res)
	}()
	return res
}

type nwbs interface {
	iterBlocks(mw, Mw elements.Ref) <-chan nodeWayInter
	finish()
}

type nwbsorig struct {
	infn string
	abs  blocksort.AllocBlockStore
}

func (nw *nwbsorig) iterBlocks(mw, Mw elements.Ref) <-chan nodeWayInter {
	wayNodes := readParentChildSliceBlockSort(nw.abs, mw, Mw)
	return mergeNodeAndWayNodesBlock(nw.infn, wayNodes)
}

func (nw *nwbsorig) finish() {
	nw.abs.Finish()
}

func makeNwbs(infn string, abs blocksort.AllocBlockStore) nwbs {

	return &nwbsorig{infn, abs}
}

func expandWayBoxes(nodeWays nwbs, mw elements.Ref, Mw elements.Ref, storeType int) wayBbox {
	ans := newWayBbox(storeType)

	reqgc := storeType != 2 && (Mw-mw) > 500*tileSz

	log.Println(mw, "to", Mw, "=>", (Mw-mw)/tileSz+1, "tiles; reqgc?", reqgc)

	st := time.Now()
	numNode, numWN, nextP := 0, 0, 0

	z := 0
	tz := 0.0
	gct := 0.0
	nn := 0
	ngc := 0
	for nwpb := range nodeWays.iterBlocks(mw, Mw) {
		nn++
		if reqgc && (nn%2000) == 0 {
			ss := time.Now()
			runtime.GC()
			gct += time.Since(ss).Seconds()
			ngc++
		}

		for i := 0; i < nwpb.Len(); i++ {

			if numNode == nextP {

				a, b, _ := utils.Memstats()
				if b > 9500 {
					z++
					ss := time.Now()
					debug.FreeOSMemory()
					tz += time.Since(ss).Seconds()
				}
				fmt.Printf("\rexpandWayBoxes: %-12d // %-12d %8.1fs [%-10d [% -12d % -12d] w/ %-4d] %12d vals [%8.1fmb // %8.1fmb]",
					numNode, numWN, time.Since(st).Seconds(),
					nwpb.Id(i), nwpb.Lon(i), nwpb.Lat(i),
					nwpb.NumWays(i), ans.Len(), a, b)
				nextP += 1952373

			}

			numNode++
			ln, lt := nwpb.Lon(i), nwpb.Lat(i)
			for j := 0; j < nwpb.NumWays(i); j++ {
				w := nwpb.Way(i, j)
				if w >= mw && w < Mw {
					ans.Expand(w, ln, lt)
					numWN++
				}
			}

		}
		nwpb.Clear()
		nwpb = nil
	}

	log.Printf("\rexpandWayBoxes: %-12d // %-12d: %-8.1fs  %-5d tiles %s %-50s\n", numNode, numWN, time.Since(st).Seconds(), ans.NumTiles(), utils.MemstatsStr(), "")
	log.Printf("%-10d wayboxes [between %-10d and %-10d]\n", ans.Len(), mw, Mw)
	log.Printf("called debug.FreeOSMemory() %d times [%8.1fs]", z, tz)
	if reqgc {
		log.Printf("; runtime.GC() %d times [%8.1fs]\n", ngc, gct)
	} else {
		log.Printf("\n")
	}
	return ans
}

func calcWayQts(nodeWays nwbs, storeType int, maxWay elements.Ref) (quadtreeStore, error) {

	debug.FreeOSMemory()
	qts := newQuadtreeStore(storeType > 0)
	if storeType != 1 {
		qts = expandWayBoxes(nodeWays /*infn, abs*/, 0, maxWay+1, storeType).Qts(qts, 18, 0.05)

	} else {
		//split into three parts: memory use gets too high overwise
		mp := elements.Ref(500 * tileLen)

		qts = expandWayBoxes(nodeWays /*infn, abs*/, 0, mp, storeType).Qts(qts, 18, 0.05)
		debug.FreeOSMemory()
		qts = expandWayBoxes(nodeWays /*infn, abs*/, mp, 2*mp, storeType).Qts(qts, 18, 0.05)
		debug.FreeOSMemory()
		qts = expandWayBoxes(nodeWays /*infn, abs*/, mp*2, maxWay+1, storeType).Qts(qts, 18, 0.05)
	}
	debug.FreeOSMemory()

	return qts, nil
}

type qtMap map[elements.Ref]quadtree.Quadtree

func findNodeQts(

	nodeWays nwbs,
	rels elements.Block,
	wayQts quadtreeStore,
	rls quadtreeStore,
	res chan elements.ExtendedBlock) (int, error) {

	ndrel := map[elements.Ref][]elements.Ref{}
	mw := 0
	//find which nodes are members of relations: we will fill this in
	//as we calculate the node qt values
	for i := 0; i < rels.Len(); i++ {
		e := rels.Element(i)
		mm := elements.UnpackElement(e.Pack()).(elements.Members)
		for j := 0; j < mm.Len(); j++ {
			switch mm.MemberType(j) {
			case elements.Node:
				ndrel[mm.Ref(j)] = append(ndrel[mm.Ref(j)], e.Id())
			case elements.Way:
				q := wayQts.Get(mm.Ref(j))
				if q < 0 {
					if mw < 10 {
						log.Printf("%5d MISSING WAY QT: relation %d, way %d\n", mw, e.Id(), mm.Ref(j))
					}
					mw++
				}
				rls.Expand(e.Id(), q)
			}
		}
	}
	log.Println("missing", mw, "way qts")

	k := 0
	st := time.Now()
	numNode, numWN, nextP := 0, 0, 0

	rl := make(elements.ByElementId, 0, 8000)
	var err error

	z := 0
	for nwpb := range nodeWays.iterBlocks(0, 1<<61) {
		z++

		for i := 0; i < nwpb.Len(); i++ {
			//for _,nw := range nww {

			//if nw.id==0 {
			if nwpb.Id(i) == 0 {
				continue
			}
			if numNode == nextP {

				fmt.Printf("\rcalcNodeQts: %-8d // %-8d %8.1fs [%-10d [%-9d %-9d] w/ %-4d] %s",
					numNode, numWN, time.Since(st).Seconds(),
					nwpb.Id(i), nwpb.Lon(i), nwpb.Lat(i),
					nwpb.NumWays(i), utils.MemstatsStr())
				nextP += 1952371
			}
			numNode++
			numWN += nwpb.NumWays(i)

			q := quadtree.Null
			for j := 0; j < nwpb.NumWays(i); j++ {
				q = q.Common(wayQts.Get(nwpb.Way(i, j)))
			}
			if q < 0 {

				q, err = quadtree.Calculate(quadtree.Bbox{nwpb.Lon(i), nwpb.Lat(i), nwpb.Lon(i) + 1, nwpb.Lat(i) + 1}, 0.05, 18)
				if err != nil {
					panic(err.Error())
				}
			}

			rr, ok := ndrel[nwpb.Id(i)]
			if ok {
				for _, r := range rr {
					rls.Expand(r, q)
				}
				delete(ndrel, nwpb.Id(i))
			}

			rl = append(rl, read.MakeObjQt(elements.Node, nwpb.Id(i), q))
			if len(rl) == 8000 {
				res <- elements.MakeExtendedBlock(k, rl, quadtree.Null, 0, 0, nil)
				k++
				rl = make(elements.ByElementId, 0, 8000)
			}
		}
	}
	if len(rl) > 0 {
		res <- elements.MakeExtendedBlock(k, rl, quadtree.Null, 0, 0, nil)
		k++
	}

	debug.FreeOSMemory()

	log.Printf("\rcalcNodeQts: %-8d // %-8d: %-8.1fs %s%-100s \n", numNode, numWN, time.Since(st).Seconds(), utils.MemstatsStr(), "")

	log.Printf("have %d missing node qts:\n", len(ndrel))
	zz := 0
	for k, v := range ndrel {
		if zz < 10 {
			log.Printf("% 2d node %d, rels %s\n", zz, k, v)
		}
		zz++
	}

	return k, nil

}

func writeWayQts(wayQts quadtreeStore,
	k int,
	res chan elements.ExtendedBlock) ( /*qtMap,*/ int, error) {

	st := time.Now()
	kk := 0
	//iter over way qts in blocks of 8000
	for bl := range wayQts.ObjsIter(1, 8000) {
		res <- elements.MakeExtendedBlock(k, bl, quadtree.Null, 0, 0, nil)
		k++
		kk++
	}
	log.Printf("wrote %d way blocks in %8.1fs\n", kk, time.Since(st).Seconds())

	return k, nil

}

func writeRelQts(
	rls quadtreeStore,
	rels elements.Block,
	k int,
	res chan elements.ExtendedBlock) (int, error) {

	rr := make([]nodeWay, 0, 10000)

	for i := 0; i < rels.Len(); i++ {
		e := elements.UnpackElement(rels.Element(i).Pack())
		ei := e.Id()
		mm := e.(elements.Members)

		if mm.Len() == 0 {
			rls.Set(ei, 0)
		}

		for j := 0; j < mm.Len(); j++ {
			switch mm.MemberType(j) {
			case elements.Relation:

				rr = append(rr, nodeWay{ei, mm.Ref(j)})

			}
		}
	}
	zz := 0
	//for relation members,
	for i := 0; i < 5; i++ {
		for _, p := range rr {
			if i == 0 && p.node == p.way {
				log.Printf("circular rel: %d: curr val %s\n", p.node, rls.Get(p.node))
				if rls.Get(p.node) == quadtree.Null {
					rls.Set(p.node, 0)
				}
			}
			//expand if child relation has a qt value yet
			oq := rls.Get(p.way)
			if oq != quadtree.Null {
				rls.Expand(p.node, oq)
			} else if i == 4 {
				if zz < 10 {
					log.Printf("no rel qt: %d // %d\n", p.node, p.way)
				}
				zz++
			}
		}
	} //repeating 5 times ensures nested qt heirarchies all have a value
	log.Println("have", zz, "missing rel qts")

	//iter over relations in groups of 8000
	for bl := range rls.ObjsIter(elements.Relation, 8000) {
		res <- elements.MakeExtendedBlock(k, bl, quadtree.Null, 0, 0, nil)
		k++
	}

	return k, nil
}

// Calculate a quadtree value for each entity in infn.
func CalcObjectQts(infn string, storeType int, tfs string) (<-chan elements.ExtendedBlock, error) {

	stt := time.Now()
	st := time.Now()

	var nodeWays nwbs
	var rels elements.Block
	numWays := 0
	maxWay := elements.Ref(0)
	var err error

	var abs blocksort.AllocBlockStore
	if tfs == "" {
		tfs = "tempfileslim"
	}
	abs, numWays, maxWay, rels, err = readWayNodes(infn, 4, tfs, 20)
	if err != nil {
		return nil, err
	}
	nodeWays = makeNwbs(infn, abs)

	debug.FreeOSMemory()
	if storeType == 0 && numWays > 40000000 {
		storeType = 1
	}
	log.Printf("%d ways, %d rels: useDense? %d %s\n", numWays, rels.Len(), storeType, utils.MemstatsStr())

	t1 := time.Since(st)
	st = time.Now()

	wayQts, err := calcWayQts(nodeWays, storeType, maxWay)

	if err != nil {
		return nil, err
	}
	t2 := time.Since(st)
	st = time.Now()
	res := make(chan elements.ExtendedBlock)
	go func() {
		rls := newQuadtreeStore(false)

		k, err := findNodeQts(nodeWays, rels, wayQts, rls, res)
		if err != nil {
			panic(err.Error())
		}
		nodeWays.finish()

		t3 := time.Since(st)
		st = time.Now()

		k, err = writeWayQts(wayQts, k, res)
		if err != nil {
			panic(err.Error())
		}
		t4 := time.Since(st)
		st = time.Now()
		k, err = writeRelQts(rls, rels, k, res)

		if err != nil {
			panic(err.Error())
		}
		t5 := time.Since(st)
		st = time.Now()
		tt := time.Since(stt)

		log.Printf("read way nodes: %6.1fs\n", t1.Seconds())
		log.Printf("calc way qts:   %6.1fs\n", t2.Seconds())
		log.Printf("calc node qts:  %6.1fs\n", t3.Seconds())
		log.Printf("write way qts:  %6.1fs\n", t4.Seconds())
		log.Printf("calc rel qts:   %6.1fs\n", t5.Seconds())
		log.Printf("TOTAL:          %6.1fs\n", tt.Seconds())

		close(res)

	}()
	return res, nil
}
