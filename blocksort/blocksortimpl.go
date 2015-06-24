// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package blocksort

import (
	"github.com/jharris2268/osmquadtree/pbffile"
	"github.com/jharris2268/osmquadtree/utils"

	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
    
    "time"
    "runtime/debug"
)

//sliceBlockStore: implments BlockStore

type sliceBlockStore struct {
	p []IdPacked
}

func (sbs *sliceBlockStore) Add(o IdPacked) {
	sbs.p = append(sbs.p, o)
}
func (sbs *sliceBlockStore) Flush()   {}
func (sbs *sliceBlockStore) Len() int { return len(sbs.p) }

func (sbs *sliceBlockStore) All() IdPackedList {
	return sliceIdPackedList(sbs.p)
}

type sliceIdPackedList []IdPacked

func (sbs sliceIdPackedList) Len() int          { return len(sbs) }
func (sbs sliceIdPackedList) At(i int) IdPacked { return sbs[i] }

func makeNewSliceBlockStore(int) BlockStore {
	return &sliceBlockStore{make([]IdPacked, 0, 10000)}
}




//blockStoreWriter

type blockStoreWriter interface {
	WriteBlock(keyPackable)
	GetBlocks(key int) <-chan []byte

	MakeNew(key int) BlockStore
	Finish()
}
/*
type blockStoreWriterSortable interface {
    Sort()
}*/


//mapAllocBlockStore: implements AllocBlockStore

type mapAllocBlockStore struct {
	blocks  map[int]BlockStore
	makeNew func(int) BlockStore //used to create new entries for blocks
    //bsw     blockStoreWriter // stores 
    
	tl      int  //number of objects added
	sp      int  //group allocs together
	finish  func() //to clean up BlockStore entries
    issplit bool
    
}

func newMapAllocBlockStore(makeNew func(int) BlockStore, finish func()) AllocBlockStore {
	return &mapAllocBlockStore{map[int]BlockStore{}, makeNew, 0, 1, finish, false}
}

func newMapAllocBlockStoreSplit(makeNew func(int) BlockStore, sp int, finish func()) AllocBlockStore {
	return &mapAllocBlockStore{map[int]BlockStore{}, makeNew, 0, sp, finish, true}
}

func (mabs *mapAllocBlockStore) Add(obj IdPacked) {
	alloc := obj.Key / mabs.sp
	blck, ok := mabs.blocks[alloc]
	if !ok {
		mabs.blocks[alloc] = mabs.makeNew(alloc)
		blck = mabs.blocks[alloc]
	}

	blck.Add(obj)
	mabs.tl++
}

func (mabs *mapAllocBlockStore) NumBlocks() int {
	return len(mabs.blocks)
}

func (mabs *mapAllocBlockStore) TotalLen() int {
	return mabs.tl
}

func (mabs *mapAllocBlockStore) Flush() {
	for _, b := range mabs.blocks {
		b.Flush()
	}
}
func (mabs *mapAllocBlockStore) Iter() <-chan BlockStoreAllocPair {

    //sort map keys in ascending order
	keys := make(sort.IntSlice, 0, len(mabs.blocks))
	for k, _ := range mabs.blocks {
		keys = append(keys, k)
	}
	keys.Sort()

    
	res := make(chan BlockStoreAllocPair)
    
    
	go func() {
        //add each block in turn to chan
		for i, k := range keys {
			res <- BlockStoreAllocPair{k, mabs.blocks[k], i}
		}
		close(res)
	}()
    if mabs.issplit {
        
        return sortMapAllocBlockStoreResult(res)
    }
    
	return res
}

func sortMapAllocBlockStoreResult(inc <-chan BlockStoreAllocPair) <-chan BlockStoreAllocPair {
    interim := make(chan IdPackedList)

	go func() {
		for bl := range inc /*gabs.BlockStore.Iter()*/ {
			bla := bl.Block.All()
			interim <- bla
		}
		close(interim)
	}()

	res := make(chan BlockStoreAllocPair)
	go func() {
		ii := 0
		for bla := range interim {
            // for each group of blobs, create new mapAllocBlockStore
			tabs := newMapAllocBlockStore(makeNewSliceBlockStore, nil)
			for i := 0; i < bla.Len(); i++ {
				o := bla.At(i) // add blocks
				tabs.Add(o)
			}
			tabs.Flush()
            // read in order (with new idx value)
			for t := range tabs.Iter() {
                res <- BlockStoreAllocPair{t.Alloc, t.Block, ii}
				ii++
			}

			tabs.Finish()
		}
		close(res)
	}()
	return res
}

func (mabs *mapAllocBlockStore) Finish() {
	mabs.blocks = nil
	if mabs.finish != nil {
		//fmt.Println("call mabs.finish")
		mabs.finish()
	}
}





// (de)serialize IdPackeds

func packIdPacked(o IdPacked) []byte {
	r := make(utils.PbfMsgSlice, 2)
	r[0] = utils.PbfMsg{1, nil, utils.Zigzag(int64(o.Key))}
	//r[1] = utils.PbfMsg{2,nil,utils.Zigzag(o.id)}
	r[1] = utils.PbfMsg{3, o.Data, 0}
	return r.Pack()
}

func packObjs(key int, objs []IdPacked) []byte {
	outp := make(utils.PbfMsgSlice, len(objs)+1)

	outp[0] = utils.PbfMsg{1, nil, utils.Zigzag(int64(key))}
	for i, o := range objs {
		outp[i+1] = utils.PbfMsg{2, packIdPacked(o), 0}

	}

	return outp.Pack()
}

func unpackObjs(packed []byte) (int, []IdPacked) {
	pos, msg := utils.ReadPbfTag(packed, 0)
	key := 0
	res := make([]IdPacked, 0, 8000)

	for msg.Tag > 0 {
		switch msg.Tag {
		case 1:
			key = int(utils.UnZigzag(msg.Value))
		case 2:
			res = append(res, unpackIdPacked(msg.Data))
		}
		pos, msg = utils.ReadPbfTag(packed, pos)
	}

	return key, res
}

func unpackIdPacked(packed []byte) IdPacked {
	res := IdPacked{}
	pos, msg := utils.ReadPbfTag(packed, 0)
	for msg.Tag > 0 {
		switch msg.Tag {
		case 1:
			res.Key = int(utils.UnZigzag(msg.Value))
		case 3:
			res.Data = msg.Data
		}
		pos, msg = utils.ReadPbfTag(packed, pos)
	}
	return res
}



//keyPackable

type keyPackable interface {
	Key() int
	Pack() []byte
}

    

type keyPendingPair struct {
	key     int
	pending []IdPacked
}


func (kpp *keyPendingPair) Key() int     { return kpp.key }
func (kpp *keyPendingPair) Pack() []byte { return packObjs(kpp.key, kpp.pending) }







//blockStoreFile: implements BlockStore

type blockStoreFile struct {
	key     int
	writer  blockStoreWriter
	pending []IdPacked // blocks waiting to be written
	cl      int // size of pending blocks
	target  int // size of block to write
	tl      int // number to objects stored
}

func (bsf *blockStoreFile) Add(o IdPacked) {
	if bsf.pending == nil {
		bsf.pending = make([]IdPacked, 0, bsf.target/len(o.Data))
	}
    
    // add block pending slice    
	bsf.pending = append(bsf.pending, o)
	bsf.tl++
	bsf.cl += (10 + len(o.Data)) // lenght of data plus estimate of block overhead
	
    if bsf.cl > bsf.target {
        // write to blockStoreWriter
		bsf.Flush()
	}

}
func (bsf *blockStoreFile) Len() int { return bsf.tl }

func (bsf *blockStoreFile) Flush() {
    // write pending blocks to file
	bsf.writer.WriteBlock(&keyPendingPair{bsf.key, bsf.pending[:]})
	bsf.pending = nil
	bsf.cl = 0
}

func (bsf *blockStoreFile) All() IdPackedList {
    
    /*
    ss,ok := bsf.writer.(blockStoreWriterSortable)
    if ok {
        ss.Sort()
    }*/
    
    
	blcks := bsf.writer.GetBlocks(bsf.key)
    
    // create sliceIdPackedList to store all objects
	res := make(sliceIdPackedList, bsf.tl)
	cp := 0
    
    // if we have any objects left in pending, return these first
    
	copy(res[cp:], bsf.pending)
	cp += len(bsf.pending)

    // fetch stored objects
	for b := range blcks {
		kk, oo := unpackObjs(b)
		if kk != bsf.key {
			panic(fmt.Sprintf("wtf: wrong key: expected %d, got %d", bsf.key, kk))
		}
		copy(res[cp:], oo)
		cp += len(oo)
	}

	return res
}



//blockStoreWriterIdx: a blockStoreWriter which writes objects to a temp file

type blockStoreWriterIdx struct {
	fl *os.File

	fllock      sync.WaitGroup
	blockChan   chan keyPackable
	blockClosed bool

	idx    map[int][]int64 // map of key to slice of file locations
	target int
    
    readlock      sync.Mutex
}

func newBlockStoreWriterIdx(split bool, lm int, tosort bool) blockStoreWriter {
	bsi := blockStoreWriterIdx{}
	var err error
	tempdir := os.Getenv("GOPATH")
	bsi.fl, err = ioutil.TempFile(tempdir, "osmquadtree.blocksort.tmp")
	if err != nil {
		panic(err.Error())
	}
	bsi.fllock.Add(1) // block reading (or close) untill we have finished writing
	
    
    bsi.blockChan = make(chan keyPackable) 
	
    interim := make(chan keyDataPair) // serialzed blobs to write to file
    
	go func() {
        // create several preparers        
		wg := sync.WaitGroup{}
		wg.Add(4)
		for i := 0; i < 4; i++ {
			go func() {
                // serialize (and compress) blobs from blockChan; write to bc2
				for kdp := range bsi.blockChan {
					dd := kdp.Pack()
					bb, _ := pbffile.PreparePbfFileBlock([]byte("IdPacked"), dd, true)
					interim <- keyDataPair{kdp.Key(), bb}
				}
				wg.Done()
			}()

		}
		wg.Wait() // only close interim when all prepare procs finished
		close(interim)
	}()

    // but single process to write to file
	go func() {
		for kdp := range interim {
			//fp, _ := bsi.fl.Seek(0, 2) 
			fp, _ := pbffile.WriteFileBlockAtEnd(bsi.fl, kdp.data) // file location
			bsi.idx[kdp.key] = append(bsi.idx[kdp.key], fp) // add to slice for given key
		}

		bsi.fl.Sync()
        
        if tosort {
            bsi.Sort()
        }
        
        
		bsi.fllock.Done() // finished writing, can now allow reading / closing
	}()

	bsi.idx = map[int][]int64{}
	bsi.blockClosed = false

	bsi.target = lm
    
	return &bsi
}


func (bsi *blockStoreWriterIdx) WriteBlock(kp keyPackable) {
    // called by blockStoreFile.Flush
	bsi.blockChan <- kp 
}


type keyDataPair struct {
	key  int
	data []byte
}

type keyDataPairSlice []keyDataPair
func (k keyDataPairSlice) Len() int { return len(k) }
func (k keyDataPairSlice) Swap(i,j int) { k[i],k[j] = k[j],k[i] }
func (k keyDataPairSlice) Less(i,j int) bool { return k[i].key < k[j].key }

func (bsi *blockStoreWriterIdx) GetBlocks(key int) <-chan []byte {
    // called by blockStoreFile.All
    
    bsi.readlock.Lock()
    
	if !bsi.blockClosed {
        
        // we won't be writing any more data
		close(bsi.blockChan)
		bsi.blockClosed = true
	}
    bsi.readlock.Unlock()
	bsi.fllock.Wait() // wait untill all data has been writen to disk
    
    
	idx, ok := bsi.idx[key] // fetch locations for given key
    
    //fmt.Println("key=", key, "ok=",ok, "len(idx)=",len(idx))
    
	

	out := make(chan []byte)
	go func() {
	
		tl := 0
        if ok {
            for bl := range pbffile.ReadPbfFileBlocksMultiPartial(bsi.fl, 4, idx) {
                b := bl.BlockData()
                out <- b
                tl += len(b)
            }
        }
	
		close(out)
	}()
	return out
	
}
func (bsi *blockStoreWriterIdx) MakeNew(key int) BlockStore {
	return &blockStoreFile{key, bsi, nil, 0, bsi.target, 0}
}

func (bsi *blockStoreWriterIdx) Finish() {
	// delete temporary file
    bsi.fllock.Wait()
    
	bsi.fl.Close()
	err := os.Remove(bsi.fl.Name())
	if err != nil {
		fmt.Println("os.Remove", bsi.fl.Name(), "??", err.Error())
	}
}


type kpt struct {
    key int
    pos int64
    ln  int64
}
type kpts []kpt
func (k kpts) Len() int { return len(k) }
func (k kpts) Swap(i,j int) { k[i],k[j]=k[j],k[i] }
func (k kpts) Less(i,j int) bool { return k[i].pos < k[j].pos }



func sortBlockSortWriterIdx(orig *blockStoreWriterIdx, newfl *os.File, newidx map[int][]int64) int64 {
    /*
    if !orig.blockClosed {
        // we won't be writing any more data
		close(orig.blockChan)
		orig.blockClosed = true
	}
	orig.fllock.Wait() // wait untill all data has been writen to disk
    */
    tln:=int64(0)
    nb := 0
    
    for _,vv:=range orig.idx {
        nb+=len(vv)
    }
    kk := make(kpts, 0, nb)
    for k,vv:=range orig.idx {
        for _,v:=range vv {
            kk=append(kk,kpt{k,v,-1})
        }
    }
    sort.Sort(kk)
    for i,k:=range kk {
        d:=int64(0)
        if i==(len(kk)-1) {
            d,_ = orig.fl.Seek(0,2)
        } else {
            d = kk[i+1].pos
        }
        kk[i].ln = d-k.pos
        tln += kk[i].ln
        //println(i,kk[i].key, kk[i].pos, kk[i].ln)
    }
    
    
    allBlocks := make(keyDataPairSlice, 0, len(kk))
    for _,k:=range kk {
        
        //v,_:=pbffile.ReadFileBlockAt(newfl, k.l)
        orig.fl.Seek(k.pos,0)
        
        data,_ := utils.ReadBlock(orig.fl, uint64(k.ln))
        
        allBlocks=append(allBlocks, keyDataPair{k.key,data})
    }
    sort.Sort(allBlocks)
        
    for _,kdp := range allBlocks {
        fp, _ := pbffile.WriteFileBlockAtEnd(newfl, kdp.data) // file location
		newidx[kdp.key] = append(newidx[kdp.key], fp) // add to slice for given key
    }
    newfl.Sync()
    return tln
}

func (bsi *blockStoreWriterIdx) Sort() {
    st:=time.Now()
    fmt.Printf("CALLED blockStoreWriterIdx::Sort()")
    
	tempdir := os.Getenv("GOPATH")
	fl, err := ioutil.TempFile(tempdir, "osmquadtree.blocksort.tmp")
	if err != nil {
		panic(err.Error())
	}
    idx := map[int][]int64{}
    ln := sortBlockSortWriterIdx(bsi, fl,idx)
    
    bsi.fl.Close()
	os.Remove(bsi.fl.Name())
    
    bsi.fl,bsi.idx=fl,idx
    debug.FreeOSMemory()
    fmt.Printf(" wrote %8.1fmb in %8.1fs\n", float64(ln)/1024./1024, time.Since(st).Seconds())
    
}

//blockStoreWriterSplit: writes to several temporary files

type blockStoreWriterSplit struct {
	writers map[int]blockStoreWriter 
	splitat int 
	lm      int //newBlockStoreWriterIdx block size
    tosort  bool
    
    
}

func (bsws *blockStoreWriterSplit) WriteBlock(kp keyPackable) {
	wk := kp.Key() / bsws.splitat
    if bsws.splitat==0{
        bsws.writers[0].WriteBlock(kp)
        return
    }
    
	ww, ok := bsws.writers[wk]
	if !ok {
		panic("trying to write to uninited blockStoreWriter")
	}

	ww.WriteBlock(kp)
}

func (bsws *blockStoreWriterSplit) GetBlocks(key int) <-chan []byte {
    if bsws.splitat==0{
        return bsws.writers[0].GetBlocks(key)
    }
    
	wk := key / bsws.splitat
	ww, ok := bsws.writers[wk]
	if !ok {
		return nil
	}
	return ww.GetBlocks(key)
}

func (bsws *blockStoreWriterSplit) MakeNew(key int) BlockStore {
    if bsws.splitat==0{
        if _,ok:= bsws.writers[0]; !ok {
            bsws.writers[0] = newBlockStoreWriterIdx(true, bsws.lm, bsws.tosort)
        }
        return bsws.writers[0].MakeNew(key)
    }
    
	wk := key / bsws.splitat
	ww, ok := bsws.writers[wk]
	if !ok {
		bsws.writers[wk] = newBlockStoreWriterIdx(true, bsws.lm, bsws.tosort)
		ww = bsws.writers[wk]
	}
	return ww.MakeNew(key)
}

func (bsws *blockStoreWriterSplit) Finish() {
	for _, v := range bsws.writers {
		v.Finish()
	}
}
func newBlockStoreWriterSplit(sp int, lm int, tosort bool) blockStoreWriter {
	return &blockStoreWriterSplit{map[int]blockStoreWriter{}, sp, lm, tosort}
}
/*
func (bsws *blockStoreWriterSplit) Sort() {
    if bsws.splitat==0 { return; }
    
    newbsi := blockStoreWriterIdx{}
    var err error
	tempdir := os.Getenv("GOPATH")
	newbsi.fl, err = ioutil.TempFile(tempdir, "osmquadtree.blocksort.tmp")
	if err != nil {
		panic(err.Error())
	}
    
    kk :=make(sort.IntSlice, 0,len(bsws.writers))
    for k,_:=range bsws.writers {
        kk=append(kk,k)
    }
    kk.Sort()
    
    for _,k:=range kk {
        ww,ok:=bsws.writers[k].(*blockStoreWriterIdx)
        if !ok { panic("??") }
        sortBlockSortWriterIdx(ww, newbsi.fl,newbsi.idx)
        bsws.writers[k].Finish()
    }
    
    bsws.writers = map[int]blockStoreWriter{0:&newbsi}
    bsws.splitat=0    
    
    //return newbsi
}
*/

/*
//groupAllocBlockStore: implements AllocBlockStore: sorts data retrieved
// from a mapAllocBlockStoreSplit 

type groupAllocBlockStore struct {
	BlockStore AllocBlockStore
}

func (gabs *groupAllocBlockStore) Add(obj IdPacked) {
	gabs.BlockStore.Add(obj)
}
func (gabs *groupAllocBlockStore) NumBlocks() int { return gabs.BlockStore.NumBlocks() }
func (gabs *groupAllocBlockStore) TotalLen() int  { return gabs.BlockStore.TotalLen() }
func (gabs *groupAllocBlockStore) Flush()         { gabs.BlockStore.Flush() }
func (gabs *groupAllocBlockStore) Finish()        { gabs.BlockStore.Finish() }

func (gabs *groupAllocBlockStore) Iter() <-chan BlockStoreAllocPair {
    return sortMapAllocBlockStoreResult(gabs.BlockStore.Iter())
}
*/



