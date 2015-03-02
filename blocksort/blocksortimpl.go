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

//mapAllocBlockStore: implements AllocBlockStore

type mapAllocBlockStore struct {
	blocks  map[int]BlockStore
	makeNew func(int) BlockStore
	tl      int
	sp      int
	finish  func()
}

func newMapAllocBlockStore(makeNew func(int) BlockStore, finish func()) AllocBlockStore {
	return &mapAllocBlockStore{map[int]BlockStore{}, makeNew, 0, 1, finish}
}

func newMapAllocBlockStoreSplit(makeNew func(int) BlockStore, sp int, finish func()) AllocBlockStore {
	return &mapAllocBlockStore{map[int]BlockStore{}, makeNew, 0, sp, finish}
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

	keys := make(sort.IntSlice, 0, len(mabs.blocks))
	for k, _ := range mabs.blocks {
		keys = append(keys, k)
	}
	keys.Sort()

	res := make(chan BlockStoreAllocPair)
	go func() {
		for i, k := range keys {
			res <- BlockStoreAllocPair{k, mabs.blocks[k], i}
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




//keyPackable

type keyPackable interface {
	Key() int
	Pack() []byte
}


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


type keyPendingPair struct {
	key     int
	pending []IdPacked
}


func (kpp *keyPendingPair) Key() int     { return kpp.key }
func (kpp *keyPendingPair) Pack() []byte { return packObjs(kpp.key, kpp.pending) }


//blockStoreWriter

type blockStoreWriter interface {
	WriteBlock(keyPackable)
	GetBlocks(key int) <-chan []byte

	MakeNew(key int) BlockStore
	Finish()
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
		//case 2: res.id =  utils.UnZigzag(msg.Value)
		//case 3: res.Data = elements.PackedElement(msg.Data)
		case 3:
			res.Data = msg.Data
		}
		pos, msg = utils.ReadPbfTag(packed, pos)
	}
	return res
}

//blockStoreFile: implements BlockStore

type blockStoreFile struct {
	key     int
	writer  blockStoreWriter
	pending []IdPacked
	cl      int
	target  int
	tl      int
}

func (bsf *blockStoreFile) Add(o IdPacked) {
	if bsf.pending == nil {
		bsf.pending = make([]IdPacked, 0, bsf.target/len(o.Data))
	}
	bsf.pending = append(bsf.pending, o)
	bsf.tl++

	bsf.cl += (10 + len(o.Data))
	if bsf.cl > bsf.target {
		bsf.Flush()
	}

}
func (bsf *blockStoreFile) Len() int { return bsf.tl }

func (bsf *blockStoreFile) Flush() {

	bsf.writer.WriteBlock(&keyPendingPair{bsf.key, bsf.pending[:]})
	bsf.pending = nil
	bsf.cl = 0
}

func (bsf *blockStoreFile) All() IdPackedList {

	blcks := bsf.writer.GetBlocks(bsf.key)
	res := make(sliceIdPackedList, bsf.tl)
	cp := 0
	copy(res[cp:], bsf.pending)
	cp += len(bsf.pending)

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



//blockStoreWriterIdx: implements blockStoreWriter

type keyDataPair struct {
	key  int
	data []byte
}

type blockStoreWriterIdx struct {
	fl *os.File

	fllock      sync.WaitGroup
	blockChan   chan keyPackable
	blockClosed bool

	idx    map[int][]int64
	target int
}

func (bsi *blockStoreWriterIdx) WriteBlock(kp keyPackable) {
	bsi.blockChan <- kp
}

func (bsi *blockStoreWriterIdx) GetBlocks(key int) <-chan []byte {
	if !bsi.blockClosed {
		close(bsi.blockChan)
		bsi.blockClosed = true
	}
	bsi.fllock.Wait()

	idx, ok := bsi.idx[key]
	if !ok {
		return nil
	}

	//res := make([][]byte, 0, len(idx))
	out := make(chan []byte)
	go func() {
	
		tl := 0
	
		for bl := range pbffile.ReadPbfFileBlocksMultiPartial(bsi.fl, 4, idx) {
	
			b := bl.BlockData()
			out <- b
			tl += len(b)

		}
	
		close(out)
	}()
	return out
	
}
func (bsi *blockStoreWriterIdx) MakeNew(key int) BlockStore {
	return &blockStoreFile{key, bsi, nil, 0, bsi.target, 0}
}

func (bsi *blockStoreWriterIdx) Finish() {
	//fmt.Println("call bsi.Finish()")
	bsi.fl.Close()
	err := os.Remove(bsi.fl.Name())
	if err != nil {
		fmt.Println("os.Remove", bsi.fl.Name(), "??", err.Error())
	}
}

//blockStoreWriterSplit: implements blockStoreWriter


type blockStoreWriterSplit struct {
	writers map[int]blockStoreWriter
	splitat int
	lm      int
}

func (bsws *blockStoreWriterSplit) WriteBlock(kp keyPackable) {
	wk := kp.Key() / bsws.splitat
	ww, ok := bsws.writers[wk]
	if !ok {
		panic("wtf@394")

	}

	ww.WriteBlock(kp)
}

func (bsws *blockStoreWriterSplit) GetBlocks(key int) <-chan []byte {
	wk := key / bsws.splitat
	ww, ok := bsws.writers[wk]
	if !ok {
		return nil
	}
	return ww.GetBlocks(key)
}

func (bsws *blockStoreWriterSplit) MakeNew(key int) BlockStore {
	wk := key / bsws.splitat
	ww, ok := bsws.writers[wk]
	if !ok {
		bsws.writers[wk] = newBlockStoreWriterIdx(true, bsws.lm)
		ww = bsws.writers[wk]
	}
	return ww.MakeNew(key)
}

func (bsws *blockStoreWriterSplit) Finish() {
	for _, v := range bsws.writers {
		v.Finish()
	}
}
func newBlockStoreWriterSplit(sp int, lm int) blockStoreWriter {
	return &blockStoreWriterSplit{map[int]blockStoreWriter{}, sp, lm}
}

func newBlockStoreWriterIdx(split bool, lm int) blockStoreWriter {
	bsi := blockStoreWriterIdx{}
	var err error
	tempdir := os.Getenv("GOPATH")
	bsi.fl, err = ioutil.TempFile(tempdir, "osmquadtree.blocksort.tmp")
	if err != nil {
		panic(err.Error())
	}
	bsi.fllock.Add(1)
	bsi.blockChan = make(chan keyPackable)
	bc2 := make(chan keyDataPair)

	go func() {
		wg := sync.WaitGroup{}
		wg.Add(4)
		for i := 0; i < 4; i++ {
			go func() {
				for kdp := range bsi.blockChan {
					dd := kdp.Pack()
					bb, _ := pbffile.PreparePbfFileBlock([]byte("IdPacked"), dd, true)
					bc2 <- keyDataPair{kdp.Key(), bb}
				}
				wg.Done()
			}()

		}
		wg.Wait()
		close(bc2)
	}()

	go func() {
		for kdp := range bc2 {
			fp, _ := bsi.fl.Seek(0, 2)
			pbffile.WriteFileBlock(bsi.fl, kdp.data)
			bsi.idx[kdp.key] = append(bsi.idx[kdp.key], fp)
		}

		bsi.fl.Sync()
		bsi.fllock.Done()
	}()

	bsi.idx = map[int][]int64{}
	bsi.blockClosed = false

	bsi.target = lm
	return &bsi
}



//groupAllocBlockStore: implements AllocBlockStore

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

	interim := make(chan IdPackedList)

	go func() {
		for bl := range gabs.BlockStore.Iter() {
			//fmt.Println("read",bl.alloc,bl.block.Len())

			bla := bl.block.All()

			//fmt.Println("read",bl.alloc,bla.Len())
			interim <- bla
			//bla=nil

		}
		close(interim)
	}()

	res := make(chan BlockStoreAllocPair)
	go func() {
		ii := 0
		for bla := range interim {

			tabs := newMapAllocBlockStore(makeNewSliceBlockStore, nil)
			for i := 0; i < bla.Len(); i++ {
				o := bla.At(i)
				tabs.Add(o)
			}
			tabs.Flush()

			//fmt.Println("have",tabs.TotalLen(),"in ",tabs.NumBlocks())

			for t := range tabs.Iter() {
				//fmt.Println(t.alloc, t.block.Len())

				res <- BlockStoreAllocPair{t.alloc, t.block, ii}
				ii++
			}

			tabs.Finish()
			//bla=nil
			//tabs=nil

		}
		close(res)
	}()
	return res
}
