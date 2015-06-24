// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

/*
Package blocksort groups unsorted Elements into blocks.
This can be in-memory, or written to temporary files.
*/
package blocksort

import (
	"github.com/jharris2268/osmquadtree/elements"

	"log"
	"sync"
)

type IdPacked struct {
	Key  int
	Data []byte
}

/*A collection of IdPacked structs, perhaps as a slice*/
type IdPackedList interface {
	Len() int
	At(i int) IdPacked
}

/*A BlockStore accepts many calls to Add, storing blobs of data, perhaps
in a temporary file, until Flush is called. The stored data is then retreived
by calling All*/
type BlockStore interface {
	Add(IdPacked)
	Len() int
	Flush()

	All() IdPackedList
}

type BlockStoreAllocPair struct {
	Alloc int
	Block BlockStore
	Idx   int
}

/*An AllocBlockStore accepts many calls to Add, storing blobs of data,
perhaps into one or more temporary files, until Flush is called. The
stored data is then retrieved by calling Iter(), which returns a channel
of BlockStoreAllocPair. The BlockStore contains all the blobs with the
given key Alloc. The channel is sorted by Alloc. Iter can be called more
than once. When finished, call Finish to remove any temporary files created.*/
type AllocBlockStore interface {
	Add(IdPacked)
	NumBlocks() int
	TotalLen() int
	Flush()

	Iter() <-chan BlockStoreAllocPair
	Finish()
}

/*MakeAllocBlockStore creates a new AllocBlockStore. ty must be one of:
 * "block": a simple map of slices of blobs
 * "tempfile": data is written to a temporary file (buffered into blocks of 64kb)
 * "tempfilesplit": group allocs by groups of 100, (buffered into blocks of 1mb),
        and split into several temporary files, each with 500 of these larger
        groups.
 * "tempfileslim": group allocs by groups of 500, (buffered into blocks of 64kb),
        with each written to its own temporary file.

tempfilesplit is the best option for spliting a planet file into blocks,
tempfileslim is the best option for sorting back to by element id order.
Temporary files are stored in the current directory, prefixed by
osmquadtree.blocksort. When spliting/sorting a full planet file (~28gb),
approximately 40gb of disk space will be used.*/
func MakeAllocBlockStore(ty string) AllocBlockStore {

	switch ty {
	case "block":
		return newMapAllocBlockStore(makeNewSliceBlockStore, nil)
	case "tempfile", "tempfilesort":
		bsw := newBlockStoreWriterIdx(false, 64*1024, ty == "tempfilesort")
		//return newMapAllocBlockStore(bsw.MakeNew, bsw.Finish)
		return newMapAllocBlockStoreSplit(bsw.MakeNew, 1, bsw.Finish)
	case "tempfilesplit", "tempfilesplitsort":
		bsw := newBlockStoreWriterSplit(100, 1*1024*1024, ty == "tempfilesplitsort")
		abs := newMapAllocBlockStoreSplit(bsw.MakeNew, 500, bsw.Finish)
		return abs
		//return &groupAllocBlockStore{abs}
	case "tempfileslim", "tempfileslimsort":
		bsw := newBlockStoreWriterSplit(500, 64*1024, ty == "tempfileslimsort")
		abs := newMapAllocBlockStoreSplit(bsw.MakeNew, 1, bsw.Finish)
		return abs
	}
	panic("incorrect ty " + ty)
	return nil
}

/*AddData calls addFunc on each block in the input channels, and adds the
the resultant blobs to the AllocBlockStore abs*/
func AddData(
	abs AllocBlockStore,
	inChans []chan elements.ExtendedBlock,
	addFunc func(elements.ExtendedBlock, chan IdPacked) error) error {

	res := make(chan IdPacked)
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(len(inChans))
		for _, c := range inChans {
			go func(c chan elements.ExtendedBlock) {
				for b := range c {
					addFunc(b, res)
				}
				wg.Done()
			}(c)
		}
		wg.Wait()
		close(res)
	}()

	for oo := range res {
		abs.Add(oo)
	}

	abs.Flush()

	//debug.FreeOSMemory()

	return nil
}

/*ReadData calls abs.Iter, spliting the data into nc parallel channels,
and calling outputFunc(channelidx int, data BlockStoreAllocPair) on each blob*/
func ReadData(abs AllocBlockStore, nc int, outputFunc func(int, BlockStoreAllocPair) error) error {
	itr := abs.Iter()
	wg := sync.WaitGroup{}
	wg.Add(nc)

	zz := make([]chan BlockStoreAllocPair, nc)
	for i, _ := range zz {
		zz[i] = make(chan BlockStoreAllocPair, 5)
	}
	go func() {
		for bl := range itr {
			zz[bl.Idx%nc] <- bl
		}
		for _, z := range zz {
			close(z)
		}
	}()

	for i := 0; i < nc; i++ {
		go func(i int) {
			for bl := range zz[i] {
				err := outputFunc(i, bl)
				if err != nil {
					panic(err.Error())
				}
			}

			wg.Done()

		}(i)
	}
	wg.Wait()
	return nil
}

/*SortByTile calls AddData, ReadData and abs.Finish()*/
func SortByTile(
	inChans []chan elements.ExtendedBlock,
	addFunc func(elements.ExtendedBlock, chan IdPacked) error,
	nc int,
	outputFunc func(int, BlockStoreAllocPair) error,
	abs AllocBlockStore) error {

	AddData(abs, inChans, addFunc)
	err := ReadData(abs, nc, outputFunc)
	abs.Finish()
	return err
}

type Allocater func(elements.Element) int

func makeByElementId(ipl IdPackedList) elements.ByElementId {
	ans := make(elements.ByElementId, 0, ipl.Len())

	for i := 0; i < ipl.Len(); i++ {
		ip := ipl.At(i)
		if ip.Data == nil {
			println("??? null obj")
		}
		ans = append(ans, elements.UnpackElement(ip.Data))
	}
	ans.Sort()
	return ans
}

func makeIdPacked(alloc Allocater, o elements.Element) IdPacked {
	op := o.Pack()
	if op == nil {
		println("???", o, o.String(), "=>", string(op))
	}
	return IdPacked{alloc(o), op}
}

func addToPackedPairBlock(bl elements.ExtendedBlock, alloc Allocater, res chan IdPacked) error {
	if bl == nil || bl.Len() == 0 {
		return nil
	}
	for i := 0; i < bl.Len(); i++ {
		o := bl.Element(i)
		res <- makeIdPacked(alloc, o)
	}
	return nil
}

/*SortElementsByAlloc creates an AllocBlockStore of given absType, and
then uses SortByTile to rearrange the input data inChans into groups
given by the Allocater [func(element.Element) int)] function alloc
(eg. by by using a calcqts.QtTree to find the group for an element's
Quadtree). The sorted data blocks are created using
makeBlock(idx int, alloc int, data elements.Block) (this can be used to
add the block Quadtree value for a given alloc) and written to nc parallel
output channels. If an absType of "inmem" is given, SortInMem is called
instead.*/
func SortElementsByAlloc(
	inChans []chan elements.ExtendedBlock,
	alloc Allocater,
	nc int,
	makeBlock func(int, int, elements.Block) (elements.ExtendedBlock, error),
	absType string) ([]chan elements.ExtendedBlock, error) {

	if absType == "inmem" {
		return SortInMem(inChans, alloc, nc, makeBlock)
	}

	abs := MakeAllocBlockStore(absType)

	addFunc := func(bl elements.ExtendedBlock, res chan IdPacked) error {
		return addToPackedPairBlock(bl, alloc, res)
	}

	res := make([]chan elements.ExtendedBlock, nc)
	for i, _ := range res {
		res[i] = make(chan elements.ExtendedBlock)
	}

	outputFunc := func(i int, blob BlockStoreAllocPair) error {
		pp := makeByElementId(blob.Block.All())
		bl, err := makeBlock(blob.Idx, blob.Alloc, pp)
		if err != nil {
			return err
		}
		res[i] <- bl
		return nil
	}

	go func() {
		err := SortByTile(inChans, addFunc, nc, outputFunc, abs)
		if err != nil {
			log.Println("SortByTile error:", err.Error())
		}
		for _, r := range res {
			//log.Println("close chan",i,"/",len(res))
			close(r)
		}
	}()
	return res, nil
}
