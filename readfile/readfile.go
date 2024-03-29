// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package readfile

import (
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/pbffile"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/read"
	"github.com/jharris2268/osmquadtree/utils"

	"os"
	//"io"
	//"sync"
	"strings"
	//"fmt"
)

// call readfn on each pbffile.FileBlock, writing result to outblocks
// usually called on parallel channels
func readBlocks(
	inblocks <-chan pbffile.FileBlock,
	readfn func(int, []byte, bool) (elements.ExtendedBlock, error),
	ischange bool,
	outblocks chan<- elements.ExtendedBlock) error {

	for bl := range inblocks {

		isc, ok := func(t pbffile.FileBlock) (bool, bool) {
			switch string(bl.BlockType()) {
			case "OSMData":
				return false || ischange, true
			case "OSMChange":
				return true, true
			}
			return false, false
		}(bl)
		if ok {
			dd, err := readfn(bl.Idx(), bl.BlockData(), isc)
			if err != nil {
				return err
			}
			outblocks <- dd
		} else {
			outblocks <- elements.MakeExtendedBlock(bl.Idx(), nil, quadtree.Null, 0, 0, nil)
		}
	}
	return nil
}

// MakeFileBlockChanSingle produces a chan of each fileblock in fn, in order.
// Returns (chan fileblocks, ischange, error)
func MakeFileBlockChanSingle(fn string) (<-chan pbffile.FileBlock, bool, error) {
	fl, err := os.Open(fn)
	if err != nil {
		return nil, false, err
	}
	isc := strings.HasSuffix(fn, "pbfc")
	return pbffile.ReadPbfFileBlocksDefer(fl), isc, nil
}

// MakeFileBlockChanSplit produces nc parallel chans of each fileblock.
// channel i contsists of block i, i+nc, i+2*nc etc.
// Returns (chan fileblocks, ischange, error)
func MakeFileBlockChanSplit(fn string, nc int) ([]<-chan pbffile.FileBlock, bool, error) {
	fl, err := os.Open(fn)
	if err != nil {
		return nil, false, err
	}
	isc := strings.HasSuffix(fn, "pbfc")
	return pbffile.ReadPbfFileBlocksDeferSplit(fl, nc), isc, nil
}

type ReadDataFunc func(int, []byte, bool) (elements.ExtendedBlock, error)

//
func ReadDataSingle(blocks <-chan pbffile.FileBlock, isc bool, readData ReadDataFunc) (<-chan elements.ExtendedBlock, error) {

	res := make(chan elements.ExtendedBlock)

	go func() {
		readBlocks(blocks, readData, isc, res)
		close(res)
	}()
	return res, nil
}

func ReadDataMulti(blocks []<-chan pbffile.FileBlock, isc bool, readData ReadDataFunc) ([]chan elements.ExtendedBlock, error) {
	nc := len(blocks)
	res := make([]chan elements.ExtendedBlock, nc)

	for i, _ := range res {
		res[i] = make(chan elements.ExtendedBlock)
		go func(i int) {
			//log.Println("call",i,"th readBlocks",blocks[i],readData,nc,isc,res[i])
			readBlocks(blocks[i], readData, isc, res[i])
			//log.Println("finished",i)
			close(res[i])
		}(i)
	}

	return res, nil
}

func ReadDataMultiSorted(blocks []<-chan pbffile.FileBlock, isc bool, readData ReadDataFunc) (<-chan elements.ExtendedBlock, error) {
	dd, err := ReadDataMulti(blocks, isc, readData)
	if err != nil {
		return nil, err
	}

	return CollectExtendedBlockChans(dd), nil
}

// CollectExtendedBlockChans combines resp parallel channels to single
// output channel: opposite to SplitExtendedBlockChans
func CollectExtendedBlockChans(resp []chan elements.ExtendedBlock) <-chan elements.ExtendedBlock {
	res := make(chan elements.ExtendedBlock)
	go func() {
		nc := len(resp)
		rem := nc
		i := 0

		for rem > 0 {
			b, ok := <-resp[i%nc]
			if ok {
				res <- b
			} else {
				rem--
			}
			i++
		}
		close(res)
	}()
	return res
}

// SplitExtendedBlockChans splits single input channel to nc parallel
// channels: opposite to CollectExtendedBlockChans
func SplitExtendedBlockChans(inc <-chan elements.ExtendedBlock, nc int) []chan elements.ExtendedBlock {
	res := make([]chan elements.ExtendedBlock, nc)
	for i, _ := range res {
		res[i] = make(chan elements.ExtendedBlock)
	}
	go func() {
		z := 0
		for bl := range inc {
			res[z%nc] <- bl
			z++
		}
		for _, r := range res {
			close(r)
		}
	}()
	return res
}

func ReadSomeElements(nodes bool, ways bool, relations bool) ReadDataFunc {
	return func(idx int, bl []byte, isc bool) (elements.ExtendedBlock, error) {
		return read.ReadObjsData(idx, bl, nodes, ways, relations)
	}
}

func ReadSomeElementsMulti(fn string, nc int, nn, ww, rr bool) ([]chan elements.ExtendedBlock, error) {
	blocks, isc, err := MakeFileBlockChanSplit(fn, nc)
	if err != nil {
		return nil, err
	}

	return ReadDataMulti(blocks, isc, ReadSomeElements(nn, ww, rr))
}

func ReadSomeElementsMultiSorted(fn string, nc int, nn, ww, rr bool) (<-chan elements.ExtendedBlock, error) {
	blocks, isc, err := MakeFileBlockChanSplit(fn, nc)
	if err != nil {
		return nil, err
	}

	return ReadDataMultiSorted(blocks, isc, ReadSomeElements(nn, ww, rr))
}

func ReadExtendedBlock(fn string) (<-chan elements.ExtendedBlock, error) {
	blocks, isc, err := MakeFileBlockChanSingle(fn)
	if err != nil {
		return nil, err
	}

	return ReadDataSingle(blocks, isc, read.ReadExtendedBlock)
}

func ReadExtendedBlockMulti(fn string, nc int) ([]chan elements.ExtendedBlock, error) {
	blocks, isc, err := MakeFileBlockChanSplit(fn, nc)
	if err != nil {
		return nil, err
	}

	return ReadDataMulti(blocks, isc, read.ReadExtendedBlock)
}

func ReadExtendedBlockMultiSorted(fn string, nc int) (<-chan elements.ExtendedBlock, error) {
	blocks, isc, err := MakeFileBlockChanSplit(fn, nc)
	if err != nil {
		return nil, err
	}

	return ReadDataMultiSorted(blocks, isc, read.ReadExtendedBlock)
}

func ReadQtsMulti(fn string, nc int) ([]chan elements.ExtendedBlock, error) {
	blocks, isc, err := MakeFileBlockChanSplit(fn, nc)
	if err != nil {
		return nil, err
	}

	return ReadDataMulti(blocks, isc, read.ReadQts)
}

func SortExtendedBlockChan(inc <-chan elements.ExtendedBlock) <-chan elements.ExtendedBlock {
	idxc := make(chan utils.Idxer)
	go func() {
		for i := range inc {
			idxc <- i
		}
		close(idxc)
	}()
	res := make(chan elements.ExtendedBlock)
	go func() {
		for i := range utils.SortIdxerChan(idxc) {
			res <- i.(elements.ExtendedBlock)
		}
		close(res)
	}()
	return res
}
