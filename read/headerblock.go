// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package read

import (
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"

	"fmt"
	"strings"
)

func readBbox(indata []byte) (quadtree.Bbox, error) {
	ans := quadtree.Bbox{}
	a, msg := utils.ReadPbfTag(indata, 0)
	for msg.Tag > 0 {
		switch msg.Tag {
		case 1:
			ans.Minx = utils.UnZigzag(msg.Value) / 100
		case 2:
			ans.Miny = utils.UnZigzag(msg.Value) / 100
		case 3:
			ans.Maxx = utils.UnZigzag(msg.Value) / 100
		case 4:
			ans.Maxy = utils.UnZigzag(msg.Value) / 100
		}
		a, msg = utils.ReadPbfTag(indata, a)
	}
	return ans, nil
}
func ReadPbfBbox(indata []byte) (quadtree.Bbox, error) {
	return readBbox(indata)
}

type blockIdx struct {
	quadtree          quadtree.Quadtree
	filepos, blockLen int64
	isChange          bool
}
type blockIdxSlice []blockIdx

// BlockIdx contains the file positions for the pbf data blocks within
// a file. This can be used read only parts of a pbf file: see
// readfile.ReadExtendedBlockMultiSortedPartial and
// readfile.ReadExtendedBlockMultiSortedQts
type BlockIdx interface {
	Len() int
	Filepos(int) int64
	BlockLen(int) int64
	Quadtree(int) quadtree.Quadtree
	IsChange(int) bool
}

func (bi blockIdxSlice) Len() int                         { return len(bi) }
func (bi blockIdxSlice) Filepos(i int) int64              { return bi[i].filepos }
func (bi blockIdxSlice) BlockLen(i int) int64             { return bi[i].blockLen }
func (bi blockIdxSlice) Quadtree(i int) quadtree.Quadtree { return bi[i].quadtree }
func (bi blockIdxSlice) IsChange(i int) bool              { return bi[i].isChange }

func (bi blockIdx) String() string {
	cc := " "
	if bi.isChange {
		cc = "c"
	}
	return fmt.Sprintf("%-18s%s @ %-8d [%-7d]",
		bi.quadtree, cc, bi.filepos, bi.blockLen)
}

// HeaderBlock contains the data found in pbf file HeaderBlock
type HeaderBlock struct {
	Bbox      *quadtree.Bbox
	Features  map[string][]string //The required_features, optional_features, and writing_program fields
	Index     BlockIdx
	Timestamp elements.Timestamp
}

func (hi *HeaderBlock) String() string {
	bs := "[]"
	if hi.Bbox != nil {
		bs = hi.Bbox.String()
	}
	as := ""
	if hi.Index != nil {
		as = fmt.Sprintf("[%-7d idx]", hi.Index.Len())
	}
	fs := ""
	for k, v := range hi.Features {
		if len(fs) > 0 {
			fs += ", "
		}
		fs += k + "="
		if len(v) == 1 {
			fs += v[0]
		} else {
			fs += fmt.Sprintf("[%s]", strings.Join(v, ","))
		}
	}

	ans := fmt.Sprintf("HeaderInfo: %s %s %s", bs, as, fs)
	return ans
}

func readBlockIdx(indata []byte) (*blockIdx, error) {
	ans := &blockIdx{}
	a, msg := utils.ReadPbfTag(indata, 0)
	for msg.Tag > 0 {
		var err error
		switch msg.Tag {

		case 1:
			ans.quadtree, err = readQuadtree(msg.Data)
		case 2:
			ans.isChange = (msg.Value == 1)
		case 3:
			ans.blockLen = utils.UnZigzag(msg.Value)
		}
		if err != nil {
			return nil, err
		}
		a, msg = utils.ReadPbfTag(indata, a)
	}
	return ans, nil
}

// ReadHeaderBlock returns a HeaderBlock from pbf message indata. filePos
// is the file position after the HeaderBlock has been read: this allows
// the BlockIdx to calculate file positions from the stored block lengths
func ReadHeaderBlock(indata []byte, filePos int64) (*HeaderBlock, error) {
	a, msg := utils.ReadPbfTag(indata, 0)

	ans := &HeaderBlock{}
	var idx blockIdxSlice
	for msg.Tag > 0 {
		switch msg.Tag {
		case 4, 5, 16, 17:
			if ans.Features == nil {
				// set up features map
				ans.Features = map[string][]string{}
			}

		}

		switch msg.Tag {
		case 1:
			bb, err := readBbox(msg.Data)
			if err != nil {
				return nil, err
			}
			ans.Bbox = &bb
		case 4:

			ans.Features["required"] = append(ans.Features["required"], string(msg.Data))
		case 5:

			ans.Features["optional"] = append(ans.Features["optional"], string(msg.Data))
		case 16:
			ans.Features["writingprogram"] = append(ans.Features["writingprogram"], string(msg.Data))
		case 17:
			ans.Features["source"] = append(ans.Features["source"], string(msg.Data))
		case 22:
			// index of file blocks
			if idx == nil || cap(idx) == 0 {
				idx = make(blockIdxSlice, 0, 350000)
			}
			ii, err := readBlockIdx(msg.Data)
			if err != nil {
				return nil, err
			}
			idx = append(idx, *ii)
		case 32:
			ans.Timestamp = elements.Timestamp(msg.Value)
		}
		a, msg = utils.ReadPbfTag(indata, a)
	}

	if len(idx) != 0 {
		// calculate block positions: cumulative sum of block lens (plus length of header block)
		for i, a := range idx {
			idx[i].filepos = filePos
			filePos += a.blockLen
		}
	}
	ans.Index = idx[:len(idx)]

	return ans, nil
}
