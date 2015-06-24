// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package writefile

import (
	"log"

	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/pbffile"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/readfile"
	"github.com/jharris2268/osmquadtree/utils"
	"github.com/jharris2268/osmquadtree/write"

	"io"
	"io/ioutil"
	"os"
	"time"
    "fmt"
)

type idxData struct {
	i int
	d []byte
	q quadtree.Quadtree
}

func (i *idxData) Idx() int { return i.i }

type DataQuadtreer interface {
	Quadtree() quadtree.Quadtree
	Data() []byte
}

func (i *idxData) Quadtree() quadtree.Quadtree { return i.q }
func (i *idxData) Data() []byte                { return i.d }

type IdxItem struct {
	Idx      int
	Quadtree quadtree.Quadtree
	Len      int64
	Isc      bool
}

type blockIdx []IdxItem

func (bi blockIdx) Len() int           { return len(bi) }
func (bi blockIdx) Swap(i, j int)      { bi[j], bi[i] = bi[i], bi[j] }
func (bi blockIdx) Less(i, j int) bool { return bi[i].Quadtree < bi[j].Quadtree }

func (bi blockIdx) Quadtree(i int) quadtree.Quadtree { return bi[i].Quadtree }
func (bi blockIdx) IsChange(i int) bool              { return bi[i].Isc }
func (bi blockIdx) BlockLen(i int) int64             { return bi[i].Len }

func addQtBlock(bl elements.ExtendedBlock, idxoff int) (utils.Idxer, error) {

	a, err := write.WriteExtendedBlock(bl, false, true)
	if err != nil {
		return nil, err
	}
	b, err := pbffile.PreparePbfFileBlock([]byte("OSMData"), a, true)
	if err != nil {
		return nil, err
	}
	return &idxData{bl.Idx() - idxoff, b, quadtree.Null}, nil
}

func addFullBlock(bl elements.ExtendedBlock, idxoff int, isc bool, bh []byte) (utils.Idxer, error) {
	a, err := write.WriteExtendedBlock(bl, isc, true)
	if err != nil {
		return nil, err
	}

	b, err := pbffile.PreparePbfFileBlock(bh, a, true)
	if err != nil {
		return nil, err
	}
	return &idxData{bl.Idx() - idxoff, b, bl.Quadtree()}, nil
}

func addOrigBlock(bl elements.ExtendedBlock, bh []byte) (utils.Idxer, error) {
	a, err := write.WriteExtendedBlock(bl, false, false)
	if err != nil {
		return nil, err
	}

	b, err := pbffile.PreparePbfFileBlock(bh, a, true)
	if err != nil {
		return nil, err
	}
	return &idxData{bl.Idx(), b, quadtree.Null}, err
}

func finishAndHeader(outf io.Writer, tf io.ReadWriter, ii []IdxItem, isc bool) (write.BlockIdxWrite, error) {

	tfs, ok := tf.(interface {
		Sync() error
		Seek(int64, int) (int64, error)
	})
	if ok {

		tfs.Sync()
		tfs.Seek(0, 0)
	} else {
		log.Println("tempfile not a Seeker...")
		tfr, ok := tf.(interface {
			Reset()
		})
		if ok {
			tfr.Reset()
		} else {
			log.Println("tempfile not a Reseter...")
		}
	}

	for i, _ := range ii {
		ii[i].Isc = isc
	}

	header, err := write.WriteHeaderBlock(quadtree.PlanetBbox(), blockIdx(ii))
	if err != nil {
		return nil, err
	}

	dd, err := pbffile.PreparePbfFileBlock([]byte("OSMHeader"), header, true)
	if err != nil {
		return nil, err
	}

	err = pbffile.WriteFileBlock(outf, dd)
	if err != nil {
		return nil, err
	}

	ll, err := io.Copy(outf, tf)
	if err != nil {
		return nil, err
	}

	nm := ""
	tfn, ok := tf.(interface {
		Name() string
	})
	if ok {
		nm = tfn.Name()
	}
	log.Printf("copied %d bytes from %s\n", ll, nm)
	return blockIdx(ii), nil

}

func WritePbfFile(inc []chan elements.ExtendedBlock, outfn string, isc bool) (write.BlockIdxWrite, error) {
	outf, err := os.Create(outfn)
	if err != nil {
		return nil, err
	}
	defer outf.Close()

	tf, err := ioutil.TempFile("", "osmquadtree.writefile.tmp")
	if err != nil {
		return nil, err
	}

	defer func() {
		tf.Close()
		os.Remove(tf.Name())
	}()
	return WritePbfIndexed(inc, outf, tf, true, isc, false)
}

func WritePbfIndexed(inc []chan elements.ExtendedBlock, outf io.Writer, tf io.ReadWriter, indexed bool, ischange bool, plain bool) (write.BlockIdxWrite, error) {

	addBl := func(bl elements.ExtendedBlock, i int) (utils.Idxer, error) {
		return addFullBlock(bl, i, ischange, []byte("OSMData"))
	}

	if !indexed {
		if plain {
			addBl = func(bl elements.ExtendedBlock, i int) (utils.Idxer, error) {
				return addOrigBlock(bl, []byte("OSMData"))
			}
		}

		ii, err := WriteBlocksOrdered(inc, outf, addBl, true)
		for i, _ := range ii {

			ii[i].Isc = ischange
		}
		return blockIdx(ii), err

	}
	if tf == nil {
		panic("tempfile nil")
	}
	ii, err := WriteBlocksOrdered(inc, tf, addBl, true)

	if err != nil {
		return nil, err
	}

	return finishAndHeader(outf, tf, ii, ischange)
}

func checkprogress(cc chan IdxItem, ll int) {
	st := time.Now()
	var p IdxItem
	tl := int64(0)
	for p = range cc {
		tl += p.Len
		if (p.Idx % 1371) == 0 {
			fmt.Printf("\r%8.1fs %6d %-18s %8d bytes [%12d total]", time.Since(st).Seconds(), p.Idx, p.Quadtree, p.Len, tl)
		}
	}
	if (p.Idx % 1371) != 0 {
		fmt.Printf("\r%8.1fs %6d %-18s %8d bytes [%12d total]", time.Since(st).Seconds(), p.Idx, p.Quadtree, p.Len, tl)
	}
	log.Printf("\n")
}

func WriteQts(inc <-chan elements.ExtendedBlock, outfn string) error {
	outf, err := os.Create(outfn)
	if err != nil {
		return err
	}
	defer outf.Close()
	_, err = WriteBlocksOrdered(readfile.SplitExtendedBlockChans(inc, 4), outf, addQtBlock, false)
	return err
}

func WriteBlocksOrdered(
	inchans []chan elements.ExtendedBlock,
	outf io.Writer,
	addBlock func(elements.ExtendedBlock, int) (utils.Idxer, error),
	prog bool) ([]IdxItem, error) {

	vv := make([]chan utils.Idxer, len(inchans))
	for j, _ := range inchans {
		vv[j] = make(chan utils.Idxer, 5)
	}

	for i, _ := range inchans {

		go func(i int) {
			for bl := range inchans[i] {
				t, err := addBlock(bl, 0)
				if err != nil {
					panic(err.Error())
				}

				vv[i] <- t
			}
			close(vv[i])
		}(i)
	}

	st := time.Now()
	items := make([]IdxItem, 0, 450000)
	rem := 4
	j := 0

	var progc chan IdxItem
	if prog {
		progc = make(chan IdxItem)
		go checkprogress(progc, 1371)
	}

	for rem > 0 {

		p, ok := <-vv[j%4]
		if !ok {
			rem -= 1
		} else {

			d := p.(DataQuadtreer)
			if d.Data() != nil {

				pbffile.WriteFileBlock(outf, d.Data())
				li := IdxItem{p.Idx(), d.Quadtree(), int64(len(d.Data())), false}
				items = append(items, li)
				if prog {
					progc <- li
				}

			} else {
				log.Printf("\n%8.1fs: NULL p.Idx()\n", time.Since(st).Seconds(), p.Idx())

				items = append(items, IdxItem{p.Idx(), d.Quadtree(), int64(0), false})
			}
		}

		j++
	}
	if prog {
		close(progc)
	}

	return items, nil
}
