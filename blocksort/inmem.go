// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package blocksort

import (
	"log"
	"sort"
	"sync"

	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/utils"
)

type objsPacked struct {
	key    int
	packed []byte
	len    int
}

/*SortInMem is a simplier version of SortElementsByAlloc, storing packed
elements in memory. Therefore it can only be used for smaller amounts of
data (eg. input pbf files of less than 6% of the available ram).*/
func SortInMem(
	inChans []chan elements.ExtendedBlock,
	alloc Allocater,
	nc int,
	makeBlock func(int, int, elements.Block) (elements.ExtendedBlock, error)) ([]chan elements.ExtendedBlock, error) {

	cc := make(chan objsPacked)
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(len(inChans))
		for _, inc := range inChans {
			go func(inc chan elements.ExtendedBlock) {
				for b := range inc {

					ll := map[int][][]byte{} // temp store
					for i := 0; i < b.Len(); i++ {
						e := b.Element(i)
						ii := alloc(e)

						ll[ii] = append(ll[ii], e.Pack()) //pack each object in block
					}
					for k, v := range ll {

						// a big []byte is uses less memory then lots of little ones

						outp := make(utils.PbfMsgSlice, len(v))

						for i, o := range v {
							outp[i] = utils.PbfMsg{1, o, 0}
						}

						cc <- objsPacked{k, outp.Pack(), len(v)}

					}
				}
				wg.Done()
			}(inc)

		}
		wg.Wait()
		close(cc)
	}()

	// map of key to slice of stored objects
	tt := map[int][]objsPacked{}
	for c := range cc {
		tt[c.key] = append(tt[c.key], c)
	}

	// sort keys

	kk := make([]int, 0, len(tt))
	for k, _ := range tt {
		kk = append(kk, k)
	}
	sort.Ints(kk)

	res := make([]chan elements.ExtendedBlock, nc)
	for i, _ := range res {
		res[i] = make(chan elements.ExtendedBlock)

		go func(i int) {
			// chan 0 iterates 0, nc, 2*nc, chan 1 => 1,nc+1,2*nc+1 etc.
			for j := i; j < len(kk); j += nc {
				// alloc keys can have gaps
				k := kk[j]

				vv, ok := tt[k]
				if !ok {
					log.Println("WTF", j, k)
					continue
				}

				tl := 0
				for _, v := range vv {
					tl += v.len
				}

				tb := make(elements.ByElementId, 0, tl)
				pp := 0

				for _, v := range vv {
					//retrive objects for each packed block
					pos, msg := utils.ReadPbfTag(v.packed, 0)
					for ; msg.Tag > 0; pos, msg = utils.ReadPbfTag(v.packed, pos) {
						e := elements.UnpackElement(msg.Data)
						tb = append(tb, e)
						pp++
					}
				}
				// sort unpacked objects
				tb.Sort()

				bl, _ := makeBlock(j, k, tb)
				//return on appropiate channel
				res[i] <- bl

			}
			close(res[i])

		}(i)

	}

	return res, nil
}
