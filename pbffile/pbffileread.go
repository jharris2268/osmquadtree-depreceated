package pbffile

import (
	"encoding/binary"
	"errors"

	"io"
	"os"
	"sync"

	//"reflect"

	"github.com/jharris2268/osmquadtree/utils"
)

type fileBlock struct {
	idx       int
	filePos   int64
	blockType []byte
	blockData []byte
}



func readNextBlock(file io.ReadSeeker, idx int) (*fileBlock, error) {
	pos, err := file.Seek(0, 1)
	if err != nil {
		return nil, err
	}
	res := fileBlock{idx, pos, nil, nil}

	var blobHeaderSize int32

	err = binary.Read(file, binary.BigEndian, &blobHeaderSize)
	if err != nil {
		return nil, err
	}

	if blobHeaderSize < 0 || blobHeaderSize > (64*1024*1024) {
		return nil, err
	}

	blobHeaderBytes, err := utils.ReadBlock(file, uint64(blobHeaderSize))
	if err != nil {
		return nil, err
	}

	blsz := uint64(0)
	res.blockType, blsz, err = readBlobHeader(blobHeaderBytes)

	res.blockData, err = utils.ReadBlock(file, blsz)
	if err != nil {
		return nil, err
	}

	return &res, nil
}
func readBlobHeader(data []byte) ([]byte, uint64, error) {

	pos, msg := utils.ReadPbfTag(data, 0)
	blockType, dataSize := []byte{}, uint64(0)
	for msg.Tag > 0 {
		if msg.Tag == 1 {
			if msg.Data == nil {
				return nil, 0, errors.New("b=1: incorrect message type")
			}
			blockType = msg.Data
		} else if msg.Tag == 3 {
			if msg.Data != nil {
				return nil, 0, errors.New("b=3 incorrect message type")
			}
			dataSize = msg.Value
		}
		pos, msg = utils.ReadPbfTag(data, pos)
	}

	return blockType, dataSize, nil
}

func readBlockData(inblock *fileBlock) (*fileBlock, error) {

	pos, msg := utils.ReadPbfTag(inblock.blockData, 0)

	rs, zd := uint64(0), []byte{}

	for msg.Tag > 0 {
		switch msg.Tag {
		case 1:
			if msg.Data == nil {
				return nil, errors.New("b=1: incorrect message type")
			}
			inblock.blockData = msg.Data
			return inblock, nil
		case 2:
			if msg.Data != nil {
				return nil, errors.New("b=2: incorrect message type")
			}
			rs = msg.Value
		case 3:
			if msg.Data == nil {
				return nil, errors.New("b=3: incorrect message type")
			}
			zd = msg.Data
		}
		pos, msg = utils.ReadPbfTag(inblock.blockData, pos)
	}
	if rs == 0 || len(zd) == 0 {
		return nil, errors.New("No data??")
	}
	var err error
	inblock.blockData, err = utils.Decompress(zd, rs)
	if err != nil {
		return nil, err
	}
	return inblock, nil
}

type FileBlock interface {
	Idx() int
	FilePosition() int64
	BlockType() []byte
	BlockData() []byte
}

func (fb *fileBlock) Idx() int            { return fb.idx }
func (fb *fileBlock) FilePosition() int64 { return fb.filePos }
func (fb *fileBlock) BlockType() []byte   { return fb.blockType }
func (fb *fileBlock) BlockData() []byte   { return fb.blockData }

func ReadPbfFileBlocks(file io.ReadSeeker) <-chan FileBlock {
	res := make(chan FileBlock)
	i := 0
	go func() {
        fc,ok := file.(io.ReadCloser)
        if ok {
            defer fc.Close()
        }

		for {
			bl, err := readNextBlock(file, i)
			i++
			if err != nil {
				if err != io.EOF {
					println(err.Error())
				}
				close(res)
				return
			}
			//println(bl.FilePosition(),string(bl.BlockType()),len(bl.BlockData()))
			bl, err = readBlockData(bl)
			if err != nil {
				println(err.Error())
				close(res)
				return
			}
			res <- bl
		}
	}()
	return res
}

func ReadPbfFileBlocksMulti(file *os.File, nc int) <-chan FileBlock {
	resA := make(chan *fileBlock)
	i := 0
	go func() {
		defer file.Close()
		for {
			bl, err := readNextBlock(file, i)
			i++
			if err != nil {
				if err != io.EOF {
					println(err.Error())
				}
				close(resA)

				return
			}
			resA <- bl
		}
	}()
	return readBlockDataMulti(resA, nc)
}

func readBlockDataMulti(resA <-chan *fileBlock, nc int) <-chan FileBlock {
	res := make(chan FileBlock)
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(nc)
		for i := 0; i < nc; i++ {
			go func() {
				for bl := range resA {
					//println(bl.FilePosition(),string(bl.BlockType()),len(bl.BlockData()))
					bln, err := readBlockData(bl)
					if err != nil {
						println(err.Error())
						//close(res)
						wg.Done()
						return
					}
					res <- bln
				}
				wg.Done()
			}()
		}
		wg.Wait()
		close(res)
	}()
	return res
}

func ReadPbfFileBlocksMultiPartial(file *os.File, nc int, filePoses []int64) <-chan FileBlock {
	resA := make(chan *fileBlock)

	go func() {
		for i, fp := range filePoses {
			file.Seek(fp, 0)
			bl, err := readNextBlock(file, i)
			i++
			if err != nil {
				if err != io.EOF {
					println(err.Error())
				}
				close(resA)
				return
			}
			resA <- bl
		}
		close(resA)
	}()
	return readBlockDataMulti(resA, nc)
}

func ReadPbfFileBlockAt(file *os.File, pos int64) (FileBlock, error) {
	_, err := file.Seek(pos, 0)
	if err != nil {
		return nil, err
	}
	var bl *fileBlock
	bl, err = readNextBlock(file, 0)
	if err != nil {
		return nil, err
	}
	bl, err = readBlockData(bl)
	if err != nil {
		return nil, err
	}
	return bl, nil
}

func prepHeaderBlocks(blckType []byte, blckData []byte, comp bool) ([]byte, []byte, error) {
	msgs := make(utils.PbfMsgSlice, 0, 3)
	if !comp {
		msgs = append(msgs, utils.PbfMsg{1, blckData, 0})
	} else {
		msgs = append(msgs, utils.PbfMsg{2, nil, uint64(len(blckData))})
		cc, err := utils.Compress(blckData)
		if err != nil {
			return nil, nil, err
		}
		msgs = append(msgs, utils.PbfMsg{3, cc, 0})
	}

	bl := msgs.Pack()

	msgs2 := make(utils.PbfMsgSlice, 0, 2)
	msgs2 = append(msgs2, utils.PbfMsg{1, blckType, 0})
	msgs2 = append(msgs2, utils.PbfMsg{3, nil, uint64(len(bl))})
	bh := msgs2.Pack()
	return bl, bh, nil
}

func WritePbfFileBlock(file io.WriteSeeker, blckType []byte, blckData []byte, comp bool) (int, error) {

	bl, bh, err := prepHeaderBlocks(blckType, blckData, comp)
	if err != nil {
		return 0, err
	}

	file.Seek(0, 1)

	ln := 4 + len(bh) + len(bl)
	ml := int32(len(bh))

	err = binary.Write(file, binary.BigEndian, &ml)
	if err != nil {
		return 0, err
	}

	WriteFileBlockAtEnd(file, bh)
	WriteFileBlockAtEnd(file, bl)
	return ln, nil
}

func PreparePbfFileBlock(blckType []byte, blckData []byte, comp bool) ([]byte, error) {

	bl, bh, err := prepHeaderBlocks(blckType, blckData, comp)
	if err != nil {
		return nil, err
	}

	res := make([]byte, 4+len(bh)+len(bl))

	ml := int32(len(bh))
	res[0] = byte(ml >> 24)
	res[1] = byte(ml >> 16)
	res[2] = byte(ml >> 8)
	res[3] = byte(ml)

	copy(res[4:], bh)
	copy(res[4+len(bh):], bl)
	return res, nil
}

func WriteFileBlock(file io.WriteSeeker, res []byte) (int64, error) {
	p, _ := file.Seek(0, 2)
	return p, WriteFileBlockAtEnd(file, res)
}


func WriteFileBlockAtEnd(file io.Writer, res []byte) error {

	l := 0
	for l < len(res) {
		p, err := file.Write(res[l:])
		if err != nil {
			return err
		}
		l += p
	}
	return nil
}

type FileBlockWrite interface {
	Idx() int
	BlockType() []byte
	BlockData() []byte
}



type Quadtreer interface {
	Quadtree() int64
	IsChange() bool
}

func MakeFileBlockWrite(idx int, blockType []byte, blockData []byte) FileBlockWrite {
	return &fileBlock{idx, 0, blockType, blockData}
}

type fileBlockQt struct {
	idx int
	bb  []byte
	qt  int64
	isc bool
}

func (fbq *fileBlockQt) Idx() int          { return fbq.idx }
func (fbq *fileBlockQt) BlockType() []byte { return []byte("OSMData") }
func (fbq *fileBlockQt) BlockData() []byte { return fbq.bb }
func (fbq *fileBlockQt) Quadtree() int64   { return fbq.qt }
func (fbq *fileBlockQt) IsChange() bool    { return fbq.isc }

func MakeFileBlockWriteQt(idx int, blockType []byte, blockData []byte, qt int64, isc bool) FileBlockWrite {
	return &fileBlockQt{idx, blockData, qt, isc}
}

type BlockIndex interface {
	Len() int
	BlockLen(int) int64
	Quadtree(int) int64
	Idx(int) int
	IsChange(int) bool
}

type blockLen struct {
	idx      int
	len      int64
	quadtree int64
	isChange bool
}

type blockLenSlice []blockLen

func (bls blockLenSlice) Len() int             { return len(bls) }
func (bls blockLenSlice) BlockLen(i int) int64 { return bls[i].len }
func (bls blockLenSlice) Quadtree(i int) int64 { return bls[i].quadtree }
func (bls blockLenSlice) IsChange(i int) bool  { return bls[i].isChange }
func (bls blockLenSlice) Idx(i int) int        { return bls[i].idx }

func WriteFileBlocks(file io.WriteSeeker, inBlocks <-chan FileBlockWrite) (BlockIndex, error) {

	prepBlocks := make(chan utils.Idxer)
	nb := 0
	go func() {
		nbs := make([]int, 4)
		wg := sync.WaitGroup{}
		wg.Add(4)
		for i := 0; i < 4; i++ {
			go func(i int) {
				for bl := range inBlocks {
					pb, err := PreparePbfFileBlock(bl.BlockType(), bl.BlockData(), true)
					if err != nil {
						panic(err.Error())
					}
					blq, ok := bl.(Quadtreer)
					if ok {
						prepBlocks <- MakeFileBlockWriteQt(bl.Idx(), nil, pb, blq.Quadtree(), blq.IsChange())
					} else {
						prepBlocks <- MakeFileBlockWrite(bl.Idx(), nil, pb)
					}

					nbs[i]++
				}

				wg.Done()
			}(i)
		}
		wg.Wait()
		nb = nbs[0] + nbs[1] + nbs[2] + nbs[3]
		close(prepBlocks)
	}()

	prepBlocksSorted := utils.SortIdxerChan(prepBlocks)

	blockLens := make(blockLenSlice, 0, nb)
	for bl := range prepBlocksSorted {
		bld := bl.(FileBlockWrite).BlockData()
		_, err := WriteFileBlock(file, bld)
		if err != nil {
			return nil, err
		}

		blckln := blockLen{idx: bl.Idx(), len: int64(len(bld))}

		blq, ok := bl.(Quadtreer)
		if ok {
			//println(bl.Idx(), blq.Quadtree())
			blckln.quadtree = blq.Quadtree()
			blckln.isChange = blq.IsChange()

		} /*else {
		    println(bl.Idx(),reflect.TypeOf(bl).String())
		}*/
		blockLens = append(blockLens, blckln)
	}
    
    sy,ok := file.(interface{ Sync() })
    if ok {
        sy.Sync()
    }
    
	//file.Sync()
	return blockLens, nil
}




func ReadPbfFileBlocksDefer(file io.ReadSeeker) <-chan FileBlock {
	resA := make(chan FileBlock)
	i := 0
	go func() {
		fc,ok := file.(io.ReadCloser)
        if ok {
            defer fc.Close()
        }
		for {
			bl, err := readNextBlock(file, i)
			i++
			if err != nil {
				if err != io.EOF {
					println(err.Error())
				}
				close(resA)

				return
			}
			resA <- &deferedFileBlock{bl,false}
		}
	}()
	return resA
}

type deferedFileBlock struct {
    fb *fileBlock
    cc bool
}

func (dfb *deferedFileBlock) calc() *fileBlock {
    if dfb.cc {
        return dfb.fb
    }
    dfb.cc=true
    bl, err := readBlockData(dfb.fb)
    if err != nil {
        dfb.fb = &fileBlock{dfb.fb.idx, dfb.fb.filePos, []byte("ERROR"), []byte(err.Error())}
    } else {
        dfb.fb = bl
    }
    return dfb.fb
}

func (dfb *deferedFileBlock) Idx() int { return dfb.calc().Idx() }
func (dfb *deferedFileBlock) FilePosition() int64 { return dfb.calc().FilePosition() }
func (dfb *deferedFileBlock) BlockType() []byte   { return dfb.calc().BlockType() }
func (dfb *deferedFileBlock) BlockData() []byte   { return dfb.calc().BlockData() }


func ReadPbfFileBlocksDeferPartial(file io.ReadSeeker, locs []int64) <-chan FileBlock {
	resA := make(chan FileBlock)
	//i := 0
	go func() {
		fc,ok := file.(io.ReadCloser)
        if ok {
            defer fc.Close()
        }
		for i,lc := range locs {
            _, err := file.Seek(lc,0)
            if err != nil {
                println(err.Error())
				close(resA)
                return
            }
            
			bl, err := readNextBlock(file, i)
			i++
			if err != nil {
				if err != io.EOF {
					println(err.Error())
				}
				close(resA)

				return
			}
			resA <- &deferedFileBlock{bl,false}
		}
        close(resA)
	}()
	return resA
}


func ReadPbfFileBlocksDeferSplitPartial(file io.ReadSeeker, locs []int64, ns int) []<-chan FileBlock {
	resA := make([]chan FileBlock,ns)
    for i,_ := range resA {
        resA[i] = make(chan FileBlock)
    }
	//i := 0
	go func() {
		fc,ok := file.(io.ReadCloser)
        if ok {
            defer fc.Close()
        }
		for i,lc := range locs {
            _, err := file.Seek(lc,0)
            if err != nil {
                println(err.Error())
				for _,r:=range resA {
                    close(r)
                }
                return
            }
            
			bl, err := readNextBlock(file, i)
			i++
			if err != nil {
				if err != io.EOF {
					println(err.Error())
				}
				for _,r:=range resA {
                    close(r)
                }

				return
			}
			resA[bl.Idx()%ns] <- &deferedFileBlock{bl,false}
		}
        for _,r:=range resA {
            close(r)
        }
            
	}()
    
    resRet := make([]<-chan FileBlock,ns)
    for i,r:=range resA {
        resRet[i]=r
    }
    
	return resRet
}


func ReadPbfFileBlockAtDefered(file io.ReadSeeker, pos int64) (FileBlock, error) {
	_, err := file.Seek(pos, 0)
	if err != nil {
		return nil, err
	}
	var bl *fileBlock
	bl, err = readNextBlock(file, 0)
	if err != nil {
		return nil, err
	}
	return &deferedFileBlock{bl,false},nil
}



func ReadPbfFileBlocksDeferSplit(file io.ReadSeeker, ns int) []<-chan FileBlock {
    
	resA := make([]chan FileBlock,ns)
    for i,_ := range resA {
        resA[i] = make(chan FileBlock)
    }
	i := 0
	go func() {
		fc,ok := file.(io.ReadCloser)
        if ok {
            defer fc.Close()
        }
		for {
			bl, err := readNextBlock(file, i)
			i++
			if err != nil {
				if err != io.EOF {
					println(err.Error())
				}
                for _,r:=range resA {
                    close(r)
                }
				return
			}
			resA[bl.Idx()%ns] <- &deferedFileBlock{bl,false}
		}
        for _,r:=range resA {
            close(r)
        }
            
	}()
    
    resRet := make([]<-chan FileBlock,ns)
    for i,r:=range resA {
        resRet[i]=r
    }
    
	return resRet
}
