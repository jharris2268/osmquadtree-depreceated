// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package packeddatastore

import (
	"log"

	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/geometry"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/sqlselect"
	"github.com/jharris2268/osmquadtree/utils"

	"fmt"
	"math"
)

type BlockStore interface {
	Add([]byte) int
	Get(int) []byte
	Stats() (int, int)
}

func MakeBlockStore() BlockStore {
	return blockStoreImpl{}
}

type PackedDataStore interface {
	AddBlock(elements.ExtendedBlock)
	Filter(quadtree.Bbox, geometry.GeometryType, []string) sqlselect.Result

	Keys() quadtree.QuadtreeSlice
	ByKey(q quadtree.Quadtree) elements.Block
}

func MakePackedDataStore(commonstrs []string, qtalloc func(e elements.FullElement) quadtree.Quadtree, bs BlockStore) PackedDataStore {
	if len(commonstrs) > 300000 {
		panic("too many strings")
	}

	commonvals := map[string]int64{}
	for i, c := range commonstrs {
		commonvals[c] = int64(i)
	}

	return &packedDataStoreImpl{
		commonstrs,
		commonvals,
		bs,
		map[quadtree.Quadtree][]objRow{},
		map[quadtree.Quadtree][]objRow{},
		map[quadtree.Quadtree][]objRow{},
		qtalloc,
	}
}

type packedDataStoreImpl struct {
	commonstrs []string
	commonvals map[string]int64
	blockstore BlockStore
	point      map[quadtree.Quadtree][]objRow
	line       map[quadtree.Quadtree][]objRow
	polygon    map[quadtree.Quadtree][]objRow
	qtalloc    func(e elements.FullElement) quadtree.Quadtree
}

/*blockstore... */

type block struct {
	data  []byte
	start []int
	tot   int
}

func (v *block) Add(bl []byte) int {
	i := len(v.start)
	copy(v.data[v.tot:], bl)
	v.start = append(v.start, v.tot)
	v.tot += len(bl)
	return i
}

func (v *block) Get(i int) []byte {
	if i >= len(v.start) {
		return nil
	}
	s := v.start[i]
	t := v.tot
	if i < (len(v.start) - 1) {
		t = v.start[i+1]
	}
	return v.data[s:t]
}

func newBlock() *block {
	return &block{make([]byte, 1024*1024), make([]int, 0, 1024), 0}
}

type blockStoreImpl map[int]*block

func (bs blockStoreImpl) Add(bl []byte) int {
	l := len(bl)
	if l > 1024*1024 {
		k := len(bs)
		bs[k] = &block{bl, []int{0}, len(bl)}
		return (k << 32) | 0
	}

	for k, v := range bs {

		if v.tot+l < 1024*1024 {
			i := v.Add(bl)
			return (k << 32) | i
		}

	}

	nb := newBlock()
	k := len(bs)
	i := nb.Add(bl)
	bs[k] = nb
	return (k << 32) | i
}

func (bs blockStoreImpl) Get(idx int) []byte {
	k := idx >> 32
	i := idx & 0xffffffff
	v, ok := bs[k]
	if !ok {
		return nil
	}
	return v.Get(i)
}

func (bs blockStoreImpl) Stats() (int, int) {
	no := 0
	for _, b := range bs {
		no += len(b.start)
	}

	return no, 1024 * 1024 * len(bs)
}

/*packedDataStoreImpl Add */

func packObjTags(tags []int64, strs []string) []byte {
	tl := 2*len(tags) + 3*len(strs) + 4
	for _, s := range strs {
		tl += len([]byte(s))
	}
	mm := make([]byte, tl)
	p := 0

	p = utils.WriteVarint(mm, p, int64(len(tags)))
	for _, t := range tags {
		p = utils.WriteVarint(mm, p, t)
	}
	p = utils.WriteVarint(mm, p, int64(len(strs)))
	for _, s := range strs {
		p = utils.WriteData(mm, p, []byte(s))
	}
	return mm[:p]
}

func gets(i int64, cs []string, ss []string) string {
	if i < 0 {
		return cs[-i]
	}
	return ss[i]
}

func readObjTags(tt []byte, commonstrs []string) map[string]string {
	nv, p := utils.ReadVarint(tt, 0)
	nt := make([]int64, nv)
	for i := 0; i < int(nv); i++ {
		nt[i], p = utils.ReadVarint(tt, p)
	}

	ns, p := utils.ReadVarint(tt, p)
	ss := make([]string, ns)
	for i := 0; i < int(ns); i++ {
		var b []byte
		b, p = utils.ReadData(tt, p)
		ss[i] = string(b)
	}

	ans := map[string]string{}
	for i := 0; i < len(nt); i += 2 {
		k := gets(nt[i], commonstrs, ss)
		v := gets(nt[i+1], commonstrs, ss)
		ans[k] = v
	}
	return ans
}

type objRow struct {
	data int
	bbox [4]int32
	tags int
}

func getstr(s string, commonvals map[string]int64, ss []string) (int64, []string) {
	i, ok := commonvals[s]
	if ok {
		return i, ss
	}
	i = int64(len(ss))
	ss = append(ss, s)
	return i, ss
}

func makeObjRowTags(tgs elements.Tags, commonvals map[string]int64) ([]int64, []string) {

	tg := make([]int64, 2*tgs.Len())
	ss := make([]string, 0, 5)
	for i := 0; i < tgs.Len(); i++ {
		tg[2*i], ss = getstr(tgs.Key(i), commonvals, ss)
		tg[2*i+1], ss = getstr(tgs.Value(i), commonvals, ss)
	}

	return tg, ss
}

func (pdsi *packedDataStoreImpl) AddBlock(bl elements.ExtendedBlock) {

	for i := 0; i < bl.Len(); i++ {
		e := bl.Element(i)
		gt, bx, _, err := geometry.ExtractGeometryBboxData(e)
		if err != nil {
			return
		}
		fe := e.(geometry.Geometry)
		tg, ss := makeObjRowTags(fe.Tags(), pdsi.commonvals)
		ot := pdsi.blockstore.Add(packObjTags(tg, ss))

		pp := elements.PackElement(fe.Type(), fe.ChangeType(), fe.Id(), fe.Quadtree(), fe.GeometryData(), nil, nil)
		od := pdsi.blockstore.Add(pp)

		or := objRow{od, [4]int32{int32(bx.Minx), int32(bx.Miny), int32(bx.Maxx), int32(bx.Maxy)}, ot}

		k := pdsi.qtalloc(fe)

		switch gt {
		case geometry.Point:
			pdsi.point[k] = append(pdsi.point[k], or)
		case geometry.Linestring:
			pdsi.line[k] = append(pdsi.line[k], or)
		case geometry.Polygon, geometry.Multi:
			pdsi.polygon[k] = append(pdsi.polygon[k], or)
		}

	}

}

/* objRow result... */

type fr string

func (f fr) String() string { return string(f) }
func (f fr) Value(row sqlselect.Row) sqlselect.Value {
	i := row.Index(string(f))
	return row.Value(i)
}

func (f fr) Key() string { return string(f) }

type filtRowSlice struct {
	cols []string
	cm   map[string]int
	vals []sqlselect.Value
}

func (frs *filtRowSlice) Len() int                { return len(frs.vals) / len(frs.cols) }
func (frs *filtRowSlice) Row(i int) sqlselect.Row { return filtRow{i, frs} }
func (frs *filtRowSlice) Columns() []sqlselect.Rower {
	r := make([]sqlselect.Rower, len(frs.cols))
	for i, c := range frs.cols {
		r[i] = fr(c)
	}

	return r
}

type filtRow struct {
	i   int
	frs *filtRowSlice
}

func (f filtRow) Len() int                    { return len(f.frs.cols) }
func (f filtRow) Key(i int) string            { return f.frs.cols[i] }
func (f filtRow) Value(i int) sqlselect.Value { return f.frs.vals[i+len(f.frs.cols)*f.i] }
func (f filtRow) Index(k string) int {
	a, ok := f.frs.cm[k]
	if ok {
		return a
	}
	return -1
}

func rowString(rw sqlselect.Row) string {
	r := "{"
	for j := 0; j < rw.Len(); j++ {
		r += fmt.Sprintf("%s=%s; ", rw.Key(j), rw.Value(j).String())
	}
	r += "}"
	return r
}

func (pdsi *packedDataStoreImpl) Filter(bb quadtree.Bbox, gt geometry.GeometryType, cols []string) sqlselect.Result {
	rs := make([]sqlselect.Value, 0, 1000)
	cm := map[string]int{}
	for i, c := range cols {
		cm[c] = i
	}

	var objs map[quadtree.Quadtree][]objRow
	switch gt {
	case geometry.Point:
		objs = pdsi.point
	case geometry.Linestring:
		objs = pdsi.line
	case geometry.Polygon:
		objs = pdsi.polygon
	}
	if objs == nil {
		return nil
	}

	for k, v := range objs {
		if !k.Bounds(0.05).Intersects(bb) {
			continue
		}
		for _, o := range v {

			ob := quadtree.Bbox{int64(o.bbox[0]), int64(o.bbox[1]), int64(o.bbox[2]), int64(o.bbox[3])}
			if !ob.Intersects(bb) {
				continue
			}

			r := make([]sqlselect.Value, len(cols))

			gd := pdsi.blockstore.Get(o.data)
			pg, ok := elements.UnpackElement(gd).(elements.PackedGeometry)
			if !ok {
				log.Println("?? not a geometry", elements.PackedElement(gd))
				continue
			}

			zo, ar, _ := geometry.ReadGeometryZOrderWayArea(pg.GeometryData())

			vs := readObjTags(pdsi.blockstore.Get(o.tags), pdsi.commonstrs)

			for i, c := range cols {
				switch c {
				case "osm_id":

					oi := int64(pg.Id()) & 0xffffffffffff
					if (pg.Id() >> 59) == 2 {
						oi *= -1
					}
					r[i] = sqlselect.IntValue(oi)

				case "way":
					r[i] = sqlselect.GeomValue(pg.GeometryData())
				case "z_order":
					r[i] = sqlselect.IntValue(zo)
				case "way_area":
					r[i] = sqlselect.FloatValue(ar)
				case "quadtree":
					r[i] = sqlselect.IntValue(int64(pg.Quadtree()))

				default:
					s, ok := vs[c]
					if ok {
						r[i] = sqlselect.StringValue(s)
					} else {
						r[i] = &sqlselect.NullValue{}
					}
				}
			}
			rs = append(rs, r...)
		}
	}
	ss := &filtRowSlice{cols, cm, rs}

	return ss
}

func (pdsi *packedDataStoreImpl) Keys() quadtree.QuadtreeSlice {
	qtm := map[quadtree.Quadtree]bool{}
	for k := range pdsi.point {
		qtm[k] = true
	}
	for k := range pdsi.line {
		qtm[k] = true
	}
	for k := range pdsi.polygon {
		qtm[k] = true
	}

	qts := make(quadtree.QuadtreeSlice, 0, len(qtm))
	for k := range qtm {
		qts = append(qts, k)
	}
	return qts
}

func (pdsi *packedDataStoreImpl) ByKey(qt quadtree.Quadtree) elements.Block {

	pt := pdsi.point[qt]
	ln := pdsi.line[qt]
	py := pdsi.polygon[qt]
	res := make(elements.ByElementId, 0, len(pt)+len(ln)+len(py))
	for _, pp := range [][]objRow{pt, ln, py} {
		if pp != nil {
			for _, o := range pp {
				ele := elements.UnpackElement(pdsi.blockstore.Get(o.data)).(elements.PackedGeometry)
				tags := readObjTags(pdsi.blockstore.Get(o.tags), pdsi.commonstrs)
				kk, vv := make([]string, 0, len(tags)), make([]string, 0, len(tags))
				for k, v := range tags {
					kk = append(kk, k)
					vv = append(vv, v)
				}
				ele.(interface {
					SetTags(elements.Tags)
				}).SetTags(elements.MakeTags(kk, vv))

				res = append(res, ele)
			}
		}
	}
	res.Sort()
	return res
}

/* rowAsElement */

type rowAsElement struct {
	i int
	r sqlselect.Row
}
type rowAsTags struct {
	r sqlselect.Row
}

func (rat rowAsTags) Len() int {
	c := 0
	for i := 0; i < rat.r.Len(); i++ {
		switch rat.r.Key(i) {
		case "way":
			//pass
		default:
			c++
		}
	}
	return c
}
func packInt(i uint64) []byte {
	r := make([]byte, 10)
	p := utils.WriteUvarint(r, 0, i)
	return r[:p]
}
func (rat rowAsTags) KeyVal(ii int) ([]byte, []byte) {
	c := 0
	for i := 0; i < rat.r.Len(); i++ {
		switch rat.r.Key(i) {
		case "way":
			continue
		}
		if ii == c {
			k := rat.r.Key(i)
			v := rat.r.Value(i)
			switch v.Type() {
			case "STRING":
				return []byte(k), []byte(v.AsString())
			case "INTEGER":
				return []byte("!" + k), packInt(utils.Zigzag(v.AsInt()))
			case "FLOAT":
				return []byte("%" + k), packInt(math.Float64bits(v.AsFloat()))
			}

			return []byte("$" + k), []byte{}
		}
		c++

	}
	return nil, nil
}
func (rat rowAsTags) Key(i int) string {
	k, _ := rat.KeyVal(i)
	return string(k)
}
func (rat rowAsTags) Value(i int) string {
	_, v := rat.KeyVal(i)
	if v == nil {
		return ""
	}
	return string(v)
}
func (rat rowAsTags) Pack() []byte {
	kk := make([][]byte, 0, rat.r.Len())
	vv := make([][]byte, 0, rat.r.Len())
	tl := 10
	for i := 0; i < rat.r.Len(); i++ {
		k, v := rat.KeyVal(i)
		if k == nil {
			continue
		}
		kk = append(kk, k)
		vv = append(vv, v)
		tl += len(k) + len(v) + 20
	}
	res := make([]byte, tl)
	p := utils.WriteVarint(res, 0, int64(len(kk)))
	for i, k := range kk {
		p = utils.WriteData(res, p, k)
		p = utils.WriteData(res, p, vv[i])
	}
	return res[:p]
}

func (rae rowAsElement) Id() elements.Ref                  { return elements.Ref(rae.i) }
func (rae rowAsElement) Type() elements.ElementType        { return elements.Geometry }
func (rae rowAsElement) ChangeType() elements.ChangeType   { return elements.Normal }
func (rae rowAsElement) Tags() elements.Tags               { return rowAsTags{rae.r} }
func (rae rowAsElement) Info() elements.Info               { return nil }
func (rae rowAsElement) Quadtree() quadtree.Quadtree       { return quadtree.Null }
func (rae rowAsElement) SetQuadtree(quadtree.Quadtree)     {}
func (rae rowAsElement) SetChangeType(elements.ChangeType) {}

func (rae rowAsElement) GeometryData() []byte {
	wy := rae.r.Index("way")
	if wy < 0 {
		return nil
	}
	ii := rae.r.Value(wy).(*sqlselect.NullValue).F.([]byte)
	return ii
}
func (rae rowAsElement) Pack() []byte {
	return elements.PackElement(rae.Type(), rae.ChangeType(), rae.Id(), quadtree.Null, rae.GeometryData(), nil, rae.Tags().Pack())
}
func (rae rowAsElement) String() string {
	return fmt.Sprintf("Row %d: {%s} %d bytes", rae.i, rowString(rae.r))
}

func MakeRowAsElement(i int, r sqlselect.Row) elements.FullElement {
	return rowAsElement{i, r}
}
