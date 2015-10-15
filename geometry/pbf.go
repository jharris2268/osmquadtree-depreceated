// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package geometry

import (
	"math"
    "fmt"
	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
	"github.com/jharris2268/osmquadtree/utils"
)

func readPoint(indata []byte) (Coord, error) {
	pos, msg := utils.ReadPbfTag(indata, 0)
	id, ln, lt := int64(0), int64(0), int64(0)

	for msg.Tag > 0 {
		switch msg.Tag {
		case 1:
			id = utils.UnZigzag(msg.Value)
		case 2:
			ln = utils.UnZigzag(msg.Value)
		case 3:
			lt = utils.UnZigzag(msg.Value)
		}
		pos, msg = utils.ReadPbfTag(indata, pos)
	}
	return coordImpl{elements.Ref(id), ln, lt}, nil
}

func readLinestring(indata []byte) ([]Coord, error) {
	pos, msg := utils.ReadPbfTag(indata, 0)
	id, ln, lt := []int64{}, []int64{}, []int64{}
	var err error
	for msg.Tag > 0 {
		switch msg.Tag {
		case 1:
			id, err = utils.ReadDeltaPackedList(msg.Data)
		case 2:
			ln, err = utils.ReadDeltaPackedList(msg.Data)
		case 3:
			lt, err = utils.ReadDeltaPackedList(msg.Data)
		}
		if err != nil {
			return nil, err
		}
		pos, msg = utils.ReadPbfTag(indata, pos)
	}
	ans := make([]Coord, len(id))
	for i, ii := range id {
		ans[i] = coordImpl{elements.Ref(ii), ln[i], lt[i]}
	}
	return ans, nil
}

func readPolygon(indata []byte) ([][]Coord, error) {
	pos, msg := utils.ReadPbfTag(indata, 0)
	ans := make([][]Coord, 0, 5)
	for msg.Tag > 0 {
		if msg.Tag == 1 {
			ln, err := readLinestring(msg.Data)
			if err != nil {
				return nil, err
			}
			ans = append(ans, ln)
		}
		pos, msg = utils.ReadPbfTag(indata, pos)
	}
	return ans, nil
}

func extractGeometryData(indata []byte) (
	GeometryType, [][][]Coord, int64, float64, error) {

	pos, msg := utils.ReadPbfTag(indata, 0)

	objs := make([][][]Coord, 0, 5)
	zo := int64(0)
	ar := float64(0.0)
	gt := NullGeometry

	for msg.Tag > 0 {
		var err error
		switch msg.Tag {
		case 10:
			gt = GeometryType(msg.Value)
		case 11:
			zo = utils.UnZigzag(msg.Value)
		case 12:
			ar = math.Float64frombits(msg.Value)
		case 13:
			var pt Coord
			pt, err = readPoint(msg.Data)
			objs = append(objs, [][]Coord{[]Coord{pt}})
		case 14:
			var ln []Coord
			ln, err = readLinestring(msg.Data)
			objs = append(objs, [][]Coord{ln})
		case 15:
			var py [][]Coord
			py, err = readPolygon(msg.Data)

			objs = append(objs, py)
		}
		if err != nil {
			return 0, nil, 0, 0, err
		}
		pos, msg = utils.ReadPbfTag(indata, pos)
	}

	return gt, objs, zo, ar, nil

}

func readBbox(indata []byte) (quadtree.Bbox, error) {
	mx, my, sx, sy := int64(0), int64(0), int64(0), int64(0)

	for pos, msg := utils.ReadPbfTag(indata, 0); msg.Tag > 0; pos, msg = utils.ReadPbfTag(indata, pos) {
		switch msg.Tag {
		case 1:
			mx = utils.UnZigzag(msg.Value)
		case 2:
			my = utils.UnZigzag(msg.Value)
		case 5:
			sx = utils.UnZigzag(msg.Value)
		case 6:
			sy = utils.UnZigzag(msg.Value)
		}
	}
	return quadtree.Bbox{mx, my, mx + sx, my + sy}, nil
}

func packBbox(bb quadtree.Bbox) []byte {
	ans := make(utils.PbfMsgSlice, 4)
	ans[0] = utils.PbfMsg{1, nil, utils.Zigzag(bb.Minx)}
	ans[1] = utils.PbfMsg{2, nil, utils.Zigzag(bb.Miny)}

	ans[2] = utils.PbfMsg{5, nil, utils.Zigzag(bb.Maxx - bb.Minx)}
	ans[3] = utils.PbfMsg{6, nil, utils.Zigzag(bb.Maxy - bb.Miny)}
	return ans.Pack()
}

func extractGeometry(gp elements.PackedGeometry) (Geometry, error) {
	gt, objs, zorder, area, err := extractGeometryData(gp.GeometryData())
	if err != nil {
		return nil, err
	}

	switch gt {
	case Point:
		return makePointGeometry(gp, gp.Tags(), objs[0][0][0]), nil
	case Linestring:
		return makeLinestringGeometry(gp, gp.Tags(), objs[0][0], zorder), nil
	case Polygon:
		return makePolygonGeometry(gp, gp.Tags(), objs[0], zorder, area), nil
	case MultiPoint, MultiLinestring, MultiPolygon, Multi:
		return makeMultiGeometry(gp, gp.Tags(), objs, zorder, area), nil
	}
    fmt.Println("???",gt,objs,zorder,area)
	return nil, UnrecognisedGeometryError
}
func readGeometryBbox(indata []byte) (GeometryType, *quadtree.Bbox, error) {
	pos, msg := utils.ReadPbfTag(indata, 0)
	gt := NullGeometry
	for msg.Tag > 0 {
		switch msg.Tag {
		case 10:
			gt = GeometryType(msg.Value)
		case 13:
			if gt == 1 {
				pt, err := readPoint(msg.Data)
				if err != nil {
					return 0, nil, err
				}
				return 1, &quadtree.Bbox{pt.Lon(), pt.Lat(), pt.Lon(), pt.Lat()}, nil
			}
		case 16:
			bx, err := readBbox(msg.Data)
			if err != nil {
				return 0, nil, err
			}
			return gt, &bx, nil

		}
		pos, msg = utils.ReadPbfTag(indata, pos)

	}
	return gt, nil, nil
}

func packCoord(c Coord) []byte {
	msgs := make(utils.PbfMsgSlice, 3)
	msgs[0] = utils.PbfMsg{1, nil, utils.Zigzag(int64(c.Ref()))}
	msgs[1] = utils.PbfMsg{2, nil, utils.Zigzag(c.Lon())}
	msgs[2] = utils.PbfMsg{3, nil, utils.Zigzag(c.Lat())}
	return msgs.Pack()
}

func packPointData(c Coord) []byte {

	msgs := make(utils.PbfMsgSlice, 2)
	msgs[0] = utils.PbfMsg{10, nil, 1}
	msgs[1] = utils.PbfMsg{13, packCoord(c), 0}

	return msgs.Pack()
}

func packRing(cc []Coord) []byte {
	ii, ln, lt := make([]int64, len(cc)), make([]int64, len(cc)), make([]int64, len(cc))
	for i, c := range cc {
		ii[i] = int64(c.Ref())
		ln[i] = c.Lon()
		lt[i] = c.Lat()
	}
	msgs := make(utils.PbfMsgSlice, 3)
	iip, err := utils.PackDeltaPackedList(ii)
	if err != nil {
		return nil
	}
	msgs[0] = utils.PbfMsg{1, iip, 0}

	lnp, err := utils.PackDeltaPackedList(ln)
	if err != nil {
		return nil
	}
	msgs[1] = utils.PbfMsg{2, lnp, 0}

	ltp, err := utils.PackDeltaPackedList(lt)
	if err != nil {
		return nil
	}
	msgs[2] = utils.PbfMsg{3, ltp, 0}
	return msgs.Pack()
}

func packLinestringData(cc []Coord, zo int64, bb *quadtree.Bbox) []byte {
	msgs := make(utils.PbfMsgSlice, 4)
	msgs[0] = utils.PbfMsg{10, nil, 2}
	msgs[1] = utils.PbfMsg{11, nil, utils.Zigzag(zo)}
	msgs[2] = utils.PbfMsg{14, packRing(cc), 0}
	if bb != nil {
		msgs[3] = utils.PbfMsg{16, packBbox(*bb), 0}
	}

	return msgs.Pack()
}

func packPolygon(cc [][]Coord) []byte {
	msgs := make(utils.PbfMsgSlice, len(cc))
	for i, c := range cc {
		msgs[i] = utils.PbfMsg{1, packRing(c), 0}
	}
	return msgs.Pack()
}

func packPolygonData(cc [][]Coord, zo int64, ar float64, bb *quadtree.Bbox) []byte {
	msgs := make(utils.PbfMsgSlice, 5)
	msgs[0] = utils.PbfMsg{10, nil, 3}
	msgs[1] = utils.PbfMsg{11, nil, utils.Zigzag(zo)}
	msgs[2] = utils.PbfMsg{12, nil, math.Float64bits(ar)}
	msgs[3] = utils.PbfMsg{15, packPolygon(cc), 0}
	if bb != nil {
		msgs[4] = utils.PbfMsg{16, packBbox(*bb), 0}
	}

	return msgs.Pack()
}

func packMultiGeometryData(cc [][][]Coord, zo int64, ar float64, bb *quadtree.Bbox) []byte {
	msgs := make(utils.PbfMsgSlice, 4+len(cc))
	msgs[0] = utils.PbfMsg{10, nil, 7}
	msgs[1] = utils.PbfMsg{11, nil, utils.Zigzag(zo)}
	msgs[2] = utils.PbfMsg{12, nil, math.Float64bits(ar)}
	for i, c := range cc {
		msgs[i+3] = utils.PbfMsg{15, packPolygon(c), 0}
	}
	if bb != nil {
		msgs[len(cc)+3] = utils.PbfMsg{16, packBbox(*bb), 0}
	}

	return msgs.Pack()
}
