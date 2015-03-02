// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package quadtree

import (
    "math"
    "errors"
    "sort"
)

var WrongCharErr = errors.New("Wrong Character [not A,B,C,D]")


//A quadtree is a tree structure in which each node has four child nodes.
//This makes very useful for storing geodata [1].
//
//This package represents the nodes of this tree as a (64 bit) integer.
//The positive part is made up of 28 pairs of x and y bits, and the depth
//as 5 bits:
//xyxy xyxy xyxy xyxy xyxy xyxy xyxy xyxy xyxy xyxy xyxy xyxy xyxy xyxy ddddd
//
//Negative numbers can be used to indicate special values:
//e.g. -1 for None, and -2 or unknown.
//
//An alternative method is to express these as strings: each xy pair of
//bits is encoded as an 'A', 'B' , 'C' or 'D'. The depth can be inferred
//from the length of the string. Finally, these values can also be
//transformed to the x, y and z coordinates used from slippy map
//tilenames: set z to equal the depth and make the x and y out the first
//z x and y bits.
//
//(Note that this is broadly the same as described in [2])
//
//[1] http://en.wikipedia.org/wiki/Quadtree
//[2] http://wiki.openstreetmap.org/wiki/QuadTiles
type Quadtree int64


const Null = Quadtree(-1)





func makeQuadTreeFloat(mx, my, Mx, My float64, mxl uint, bf float64) Quadtree {
	if mx > Mx || my > My {
		return -1
	}
	if Mx == mx {
		Mx += 0.0000001
	}
	if My == my {
		My += 0.0000001
	}
	mym := merc(my) / 90.0
	Mym := merc(My) / 90.0
	mxm := mx / 180.0
	Mxm := Mx / 180.0

	return Quadtree(makeQuadTree_(mxm, mym, Mxm, Mym, mxl, bf, 0))
}

func findQuad(mx, my, Mx, My, bf float64) int64 {
	if mx < (-1-bf) || my < (-1-bf) || Mx > (1+bf) || My > (1+bf) {
		return -1
	}

	if (Mx <= 0) && (my >= 0) {
		return 0
	} else if (mx >= 0) && (my >= 0) {
		return 1
	} else if (Mx <= 0) && (My <= 0) {
		return 2
	} else if (mx >= 0) && (My <= 0) {
		return 3

	} else if (Mx < bf && math.Abs(Mx) < math.Abs(mx)) && (my > -bf && math.Abs(My) >= math.Abs(my)) {
		return 0
	} else if (mx > -bf && math.Abs(Mx) >= math.Abs(mx)) && (my > -bf && math.Abs(My) >= math.Abs(my)) {
		return 1
	} else if (Mx < bf && math.Abs(Mx) < math.Abs(mx)) && (My < bf && math.Abs(My) < math.Abs(my)) {
		return 2
	} else if (mx > -bf && math.Abs(Mx) >= math.Abs(mx)) && (My < bf && math.Abs(My) < math.Abs(my)) {
		return 3
	}
	return -1
}

func makeQuadTree_(mx, my, Mx, My float64, mxl uint, bf float64, cl uint) int64 {

	if mxl == 0 {
		return 0
	}

	q := findQuad(mx, my, Mx, My, bf)
	if q == -1 {
		return 0
	}
	if q == 0 || q == 2 {
		mx += 0.5
		Mx += 0.5
	} else {
		mx -= 0.5
		Mx -= 0.5
	}
	if q == 2 || q == 3 {
		my += 0.5
		My += 0.5
	} else {
		my -= 0.5
		My -= 0.5
	}
	return (q << (61 - 2*cl)) + 1 + makeQuadTree_(2*mx, 2*my, 2*Mx, 2*My, mxl-1, bf, cl+1)
}




func (qt Quadtree) String() string {
    if (qt<=Null) {
        return "NULL"
    }
    
    l := qt & 31
	r := make([]byte, l)
	for i, _ := range r {
		v := (qt >> (61 - 2*uint(i))) & 3
		switch v {
		case 0:
			r[i] = 'A'
		case 1:
			r[i] = 'B'
		case 2:
			r[i] = 'C'
		case 3:
			r[i] = 'D'
		}
	}

	return string(r)
}

func (qt Quadtree) Bounds(buffer float64) Bbox {
    
    mx, my, Mx, My := -180., -90., 180., 90.

	l := qt & 31
	r := make([]byte, l)
	for i, _ := range r {
		v := (qt >> (61 - 2*uint(i))) & 3

		switch v {
		case 0, 2:
			Mx -= (Mx - mx) / 2
		case 1, 3:
			mx += (Mx - mx) / 2
		}
		switch v {
		case 2, 3:
			My -= (My - my) / 2
		case 0, 1:
			my += (My - my) / 2
		}

	}

	my = unMerc(my)
	My = unMerc(My)

	if buffer > 0.0 {
		xx := (Mx - mx) * buffer
		yy := (My - my) * buffer
		mx -= xx
		my -= yy
		Mx += xx
		My += yy
	}

	return Bbox{ToInt(mx), ToInt(my), ToInt(Mx), ToInt(My)}
    
}

//Nb. y=0 is north
func (qt Quadtree) Tuple() (int64,int64,int64) {
    z := int64(qt & 31)
	x := int64(0)
	y := int64(0)
	for i := 0; i < int(z); i++ {
		x <<= 1
		y <<= 1
		t := (qt >> uint64(61-2*i)) & 3
		if (t & 1) == 1 {
			x |= 1
		}
		if (t & 2) == 2 {
			y |= 1
		}
	}
	//my := int64(1<<uint(z))

	return x, y, z
}


//Return parent quadtree tile at given level
func (qt Quadtree) Round(level uint) Quadtree {
    if uint(qt&31) < level {
		return qt
	}
	qt >>= (63 - 2*level)
	qt <<= (63 - 2*level)
	return qt + Quadtree(level)
}

//Return largest quadtree tile which is parent to both qt and other
func (qt Quadtree) Common(other Quadtree) Quadtree {
    if qt == -1 {
		return other
	} else if other == -1 {
		return qt
	} else if qt == other {
		return qt
	}

	d := qt & 31
	if other&31 < d {
		d = other & 31
	}
	p := Quadtree(0)

	for i := uint(0); i < uint(d); i++ {
		q := qt.Round(i+1)
		if q != other.Round(i+1) {

			return p
		}
		p = q
	}

	return p
}

//Return smallest quadtree tile which contains the given Bbox.
//If buffer > 0, allow tiles which almost contain object.
//Only descend to makeLevel 
func Calculate(box Bbox, buffer float64, maxLevel uint) (Quadtree,error) {
    return makeQuadTreeFloat(
		ToFloat(box.Minx), ToFloat(box.Miny),
		ToFloat(box.Maxx), ToFloat(box.Maxy),
		maxLevel, buffer), nil
}

func FromTuple(x int64, y int64, z int64) (Quadtree, error) {
    ans := int64(0)
	scale := int64(1)
	for i := uint(0); i < uint(z); i++ {
		ans += ((x>>i)&1|((y>>i)&1)<<1) * scale
		scale *= 4
	}

	ans <<= (63 - (2 * uint(z)))
	ans |= z
	return Quadtree(ans), nil
}

func FromString(str string) (Quadtree, error) {
    
    ans:=0
    for i:=0; i < len(str); i++ {
        p:=-1
        switch str[i] {
            case 'A': p=0
            case 'B': p=1
            case 'C': p=2
            case 'D': p=3
            default: return 0, WrongCharErr
        }
        ans |= p<<(61-2*uint(i))
    }
    ans |= len(str)
    
    return Quadtree(ans),nil
    
}


func merc(y float64) float64 {
	return math.Log(math.Tan(math.Pi*(1.0+y/90.0)/4.0)) * 90.0 / math.Pi
}

func unMerc(d float64) float64 {
	return (math.Atan(math.Exp(d*math.Pi/90.0))*4/math.Pi - 1.0) * 90.0
}

const earth_half_circum = 20037508.3428


//Transform lon lat into spherical mercartor coordinate
func Mercator(ln float64, lt float64) (float64, float64) {
	return ln * earth_half_circum / 180.0, merc(lt) * earth_half_circum / 90.0
}

//Transform spherical mercartor to lon lat
func UnMercator(x, y float64) (float64, float64) {
	return x * 180.0 / earth_half_circum, unMerc(y * 90.0 / earth_half_circum)
}

type QuadtreeSlice []Quadtree
func (qs QuadtreeSlice) Len() int { return len(qs) }
func (qs QuadtreeSlice) Swap(i,j int) { qs[i],qs[j] = qs[j],qs[i] }
func (qs QuadtreeSlice) Less(i,j int) bool { return qs[i] < qs[j] }

func (qs QuadtreeSlice) Sort() { sort.Sort(qs) }
