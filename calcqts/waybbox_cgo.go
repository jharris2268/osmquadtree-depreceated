package calcqts

/* #cgo LDFLAGS: -lm
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <stdint.h>

inline int64_t toInt(double v) {
    if (v>0) {
        return (v*10000000)+1;
    }
    return (v*10000000)-1;
}

inline double toFloat(int64_t v) {
    return (double)v * 0.0000001;
}

inline double merc(double y) {
	return log(tan(M_PI*(1.0+y/90.0)/4.0)) * 90.0 / M_PI;
}

inline double unMerc(double d) {
	return (atan(exp(d*M_PI/90.0))*4/M_PI - 1.0) * 90.0;
}


int64_t findQuad(double mx, double my, double Mx, double My, double bf) {
	if ((mx < (-1-bf)) || (my < (-1-bf)) || (Mx > (1+bf)) || (My > (1+bf))) {
		return -1;
	}

	if ((Mx <= 0) && (my >= 0)) {
		return 0;
	} else if ((mx >= 0) && (my >= 0)) {
		return 1;
	} else if ((Mx <= 0) && (My <= 0)) {
		return 2;
	} else if ((mx >= 0) && (My <= 0)) {
		return 3;

	} else if ((Mx < bf) && (fabs(Mx) < fabs(mx)) && (my > -bf) && (fabs(My) >= fabs(my))) {
		return 0;
	} else if ((mx > -bf) && (fabs(Mx) >= fabs(mx)) && (my > -bf) && (fabs(My) >= fabs(my))) {
		return 1;
	} else if ((Mx < bf) && (fabs(Mx) < fabs(mx)) && (My < bf) && (fabs(My) < fabs(my))) {
		return 2;
	} else if ((mx > -bf) && (fabs(Mx) >= fabs(mx)) && (My < bf) && (fabs(My) < fabs(my))) {
		return 3;
	}
	return -1;
}

int64_t makeQuadTree_(double mx, double my, double Mx, double My, double bf, size_t mxl, size_t cl)  {

	if (mxl == 0) {
		return 0;
	}

	int64_t q = findQuad(mx, my, Mx, My, bf);
	if (q == -1) {
		return 0;
	}
	if ((q == 0) || (q == 2)) {
		mx += 0.5;
		Mx += 0.5;
	} else {
		mx -= 0.5;
		Mx -= 0.5;
	}
	if ((q == 2) || (q == 3)) {
		my += 0.5;
		My += 0.5;
	} else {
		my -= 0.5;
		My -= 0.5;
	}
	return (q << (61 - 2*cl)) + 1 + makeQuadTree_(2*mx, 2*my, 2*Mx, 2*My, bf, mxl-1, cl+1);
}

int64_t makeQuadTreeFloat(double mx, double my, double Mx, double My, double bf, size_t mxl) {
	if ((mx > Mx) || (my > My)) {
		return -1;
	}
	if (Mx == mx) {
		Mx += 0.0000001;
	}
	if (My == my) {
		My += 0.0000001;
	}
	double mym = merc(my) / 90.0;
	double Mym = merc(My) / 90.0;
	double mxm = mx / 180.0;
	double Mxm = Mx / 180.0;

	return makeQuadTree_(mxm, mym, Mxm, Mym, bf, mxl, 0);
}

int64_t calcQuadtree(int32_t mx, int32_t my, int32_t Mx, int32_t My, size_t mxl, double buf) {
    return makeQuadTreeFloat(toFloat(mx),toFloat(my),toFloat(Mx),toFloat(My),buf,mxl);
}

struct wayBoxImpl {
    int mxw;
    int32_t* a;
    int32_t* b;
    int32_t* c;
    int32_t* d;
};

//typedef void* WayBoxes;
typedef struct wayBoxImpl* WayBoxes;

WayBoxes WayBoxesInit(int mxw) {
    struct wayBoxImpl *wb;
    wb=malloc(sizeof(struct wayBoxImpl));
    wb->mxw = mxw;
    wb->a=malloc(mxw*sizeof(int32_t));
    wb->b=malloc(mxw*sizeof(int32_t));
    wb->c=malloc(mxw*sizeof(int32_t));
    wb->d=malloc(mxw*sizeof(int32_t));
    int i;
    for (i=0; i < mxw; i++) {
        wb->a[i]=2000000000;
        wb->b[i]=2000000000;
        wb->c[i]=-2000000000;
        wb->d[i]=-2000000000;
    }
    return wb;    
}

int WayBoxesExpand(WayBoxes wb, int w, int ln, int lt) {
    
    //struct wayBoxImpl *wb = wbp;
    if (w >= wb->mxw) {
        return;
    }
    int isn =0;
    if (wb->a[w] == 2000000000) {
        isn=1;
    }
    if (ln < wb->a[w]) { wb->a[w] = ln; }
    if (lt < wb->b[w]) { wb->b[w] = lt; }
    if (ln > wb->c[w]) { wb->c[w] = ln; }
    if (lt > wb->d[w]) { wb->d[w] = lt; }
    return isn;
}

int WayBoxesHasValue(WayBoxes wb, int w) {
    if (w >= wb->mxw) { return 0; }
    if (wb->a[w] == 2000000000) { return 0; }
    return 1;
}

long WayBoxesMinx(WayBoxes wb, int w) {
    return wb->a[w];
}
long WayBoxesMiny(WayBoxes wb, int w) {
    return wb->b[w];
}
long WayBoxesMaxx(WayBoxes wb, int w) {
    return wb->c[w];
}
long WayBoxesMaxy(WayBoxes wb, int w) {
    return wb->d[w];
}


long WayBoxesQuadtree(WayBoxes wb, int w, double buf, size_t mxl) {
    //struct wayBoxImpl *wb = wbp;
    if (w>=wb->mxw) {
        return;
    }
    if (wb->a[w] == 2000000000) {
        return -1;
    }
    long q= calcQuadtree(wb->a[w],wb->b[w],wb->c[w],wb->d[w],mxl, buf);
    if (q<0) {
        printf("??? %d %d %d %d %d => %ld\n",w,wb->a[w],wb->b[w],wb->c[w],wb->d[w],q);
        q=0;
    }
    return q;
}

void WayBoxesFree(WayBoxes wb) {
    //struct wayBoxImpl *wb = wbp;
    free(wb->a);
    free(wb->b);
    free(wb->c);
    free(wb->d);
    free(wb);
}

int WayBoxesNext(WayBoxes wb, int w) {
    //struct wayBoxImpl *wb = wbp;
    if (w>=wb->mxw) {
        return;
    }
    for ( ; w < wb->mxw; ++w) {
        if (wb->a[w] != 2000000000) {
            return w;
        }
    }
    return wb->mxw;
}

*/
import "C"

import (
    "github.com/jharris2268/osmquadtree/quadtree"
    "fmt"
)


type wayBboxTileCgo struct {
    boxes C.WayBoxes
    length int
}

func (wbt *wayBboxTileCgo) Expand(i int, ln int64, lt int64) bool {
    if wbt.boxes==nil {
        panic("wbt.boxes==nil")
    }
    if C.WayBoxesExpand(wbt.boxes, C.int(i), C.int(ln), C.int(lt))==C.int(1) {
        wbt.length++
        return true
    }
    return false
}
func (wbt *wayBboxTileCgo) Get(i int) quadtree.Bbox {
    ci:=C.int(i)
    if C.WayBoxesHasValue(wbt.boxes,ci)==C.int(0) {
        return *quadtree.NullBbox()
    }
    return quadtree.Bbox{
            int64(C.WayBoxesMinx(wbt.boxes,ci)), 
            int64(C.WayBoxesMiny(wbt.boxes,ci)), 
            int64(C.WayBoxesMaxx(wbt.boxes,ci)), 
            int64(C.WayBoxesMaxy(wbt.boxes,ci)),
    }
}

func (wbt *wayBboxTileCgo) CalcQuadtree(buf float64, mxl uint) (int, quadtreeTile) {
    
    if wbt.boxes==nil {
        panic("wbt.boxes==nil")
    }
    
    ll:=0
    res:=newQuadtreeTile()
    for i:=C.WayBoxesNext(wbt.boxes,C.int(0)); i < C.int(tileSz); i=C.WayBoxesNext(wbt.boxes,i+1) {
        qt := C.WayBoxesQuadtree(wbt.boxes,i,C.double(buf),C.size_t(mxl))
        ll++
        if qt<0 {
            fmt.Println("??",i,qt)
        } else {
            res.Set(int(i), quadtree.Quadtree(qt))
        }
    }
    C.WayBoxesFree(wbt.boxes)
    wbt.boxes=nil
    return ll,res
}

func (wbt *wayBboxTileCgo) Len() int {
    return wbt.length
}

func newWayBboxTileCgo() wayBboxTile {
    return &wayBboxTileCgo{C.WayBoxesInit(C.int(tileSz)),0}
}

