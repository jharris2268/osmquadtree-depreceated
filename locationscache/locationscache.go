// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package locationscache

import (
	
	"fmt"
	
    "errors"
	
	"github.com/jharris2268/osmquadtree/elements"
	
)

type IdxItem struct {
	Idx       int
	Filename  string
	Timestamp elements.Timestamp
	State     int64
}

type TilePair struct {
    File int
    Tile int
}
type TilePairSet map[TilePair]bool
type Locs map[elements.Ref]TilePair



type LocationsCache interface {
    FindTiles(inc <-chan int64) (Locs, TilePairSet)
    AddTiles(lcs Locs, idx IdxItem) int
    
    NumFiles() int
    FileSpec(i int) IdxItem
    
    Close()
}


func OpenLocationsCache(prfx string, lctype string) (LocationsCache, error) {
    switch lctype {
        //case "flat": return OpenFlatFileLocationsCache(prfx)
        case "leveldb": return OpenLevelDbLocationsCache(prfx)
        case "null": return OpenNullLocationsCache(prfx)
        case "pbf": return OpenPbfIndexLocationsCache(prfx)
    }
    return nil, errors.New(fmt.Sprintf("%q not a reconised lctype",lctype))

}

func GetLastState(prfx string, lctype string) (int64, error) {
    switch lctype {
        case /*"flat",*/"null","pbf": return GetLastStateFileList(prfx)
        case "leveldb": return GetLastStateLevelDb(prfx)
        
    }
    return -1, errors.New(fmt.Sprintf("%q not a reconised lctype",lctype))
}

func MakeLocationsCache(inBlocks []chan elements.ExtendedBlock, lctype string, infn string, prfx string, endDate elements.Timestamp, state int64) error {
    switch lctype {
    
        case "leveldb": return MakeLocationsCacheLevelDb(inBlocks, infn, prfx, int64(endDate), state)
        case "null": return MakeLocationsCacheNull(inBlocks, infn, prfx, endDate, state)
        case "pbf": return MakeLocationsCachePbfIndex(inBlocks, infn, prfx, endDate, state)
        
    }
    
    return errors.New(fmt.Sprintf("%q not a reconised lctype",lctype))
}
    
