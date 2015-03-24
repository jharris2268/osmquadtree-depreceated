// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

// +build windows

package locationscache

import (
    "errors"
    
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
)

var ldbNotDefined = errors.New("leveldb not availble on windows")

func MakeLocationsCacheLevelDb(
        inputChans []chan elements.ExtendedBlock,
        inputfn string, prfx string, enddate int64, state int64) error {
    
    return ldbNotDefined
}


func OpenLevelDbLocationsCache(prfx string) (LocationsCache,error) {
    return nil, ldbNotDefined
}
func GetCacheSpecsLevelDb(prfx string) ([]IdxItem, []quadtree.Quadtree, error) {
    
    return nil,nil,ldbNotDefined
}

func GetLastStateLevelDb(prfx string) (int64, error) {
    
    return -1,ldbNotDefined
}
