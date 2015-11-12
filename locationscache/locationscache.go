// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package locationscache

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/quadtree"
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
	case "leveldb":
		return OpenLevelDbLocationsCache(prfx)
	case "null":
		return OpenNullLocationsCache(prfx)
	case "pbf":
		return OpenPbfIndexLocationsCache(prfx)
	}
	return nil, errors.New(fmt.Sprintf("%q not a reconised lctype", lctype))

}

func GetLastState(prfx string, lctype string) (int64, error) {
	switch lctype {
	case /*"flat",*/ "null", "pbf":
		return GetLastStateFileList(prfx)
	case "leveldb":
		return GetLastStateLevelDb(prfx)

	}
	return -1, errors.New(fmt.Sprintf("%q not a reconised lctype", lctype))
}

func MakeLocationsCache(inBlocks []chan elements.ExtendedBlock, lctype string, infn string, prfx string, endDate elements.Timestamp, state int64) error {
	switch lctype {

	case "leveldb":
		return MakeLocationsCacheLevelDb(inBlocks, infn, prfx, int64(endDate), state)
	case "null":
		return MakeLocationsCacheNull(inBlocks, infn, prfx, endDate, state)
	case "pbf":
		return MakeLocationsCachePbfIndex(inBlocks, infn, prfx, endDate, state)

	}

	return errors.New(fmt.Sprintf("%q not a reconised lctype", lctype))
}

func GetCacheSpecs(prfx string, lctype string) ([]IdxItem, []quadtree.Quadtree, error) {
	switch lctype {
	case "null", "pbf":
		return GetCacheSpecsFileList(prfx)
	case "leveldb":
		return GetCacheSpecsLevelDb(prfx)
	}
	return nil, nil, errors.New(fmt.Sprintf("%q not a reconised lctype", lctype))
}

type UpdateSettings struct {
	SourcePrfx     string
	DiffsLocation  string
	InitialState   int64
	RoundTime      bool
	LocationsCache string
    QuadtreeTuple  bool
}

const DefaultSource = string("http://planet.openstreetmap.org/replication/day/")

func GetUpdateSettings(prfx string) (UpdateSettings, error) {
	fl, err := os.Open(prfx + "settings.json")
	if err != nil {
		_, _, err2 := GetCacheSpecsLevelDb(prfx)
		if err2 == nil {
			return UpdateSettings{"", "", 0, true, "leveldb",false}, nil
		} else {
			return UpdateSettings{}, err
		}
	}

	defer fl.Close()
	us := UpdateSettings{}
	err = json.NewDecoder(fl).Decode(&us)
	if err != nil {
		return UpdateSettings{}, err
	}

	return us, nil
}

func WriteUpdateSettings(prfx string, us UpdateSettings) error {
	fl, err := os.Create(prfx + "settings.json")
	if err != nil {
		return err
	}
	defer fl.Close()

	err = json.NewEncoder(fl).Encode(&us)
	if err != nil {
		return err
	}

	return nil
}
