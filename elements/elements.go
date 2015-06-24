// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

// Package elements defines the types used for storing osmquadtree entities
// (Element) and blocks (Block)
package elements

import (
	"github.com/jharris2268/osmquadtree/quadtree"
	"time"
    "strings"
    "fmt"
)

// ElementType shows which type of openstreetmap entity is represented by
// an Element. An element of type Geometry is a node, way or relation 
// which has been converted to a WKT style Point, Linestring or Polygon
type ElementType int

const (
	Node ElementType = iota
	Way
	Relation
	Geometry
)

func (et ElementType) String() string {
	switch et {
	case Node:
		return "Node"
	case Way:
		return "Way"
	case Relation:
		return "Relation"
	case Geometry:
		return "Geometry"
	}
	return "??"
}

// ChangeType is equivalent the delete, modify and create blocks found
// int .pbfc replication files
type ChangeType int

const (
    
	Normal ChangeType = iota //An object not part of a change block
	Delete //An object to delete from the source block
	Remove //An object to remove from the source block and place in another block
	Unchanged //An object which has been moved from another source block
	Modify //An updated, existing object
	Create //A new object
)

func ChangeTypeString(ct ChangeType) string {
	switch ct {
	case Normal:
		return "Normal"
	case Delete:
		return "Delete"
	case Remove:
		return "Remove"
	case Unchanged:
		return "Unchgd"
	case Modify:
		return "Modify"
	case Create:
		return "Create"
	}
	return "??"
}
func (ct ChangeType) String() string {
	return ChangeTypeString(ct)
}

type Ref int64

func (r Ref) String() string {
    if r>0xffffffffffff {
        t := r<<61
        return fmt.Sprintf("%d %8d", t, r&0xffffffffffff)
    }
    return fmt.Sprintf("%10d", r)
}

type Timestamp int64

func (t Timestamp) String() string {
	return time.Unix(int64(t), 0).UTC().Format("2006-01-02T15:04:05")
}
func (t Timestamp) DateString() string {
	return time.Unix(int64(t), 0).UTC().Format("20060102")
}

func (t Timestamp) FileString(round bool) string {
    if round {
        return t.DateString()
    }
    return strings.Replace(t.String(), ":","-",-1)
}

// ReadDateString parses s as either short date form "20060102" or full
// timestamp 2006-01-02T15:04:05
func ReadDateString(s string) (Timestamp, error) {
	a, err := time.Parse("2006-01-02T15:04:05", s)
	if err == nil {
		return Timestamp(a.Unix()), nil
	}
	a, err = time.Parse("20060102", s)
	if err != nil {
		return 0, err
	}
	return Timestamp(a.Unix()), nil
}

// Element is be base type for storing openstreetmap entities. 
type Element interface {
	Type()          ElementType
	Id()            Ref
	ChangeType()    ChangeType
    
    // Pack() returns a []byte contiained the serialized element data.
    // See UnpackElement to convert this back to an Element
	Pack()          []byte
	String()        string
}

type Tags interface {
	Len()           int
	Key(int)        string
	Value(int)      string

	Pack()          []byte
}

// Info contains the user and changeset metadata for an osm entity
type Info interface {
	Version()       int64
	Timestamp()     Timestamp
	Changeset()     Ref
	Uid()           int64
	User()          string
    Visible()       bool
	Pack() []byte
}

// LonLat provide a node's location
type LonLat interface {
	Lon() int64
	Lat() int64
}

// Refs provide a way's node refs
type Refs interface {
	Len()       int
	Ref(int)    Ref
}

// Members provides a relation's members
type Members interface {
	Len()           int
	MemberType(int) ElementType
	Ref(int)        Ref
	Role(int)       string
}

type Quadtreer interface {
	Quadtree()      quadtree.Quadtree
}


type FullElement interface {
	Element
	Tags() Tags
	Info() Info
	Quadtreer

	SetQuadtree(quadtree.Quadtree)
	SetChangeType(ChangeType)
}

type FullNode interface {
	FullElement
	LonLat
}

type FullWay interface {
	FullElement
	Refs
}

type FullRelation interface {
	FullElement
	Members
}

type PackedGeometry interface {
	FullElement
    
    // GeometryData returns a []byte containing the protocol buffers
    // serialized geometry data
	GeometryData() []byte 
}
