// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package xmlread

import (
	"compress/gzip"
	"encoding/xml"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jharris2268/osmquadtree/elements"
)

//<node id="1850656772" version="1" timestamp="2012-08-03T22:03:33Z" uid="30525" user="The Maarssen Mapper" changeset="12603556" lat="51.2796351" lon="0.3027582"

type tempXmlObj struct {
	changeType elements.ChangeType

	objectType elements.ElementType
	id         elements.Ref

	vs int64
	ts elements.Timestamp
	cs elements.Ref
	ui int64
	us string

	keys []string
	vals []string

	lon int64
	lat int64

	refs  []elements.Ref
	types []elements.ElementType
	roles []string
}

func ftoi(f float64) int64 {
	if f > 0 {
		return int64(f + 0.5)
	}
	return int64(f - 0.5)
}

func ReadOsmXmlFile(filename string) (<-chan elements.Element, error) {
	elems := make(chan elements.Element)

	go func() {
		for bl := range ReadXmlBlocks(filename) {
			for i := 0; i < bl.Len(); i++ {
				elems <- bl.Element(i)
			}
		}

		close(elems)
	}()
	return elems, nil
}

func ReadXmlBlocks(filename string) <-chan elements.ExtendedBlock {

	out := make(chan elements.ExtendedBlock)
	go func() {

		var reader io.Reader
		var err error
		reader, err = os.Open(filename)
		if err != nil {
			panic(err.Error())
		}

		if strings.HasSuffix(filename, ".gz") {
			reader, err = gzip.NewReader(reader)
			if err != nil {
				panic(err.Error())
			}
		}
		decoder := xml.NewDecoder(reader)

		tt := tempXmlObj{}
		ts := make(elements.ByElementId, 0, 8000)
		ii := 0

		err = func() error {
			for {

				token, err := decoder.Token()
				if err == io.EOF {
					return nil
				}

				if err != nil {
					println("??", err.Error())
					return nil
				}

				switch tok := token.(type) {
				case xml.StartElement:

					switch tok.Name.Local {
					case "create":
						tt.changeType = 5
					case "modify":
						tt.changeType = 4
					case "delete":
						tt.changeType = 1
					case "node", "way", "relation":

						tt.id, tt.vs, tt.ts, tt.cs, tt.ui = elements.Ref(0), int64(0), elements.Timestamp(0), elements.Ref(0), int64(0)
						tt.us = ""
						tt.objectType = 0
						tt.keys = make([]string, 0, 25)
						tt.vals = make([]string, 0, 25)
						switch tok.Name.Local {
						case "way":
							tt.refs = make([]elements.Ref, 0, 25)
						case "relation":
							tt.refs = make([]elements.Ref, 0, 25)
							tt.types = make([]elements.ElementType, 0, 25)
							tt.roles = make([]string, 0, 25)
						}
						tt.lat = -900000000
						tt.lon = -1800000000

						for _, attr := range tok.Attr {
							switch attr.Name.Local {
							case "id":
								id, _ := strconv.ParseInt(attr.Value, 10, 64)
								tt.id = elements.Ref(id)
							case "version":
								tt.vs, _ = strconv.ParseInt(attr.Value, 10, 64)
							case "timestamp":
								t, _ := time.Parse(time.RFC3339, attr.Value)
								tt.ts = elements.Timestamp(t.Unix())
							case "changeset":
								cs, _ := strconv.ParseInt(attr.Value, 10, 64)
								tt.cs = elements.Ref(cs)
							case "uid":
								tt.ui, _ = strconv.ParseInt(attr.Value, 10, 64)
							case "user":
								tt.us = attr.Value

							case "lon":
								ln, _ := strconv.ParseFloat(attr.Value, 64)
								tt.lon = ftoi(ln * 10000000)
							case "lat":
								lt, _ := strconv.ParseFloat(attr.Value, 64)
								tt.lat = ftoi(lt * 10000000)
							}
						}

					case "nd":
						for _, attr := range tok.Attr {
							if attr.Name.Local == "ref" {
								ref, _ := strconv.ParseInt(attr.Value, 10, 64)
								tt.refs = append(tt.refs, elements.Ref(ref))
							}
						}
					case "member":
						//member := TempMem{}
						for _, attr := range tok.Attr {
							switch attr.Name.Local {
							case "type":
								switch attr.Value {
								case "node":
									tt.types = append(tt.types, elements.Node)
								case "way":
									tt.types = append(tt.types, elements.Way)
								case "relation":
									tt.types = append(tt.types, elements.Relation)
								}
							case "role":
								tt.roles = append(tt.roles, attr.Value)
							case "ref":
								ref, _ := strconv.ParseInt(attr.Value, 10, 64)
								tt.refs = append(tt.refs, elements.Ref(ref))
							}
						}

					case "tag":

						for _, attr := range tok.Attr {
							if attr.Name.Local == "k" {
								tt.keys = append(tt.keys, attr.Value)
							} else if attr.Name.Local == "v" {
								tt.vals = append(tt.vals, attr.Value)
							}
						}

					case "osmChange":
						// pass
					default:
						println("unhandled XML tag ", tok.Name.Local, " in OSC")
					}
				case xml.EndElement:

					switch tok.Name.Local {
					case "node":
						info := elements.MakeInfo(tt.vs, tt.ts, tt.cs, tt.ui, tt.us, tt.changeType != elements.Delete)
						tags := elements.MakeTags(tt.keys, tt.vals)

						ts = append(ts, elements.MakeNode(tt.id, info, tags, tt.lon, tt.lat, 0, tt.changeType))
					case "way":
						info := elements.MakeInfo(tt.vs, tt.ts, tt.cs, tt.ui, tt.us, tt.changeType != elements.Delete)
						tags := elements.MakeTags(tt.keys, tt.vals)
						//data := osmread.MakeSimpleObjWayNodes(tt.refs)
						ts = append(ts, elements.MakeWay(tt.id, info, tags, tt.refs, 0, tt.changeType))
					case "relation":
						info := elements.MakeInfo(tt.vs, tt.ts, tt.cs, tt.ui, tt.us, tt.changeType != elements.Delete)
						tags := elements.MakeTags(tt.keys, tt.vals)
						//data := osmread.MakeSimpleObjRelMembers(tt.refs, tt.types, tt.roles)
						ts = append(ts, elements.MakeRelation(tt.id, info, tags, tt.types, tt.refs, tt.roles, 0, tt.changeType))
					}
					if len(ts) == cap(ts) {
						out <- elements.MakeExtendedBlock(ii, ts, -1, 0, 0, nil)
						ii++
						ts = make(elements.ByElementId, 0, 8000)
					}
				}
			}
			return nil
		}()
		if err != nil {
			panic(err.Error())
		}
		if len(ts) > 0 {
			out <- elements.MakeExtendedBlock(ii, ts, -1, 0, 0, nil)
		}
		close(out)
	}()
	return out
}
