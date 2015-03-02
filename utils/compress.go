// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

package utils

import (
	"bytes"
	//"compress/zlib"
	"errors"
	"fmt"
	"io"
	zlib "github.com/jharris2268/osmquadtree/utils/cgzip"
)

func ReadBlock(file io.Reader, size uint64) ([]byte, error) {

	if size == 0 {
		return []byte{}, nil
	} else if size < 0 {
		return nil, errors.New(fmt.Sprintf("wtf: read size %d [readBlock]", size))
	}

	buffer := make([]byte, size)
	idx := uint64(0)
	for {
		cnt, err := file.Read(buffer[idx:])
		if err != nil {
			return nil, err
		}
		idx += uint64(cnt)
		if idx == size {
			break
		}
	}
	return buffer, nil
}

func Decompress(comp []byte, size uint64) ([]byte, error) {
	if size == 0 {
		return nil, errors.New("decompressed size is required but not provided")
	}
	zlibBuffer := bytes.NewBuffer(comp)
	zlibReader, err := zlib.NewReader(zlibBuffer)
	if err != nil {
		return nil, err
	}
	res, err := ReadBlock(zlibReader, size)
	if err != nil {
		return nil, err
	}
	zlibReader.Close()
	return res, nil
}

func Compress(data []byte) ([]byte, error) {

	var compressedBlob bytes.Buffer
	zlibWriter := zlib.NewWriter(&compressedBlob)
	l := 0
	for l < len(data) {
		p, _ := zlibWriter.Write(data[l:])
		l += p

	}
	zlibWriter.Flush()
	zlibWriter.Close()
	return compressedBlob.Bytes(), nil
}
