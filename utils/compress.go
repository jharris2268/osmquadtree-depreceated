// Copyright 2015 James Harris. All rights reserved.
// Use of this source code is governed by the GNU General Public License
// version 3 (or any later version), both of which can be found in the
// LICENSE file.

// +build !windows

package utils

/*
#cgo pkg-config: zlib

#include <stdlib.h>
#include "zlib.h"

struct blob {
    char* Data;
    int Len;
};

typedef struct blob Blob;

Blob Decompress(char* src, int src_len, int dst_len) {
    Blob dst;
    dst.Data=malloc(dst_len);
    
    z_stream infstream;
    infstream.zalloc = Z_NULL;
    infstream.zfree = Z_NULL;
    infstream.opaque = Z_NULL;
    // setup "b" as the input and "c" as the compressed output
    infstream.avail_in = src_len; // size of input
    infstream.next_in = src; // input char array
    infstream.avail_out = dst_len; // size of output
    infstream.next_out = dst.Data; // output char array
     
    // the actual DE-compression work.
    inflateInit(&infstream);
    inflate(&infstream, Z_NO_FLUSH);
    inflateEnd(&infstream);
    
    dst.Len = infstream.total_out;
    
    return dst;
}


Blob Compress(char* src, int src_len) {
    Blob dst;
    dst.Data=malloc(src_len+100);
    
    z_stream defstream;
    defstream.zalloc = Z_NULL;
    defstream.zfree = Z_NULL;
    defstream.opaque = Z_NULL;
    // setup "a" as the input and "b" as the compressed output
    defstream.avail_in = src_len; // size of input, string + terminator
    defstream.next_in = src; // input char array
    
    defstream.avail_out = src_len+100; // size of output
    defstream.next_out = dst.Data; // output char array
    
    // the actual compression work.
    
    deflateInit(&defstream,Z_DEFAULT_COMPRESSION);
    deflate(&defstream, Z_FINISH);
    deflateEnd(&defstream);
    
    dst.Len = defstream.total_out;
    
    return dst;
}



*/
import "C"

import (
	//"bytes"
	//"compress/zlib"
	"errors"
	"fmt"
	"io"
    "unsafe"

	//zlib "github.com/jharris2268/osmquadtree/utils/cgzip"
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
    
    srcs,srci := C.CString(string(comp)), C.int(len(comp))
    dst := C.Decompress(srcs,srci,C.int(size))
    ans := C.GoBytes(unsafe.Pointer(dst.Data), C.int(dst.Len))
    C.free(unsafe.Pointer(srcs))
    C.free(unsafe.Pointer(dst.Data))
    return ans,nil
    
    
    /*
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
	return res, nil*/
}

func Compress(data []byte) ([]byte, error) {
    
    
    
    srcs,srci := C.CString(string(data)), C.int(len(data))
    dst := C.Compress(srcs,srci)
    ans := C.GoBytes(unsafe.Pointer(dst.Data), C.int(dst.Len))
    C.free(unsafe.Pointer(srcs))
    C.free(unsafe.Pointer(dst.Data))
    return ans,nil
    
    /*
	var compressedBlob bytes.Buffer
	zlibWriter := zlib.NewWriter(&compressedBlob)
	l := 0
	for l < len(data) {
		p, _ := zlibWriter.Write(data[l:])
		l += p

	}
	zlibWriter.Flush()
	zlibWriter.Close()
	return compressedBlob.Bytes(), nil*/
}
