/*
 *
 * Copyright (c) 2007-2016 The University of Waikato, Hamilton, New Zealand.
 * All rights reserved.
 *
 * This file is part of libwandio.
 *
 * This code has been developed by the University of Waikato WAND
 * research group. For further information please see http://www.wand.net.nz/
 *
 * libwandio is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * libwandio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 */


#include "config.h"
#include <zlib.h>
#include "wandio.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <blosc.h>

//repu1sion -----
#define NUM_THREADS 4
#define COMPRESSOR "zlib"	//XXX - add posibility to change compressor
#define BUF_OUT_SIZE 1024*1024
#define FREE_SPACE_LIMIT 100*1024 	//if we have less free space in buffer than 100Kb - dump it
//---------------

/* Libwandio IO module implementing a zlib writer */

enum err_t {
	ERR_OK	= 1,
	ERR_EOF = 0,
	ERR_ERROR = -1
};

struct bloscw_t {
	z_stream strm;
	Bytef outbuff[BUF_OUT_SIZE];
	iow_t *child;
	enum err_t err;
	int inoffset;
	//repu1sion: extending struct
	unsigned long check; //var for storing and recalculating crc32 for gzip footer
	unsigned long ulen;  //uncompressed data length
	int compression;
};

extern iow_source_t blosc_wsource; 

#define DATA(iow) ((struct bloscw_t *)((iow)->data))
#define min(a,b) ((a)<(b) ? (a) : (b))

//repu1sion: macroses from pigz code
/* put a 4-byte integer into a byte array in LSB order or MSB order */
#define PUT2L(a,b) (*(a)=(b)&0xff,(a)[1]=(b)>>8)
#define PUT4L(a,b) (PUT2L(a,(b)&0xffff),PUT2L((a)+2,(b)>>16))

//gz header
//gz_header gzheader;

/*
    GZIP FILE FORMAT
    -------------------
    - a 10-byte header, containing a magic number (1f 8b), a version number and a timestamp
    - optional extra headers, such as the original file name,
    - a body, containing a DEFLATE-compressed payload
    - an 8-byte footer, containing a CRC-32 checksum and the length of the original uncompressed data, modulo 2^32.[2]

    /mnt/raw/gdwk/libtrace$ hexdump -C real.gz 
    00000000  1f 8b 08 00 00 00 00 00  04 03 b5 53 cf 6b 13 41  |...........S.k.A|
    00000010  14 7e b3 6e ac ad 49 69  b5 44 41 2b d6 88 d6 83  |.~.n..Ii.DA+....|
*/

static unsigned long write_gzip_header(iow_t *child, int compression)
{
	unsigned long len = 0;		//header could have different length (10 bytes if no extra)
	unsigned char head[30] = {0};
	int bytes_written = 0;

	//forming 10 bytes gzip header
        head[0] = 31;			//magic number 0x1f 0x8b (2 bytes)
        head[1] = 139;
        head[2] = 8;                	/* deflate */
	head[3] = 0;			//don't keep filename in gzip header
	head[4] = 0; head[5] = 0; head[6] = 0; head[7] = 0;	//timestamp (4 bytes)
//        head[3] = g.name != NULL ? 8 : 0;
//        PUT4L(head + 4, g.mtime);	//timestamp (4 bytes)
        head[8] = compression >= 9 ? 2 : (compression == 1 ? 4 : 0);
        head[9] = 3;                	/* unix os */
//        writen(g.outd, head, 10);
        len = 10;

//let's omit filename stored in header now
/*
        if (g.name != NULL)
            writen(g.outd, (unsigned char *)g.name, strlen(g.name) + 1);
        if (g.name != NULL)
            len += strlen(g.name) + 1;
*/

	//writing header to file
	bytes_written = wandio_wwrite(child, head, len);
	printf("[wandio] %s() writing header\n", __func__);
	if (bytes_written <= 0) 
	{
		len = 0;
		printf("[wandio] %s() ERROR writing header!\n", __func__);
	}
	else
		printf("[wandio] %s() %lu bytes of header wrote to file successfully\n", __func__, len);

	return len;
}

//4 bytes CRC32 value of original uncompressed data , 4 bytes of uncompressed length
//@check - crc32 of uncompressed data
//@ulen - length of uncompressed data
static unsigned long write_gzip_footer(iow_t *child, unsigned long l_check, unsigned long l_ulen)
{
	unsigned long len = 8;
	unsigned char tail[8] = {0}; //for gzip have a fixed 8 bytes of footer
	int bytes_written = 0;

        PUT4L(tail, l_check);
        PUT4L(tail + 4, l_ulen);

	//writing header to file
	bytes_written = wandio_wwrite(child, tail, len);
	printf("[wandio] %s() writing footer with crc: 0x%lx and length: %lu\n", __func__, l_check, l_ulen);
	if (bytes_written <= 0) 
	{
		len = 0;
		printf("[wandio] %s() ERROR writing footer!\n", __func__);
	}
	else
		printf("[wandio] %s() %lu bytes of header wrote to file successfully\n", __func__, len);

	return len;
}


iow_t *blosc_wopen(iow_t *child, int compress_level)
{
	iow_t *iow;
	if (!child)
		return NULL;
	iow = malloc(sizeof(iow_t));
	iow->source = &blosc_wsource;
	iow->data = malloc(sizeof(struct bloscw_t));

	//repu1sion -----
	int rv = 0;
	int len = 0;
	int num_threads = NUM_THREADS;

        blosc_init();

        blosc_set_nthreads(num_threads);
        printf("Using %d threads \n", num_threads);

        rv = blosc_set_compressor(COMPRESSOR);
        if (rv < 0)
        {
                printf("Error setting compressor %s\n", COMPRESSOR);
                return NULL;
        }
        printf("Using %s compressor\n", COMPRESSOR);
	//---------------

	DATA(iow)->child = child;

	DATA(iow)->strm.next_in = NULL;
	DATA(iow)->strm.avail_in = 0;
	DATA(iow)->strm.next_out = DATA(iow)->outbuff;
	DATA(iow)->strm.avail_out = sizeof(DATA(iow)->outbuff);
	DATA(iow)->strm.zalloc = Z_NULL;
	DATA(iow)->strm.zfree = Z_NULL;
	DATA(iow)->strm.opaque = NULL;
	DATA(iow)->err = ERR_OK;
	//repu1sion:store compression
	DATA(iow)->compression = compress_level;

	deflateInit2(&DATA(iow)->strm, 
			compress_level,	/* Level */
			Z_DEFLATED, 	/* Method */
			15 | 16, 	/* 15 bits of windowsize, 16 == use gzip header */
			9,		/* Use maximum (fastest) amount of memory usage */
			Z_DEFAULT_STRATEGY
		);

	//writing header
	len = write_gzip_header(DATA(iow)->child, compress_level);
	if (!len)
		return NULL;

#if 0
	gzheader.text = 0;
	gzheader.time = 0;
	gzheader.os = 3;
	gzheader.hcrc = 0;
	//XXX - name?

	//deflateSetHeader(z_streamp strm, gz_headerp head);
	printf("setting gzip header\n");
	rv = deflateSetHeader(&DATA(iow)->strm, &gzheader);
	if (rv)
		printf("failed to set header. rv: %d \n", rv);
	else
		printf("header set successfully\n");
#endif

	return iow;
}

static int64_t blosc_wwrite(iow_t *iow, const char *buffer, int64_t len)
{
	if (DATA(iow)->err == ERR_EOF) {
		return 0; /* EOF */
	}
	if (DATA(iow)->err == ERR_ERROR) {
		return -1; /* ERROR! */
	}

	printf("[wandio] %s() ENTER. buf: %p , len: %ld \n", __func__, buffer, len);

	//repu1sion -----
	int csize;
	const char *dta = buffer;
	int isize = (int)len;
	int osize = BUF_OUT_SIZE;

	//somehow occasionally we have blosc_wwrite() call with 0 len
	if (len)
	{
		//crc
		DATA(iow)->check = crc32(0L, Z_NULL, 0);
		//uncompressed length
		DATA(iow)->ulen = 0;
	}
	
	//---------------
	DATA(iow)->strm.next_in = (Bytef*)buffer;  
	DATA(iow)->strm.avail_in = len;

	while (DATA(iow)->err == ERR_OK && DATA(iow)->strm.avail_in > 0) 
	{	//repu1sion: when buffer is full we write it to file and set next_out, avail_out again
		while (DATA(iow)->strm.avail_out <= FREE_SPACE_LIMIT)
		{
			int bytes_written = wandio_wwrite(DATA(iow)->child, (char *)DATA(iow)->outbuff, sizeof(DATA(iow)->outbuff));
			printf("[wandio] %s() writing full 1Mb buffer \n", __func__);
			if (bytes_written <= 0) 
			{
				DATA(iow)->err = ERR_ERROR;
				if (DATA(iow)->strm.avail_in != (uint32_t)len)
					return len-DATA(iow)->strm.avail_in;
				return -1;
			}
			DATA(iow)->strm.next_out = DATA(iow)->outbuff;
			DATA(iow)->strm.avail_out = sizeof(DATA(iow)->outbuff);
		}
		//update crc on uncompressed data
		DATA(iow)->check = crc32(DATA(iow)->check, DATA(iow)->strm.next_in, DATA(iow)->strm.avail_in);
		//repu1sion: do the blosc compression on buffer
		csize = blosc_compress(DATA(iow)->compression, 0, sizeof(char), isize, dta, DATA(iow)->strm.next_out, osize);
		//repu1sion: manage all avail_in, avail_out, next_out vars.
		if ((unsigned int)csize > DATA(iow)->strm.avail_out)
		{
			printf("[wandio] <error> overflow! compressed data size: %d , space in buffer: %u \n",
				csize, DATA(iow)->strm.avail_out);
			DATA(iow)->err = ERR_ERROR;
			return -1;
		}
		DATA(iow)->strm.avail_in -= isize;	//repu1sion: it should be 0, anyway
		DATA(iow)->strm.avail_out -= csize;	//repu1sion: decrease available space in output buffer
		DATA(iow)->strm.next_out += csize;	//repu1sion: move pointer forward
		DATA(iow)->ulen += isize;
		printf("[wandio] %s() input data size: %d , compressed data size: %d , space in buffer: %u \n",
			 __func__, isize, csize, DATA(iow)->strm.avail_out);
		printf("[wandio] %s() crc : %lx , uncompressed data size: %lu \n", __func__, DATA(iow)->check, DATA(iow)->ulen);
		
#if 0
		/* Decompress some data into the output buffer */
		int err=deflate(&DATA(iow)->strm, 0);
		switch(err) {
			case Z_OK:
				DATA(iow)->err = ERR_OK;
				break;
			default:
				DATA(iow)->err = ERR_ERROR;
		}
#endif
	}
	/* Return the number of bytes compressed */
	return len-DATA(iow)->strm.avail_in;	//repulsion: len - 0 = len, so we mostly return len here
}

//XXX - maybe we need to do blosc_compress() with rest of data here too
static void blosc_wclose(iow_t *iow)
{
	printf("[wandio] %s() \n", __func__);
#if 0
	int res;
	
	while (1) {
		//XXX - replace it
		res = deflate(&DATA(iow)->strm, Z_FINISH);

		if (res == Z_STREAM_END)
			break;
		if (res == Z_STREAM_ERROR) {
			fprintf(stderr, "Z_STREAM_ERROR while closing output\n");
			break;
		}
	
		wandio_wwrite(DATA(iow)->child, 
				(char*)DATA(iow)->outbuff,
				sizeof(DATA(iow)->outbuff)-DATA(iow)->strm.avail_out);
		DATA(iow)->strm.next_out = DATA(iow)->outbuff;
		DATA(iow)->strm.avail_out = sizeof(DATA(iow)->outbuff);
	}
#endif

	//XXX - need to do blosc_compress to rest of data in input buffer?
	deflateEnd(&DATA(iow)->strm);
	wandio_wwrite(DATA(iow)->child, (char *)DATA(iow)->outbuff, sizeof(DATA(iow)->outbuff) - DATA(iow)->strm.avail_out);
	printf("[wandio] %s() writing buffer with size: %lu \n", __func__, sizeof(DATA(iow)->outbuff) - DATA(iow)->strm.avail_out);

	//write footer
	write_gzip_footer(DATA(iow)->child, DATA(iow)->check, DATA(iow)->ulen);

	wandio_wdestroy(DATA(iow)->child);
	free(iow->data);
	free(iow);

	//repu1sion -----
	blosc_destroy();
	//---------------
}

iow_source_t blosc_wsource = {
	"bloscw",
	blosc_wwrite,
	blosc_wclose
};
