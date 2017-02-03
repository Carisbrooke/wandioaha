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
	char outbuff[BUF_OUT_SIZE];
	int avail_out;			//space remained
	char *next_out;			//ptr to next avail free space
	iow_t *child;
	enum err_t err;
	int inoffset;
	int compression;
};

char *compressors[] = {"blosclz", "lz4", "lz4hc", "snappy", "zlib", "zstd"};

extern iow_source_t blosc_wsource; 

#define DATA(iow) ((struct bloscw_t *)((iow)->data))
#define min(a,b) ((a)<(b) ? (a) : (b))

iow_t *blosc_wopen(iow_t *child, int compress_type, int compress_level);
{
	iow_t *iow;
	int rv = 0;
	int num_threads = NUM_THREADS;

	if (!child)
		return NULL;
	if (compress_type < 6)
	{
                printf("Wrong compressor type: %d\n", compress_type);
                return NULL;
	}

	iow = malloc(sizeof(iow_t));
	iow->source = &blosc_wsource;
	iow->data = malloc(sizeof(struct bloscw_t));

        blosc_init();

        blosc_set_nthreads(num_threads);
        printf("Using %d threads \n", num_threads);
	//6 is magic constant
        rv = blosc_set_compressor(compressors[compress_type - 6]);
        if (rv < 0)
        {
                printf("Error setting compressor %s\n", compressors[compress_type - 6]);
                return NULL;
        }
        printf("Using %s compressor\n", compressors[compress_type - 6]);

	DATA(iow)->child = child;
	DATA(iow)->err = ERR_OK;
	//repu1sion:store compression
	DATA(iow)->compression = compress_level;
	//init next_out ptr
	DATA(iow)->next_out = DATA(iow)->outbuff;
	DATA(iow)->avail_out = sizeof(DATA(iow)->outbuff);

	return iow;
}

//we need blosc_compress(), then wandio_wwrite(), get rid of zstream etc
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
	const char *dta = buffer;
	size_t remained = len;
	int csize;
	int isize = (int)len;
	int osize = BUF_OUT_SIZE;

	//somehow occasionally we have blosc_wwrite() call with 0 len
	if (!len)
	{
		return 0;
	}
	
	//---------------
	//DATA(iow)->strm.next_in = (Bytef*)buffer;  
	//DATA(iow)->strm.avail_in = len;

	while (DATA(iow)->err == ERR_OK && remained > 0) 
	{	//repu1sion: when buffer is full we write it to file and set next_out, avail_out again
		while (DATA(iow)->avail_out <= FREE_SPACE_LIMIT)
		{
			int bytes_written = wandio_wwrite(DATA(iow)->child, (char *)DATA(iow)->outbuff, 
							  sizeof(DATA(iow)->outbuff) - DATA(iow)->avail_out);
			printf("[wandio] %s() writing almost full buffer \n", __func__);
			if (bytes_written <= 0) 
			{
				DATA(iow)->err = ERR_ERROR;
				if (remained != (size_t)len)
					return len - remained;//return length we processed
				return -1;
			}
			DATA(iow)->next_out = DATA(iow)->outbuff;
			DATA(iow)->avail_out = sizeof(DATA(iow)->outbuff);
		}
		//repu1sion: do the blosc compression on buffer
		csize = blosc_compress(DATA(iow)->compression,0,sizeof(char), isize, dta, DATA(iow)->next_out, osize);
		//repu1sion: manage all avail_in, avail_out, next_out vars.
		if ((unsigned int)csize > DATA(iow)->avail_out)
		{
			//XXX - create mechanism to prevent overflow? write buffer with wandio_write() here?
			printf("[wandio] <error> overflow! compressed data size: %d , space in buffer: %u \n",
				csize, DATA(iow)->avail_out);
			DATA(iow)->err = ERR_ERROR;
			return -1;
		}
		remained -= isize;			//repu1sion: it should be 0, anyway
		DATA(iow)->avail_out -= csize;		//repu1sion: decrease available space in output buffer
		DATA(iow)->next_out += csize;		//repu1sion: move pointer forward
		printf("[wandio] %s() input data size: %d , compressed data size: %d , space in buffer: %u \n",
			 __func__, isize, csize, DATA(iow)->avail_out);
	}
	/* Return the number of bytes compressed */
	return len - remained;	//repulsion: len - 0 = len, so we mostly return len here
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
	wandio_wwrite(DATA(iow)->child, (char *)DATA(iow)->outbuff, sizeof(DATA(iow)->outbuff) - DATA(iow)->strm.avail_out);
	printf("[wandio] %s() writing buffer with size: %lu \n", __func__, sizeof(DATA(iow)->outbuff) - DATA(iow)->strm.avail_out);

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
