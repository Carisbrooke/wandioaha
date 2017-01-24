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
#include "ahagz_api.h"

/* Libwandio IO module implementing a zlib writer */

enum err_t {
	ERR_OK	= 1,
	ERR_EOF = 0,
	ERR_ERROR = -1
};

struct zlibw_t {
	z_stream strm;
	Bytef outbuff[1024*1024];
	iow_t *child;
	enum err_t err;
	int inoffset;
	//repu1sion: extending struct
	int num_blocks;
	
};


extern iow_source_t zlib_wsource; 

#define DATA(iow) ((struct zlibw_t *)((iow)->data))
#define min(a,b) ((a)<(b) ? (a) : (b))

//repu1sion-----
#define INPUT_BUFFER_SIZE (128*1024) //131072
#define OUTPUT_BUFFER_SIZE (INPUT_BUFFER_SIZE + (5 * ((INPUT_BUFFER_SIZE + 4095)/4096)) + 30) //131262
#define OBUFF_OFFSET (10) //output buffer offset: for GZIP header
#define TIMEOUT (30*1000) //used in ahagz_api_waitstat()

//this struct is allocated for every block, size ~ 256Kb per block
typedef struct {
  aha_stream_t stream;
  char in_buff[INPUT_BUFFER_SIZE];
  char out_buff[OUTPUT_BUFFER_SIZE];
} block_info_t;

block_info_t *blocks = NULL;

static void pulseaha_cleanup(block_info_t *blocks, int num_blocks)
{
	int i;

	if(blocks)
	{
		for(i = 0; i < num_blocks; i++)
		{
      			ahagz_api_close(&blocks[i].stream);
    		}
		free(blocks);
	}
}

static int pulseaha_get_comp_channels(void)
{
	aha_stream_t as;
	uint32_t ncomp;

	if(ahagz_api_channels_available(&as, &ncomp, NULL))
	{
		fprintf(stderr, "Error calling ahagz_api_channels_available().  Is the driver loaded?\n");
	}
	else
		printf("[wandioaha] available channels: %u\n", ncomp);

	if(ncomp < 1)
	{
		fprintf(stderr, "No compression channels available.\n");
	}

	return ncomp;
}

iow_t *hwzlib_wopen(iow_t *child, int compress_level)
{
	iow_t *iow;
	if (!child)
		return NULL;
	iow = malloc(sizeof(iow_t));
	iow->source = &zlib_wsource;
	iow->data = malloc(sizeof(struct zlibw_t));

	//repu1sion -----
	int i;
	int num_chan = 0;

	(void)compress_level; //we don't use it in aha yet
	
	// Check for number of compression channels
	num_chan = pulseaha_get_comp_channels();
	if (!num_chan)
	{
		fprintf(stderr, "No available channels!\n");
		return NULL;
	}
	DATA(iow)->num_blocks = 4 * num_chan; //we split to blocks, so if 12 channels, then 48 blocks.

	// Initialize channels and buffers
	blocks = malloc(DATA(iow)->num_blocks * sizeof(*blocks));
	if(!blocks)
	{
		fprintf(stderr, "Error allocating memory.\n");
		pulseaha_cleanup(blocks, 0);
		return NULL;
	}

	for(i = 0; i < DATA(iow)->num_blocks; i++)
	{
		if(ahagz_api_open(&blocks[i].stream, 0, 0))//need to call ahagz_api_open() for every stream 48 times!!!
		{
			fprintf(stderr, "Error calling ahagz_api_open().\n");
			pulseaha_cleanup(blocks, i);
			return NULL;
		}
	}

	//---------------

	DATA(iow)->child = child;
	DATA(iow)->err = ERR_OK;

	return iow;
}

//this func usually gets called when we have a 1 Mb in buffer
static int64_t hwzlib_wwrite(iow_t *iow, const char *buffer, int64_t len)
{
	if (DATA(iow)->err == ERR_EOF)
		return 0; /* EOF */
	if (DATA(iow)->err == ERR_ERROR)
		return -1; /* ERROR! */

	int head = 0;		//counter for input data blocks
	int tail = 0;		//counter for output data blocks
	int submitted = 0;	//increase when add block to input, decrease when add to output
	uint32_t in_cnt;
	uint32_t out_cnt;
	int64_t rv;
	char *buf_p = (char *)buffer;
	size_t remained = len;
	size_t copylen;

	//printf("[wandioaha] %s() ENTER. buf: %p , len: %ld \n", __func__, buffer, len);

	while (remained)
	{
		if (remained > INPUT_BUFFER_SIZE)
			copylen = INPUT_BUFFER_SIZE;
		else
			copylen = remained;
		memcpy(blocks[head].in_buff, buf_p, copylen);

		buf_p += copylen;
		remained -= copylen;

		if(ahagz_api_addoutput(&blocks[head].stream, blocks[head].out_buff + OBUFF_OFFSET, OUTPUT_BUFFER_SIZE - OBUFF_OFFSET))
		{
			fprintf(stderr, "Error adding output buffer.\n");
			pulseaha_cleanup(blocks, DATA(iow)->num_blocks);
			return -1;
		}
		//The AHA device begins processing data after an input buffer is added.
		if(ahagz_api_addinput(&blocks[head].stream, blocks[head].in_buff, copylen, 1, 1, 0, NULL, 0))
		{
			fprintf(stderr, "Error adding input buffer.\n");
			pulseaha_cleanup(blocks, DATA(iow)->num_blocks);
			return -1;
		}

		head++;
		//if we yet have data then we put it in #0 buffer again in hope its already processed
		//probably in some obstacles overflow is possible. need to check on real examples
		if(head == DATA(iow)->num_blocks) 
			head = 0;

		submitted++;
	}

	while (submitted)
	{
		do 
		{	//we do not really check in_cnt and out_cnt
			rv = ahagz_api_waitstat(&blocks[tail].stream, &in_cnt, &out_cnt, TIMEOUT);
		} while (rv < 0x8000 || rv == RET_BUFF_RECLAIM);

		if (rv != 0x8000)
		{
			fprintf(stderr, "Error encountered calling ahagz_api_waitstat().\n");
			pulseaha_cleanup(blocks, DATA(iow)->num_blocks);
			return -1;	
		}

		//returns output size of compressed data
		rv = ahagz_api_output_size(&blocks[tail].stream);
		if(rv < 0)
		{
			fprintf(stderr, "Error encountered calling ahagz_api_output_size().\n");
			pulseaha_cleanup(blocks, DATA(iow)->num_blocks);
			return -1;	
		}

		// Adjust size for expanded header
		int block_len = rv + OBUFF_OFFSET;

		// Build new header (XXX - check this later, why do we need to add header manually?)
		blocks[tail].out_buff[0] = 0x1f;
		blocks[tail].out_buff[1] = 0x8b;
		blocks[tail].out_buff[2] = 0x08;
		blocks[tail].out_buff[3] = 0x04;
		blocks[tail].out_buff[4] = 0x00;
		blocks[tail].out_buff[5] = 0x00;
		blocks[tail].out_buff[6] = 0x00;
		blocks[tail].out_buff[7] = 0x00;
		blocks[tail].out_buff[8] = 0x00;
		blocks[tail].out_buff[9] = 0x03;
		blocks[tail].out_buff[10] = 0x08; // Extra len
		blocks[tail].out_buff[11] = 0x00;
		blocks[tail].out_buff[12] = 'E'; // SI1
		blocks[tail].out_buff[13] = 'F'; // SI2
		blocks[tail].out_buff[14] = 0x04; // LEN
		blocks[tail].out_buff[15] = 0x00;
		blocks[tail].out_buff[16] = block_len & 0xff;         // Store compressed block length
		blocks[tail].out_buff[17] = (block_len >> 8) & 0xff;
		blocks[tail].out_buff[18] = (block_len >> 16) & 0xff;
		blocks[tail].out_buff[19] = (block_len >> 24) & 0xff;

		int bytes_written = wandio_wwrite(DATA(iow)->child, blocks[tail].out_buff, block_len);
		if (bytes_written <= 0)
		{
			DATA(iow)->err = ERR_ERROR;
			//XXX - rework this
			//if (DATA(iow)->strm.avail_in != (uint32_t)len)
			//	return len-DATA(iow)->strm.avail_in;
			return -1;
		}

		if(bytes_written != block_len)
		{
			fprintf(stderr, "Error encountered calling write().\n");
			pulseaha_cleanup(blocks, DATA(iow)->num_blocks);
			return -1;
		}

		if(ahagz_api_reinitialize(&blocks[tail].stream, 0, 0))
		{
			fprintf(stderr, "Error calling ahagz_api_reinitialize().\n");
			pulseaha_cleanup(blocks, DATA(iow)->num_blocks);
			return -1;
		}

		tail++;
		if(tail == DATA(iow)->num_blocks) 
			tail = 0;
		submitted--;
	}



#if 0
	DATA(iow)->strm.next_in = (Bytef*)buffer; /* This casts away const, but it's really const 
						   * anyway 
						   */
	DATA(iow)->strm.avail_in = len;

	while (DATA(iow)->err == ERR_OK && DATA(iow)->strm.avail_in > 0) {
		while (DATA(iow)->strm.avail_out <= 0) {
			int bytes_written = wandio_wwrite(DATA(iow)->child, 
				(char *)DATA(iow)->outbuff,
				sizeof(DATA(iow)->outbuff));
			if (bytes_written <= 0) { /* Error */
				DATA(iow)->err = ERR_ERROR;
				/* Return how much data we managed to write ok */
				if (DATA(iow)->strm.avail_in != (uint32_t)len) {
					return len-DATA(iow)->strm.avail_in;
				}
				/* Now return error */
				return -1;
			}
			DATA(iow)->strm.next_out = DATA(iow)->outbuff;
			DATA(iow)->strm.avail_out = sizeof(DATA(iow)->outbuff);
		}
		/* Decompress some data into the output buffer */
		int err=deflate(&DATA(iow)->strm, 0);
		switch(err) {
			case Z_OK:
				DATA(iow)->err = ERR_OK;
				break;
			default:
				DATA(iow)->err = ERR_ERROR;
		}
	}
	/* Return the number of bytes decompressed */
	return len-DATA(iow)->strm.avail_in;
#endif
	return len - remained;
}


//XXX - do we need a wandio_write() here or not?
static void hwzlib_wclose(iow_t *iow)
{
	
#if 0
	int res;

	while (1) {
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

	deflateEnd(&DATA(iow)->strm);

	wandio_wwrite(DATA(iow)->child, 
			(char *)DATA(iow)->outbuff,
			sizeof(DATA(iow)->outbuff)-DATA(iow)->strm.avail_out);
#endif

	pulseaha_cleanup(blocks, DATA(iow)->num_blocks);

	wandio_wdestroy(DATA(iow)->child);
	free(iow->data);
	free(iow);
}

iow_source_t zlib_wsource = {
	"hwzlibw",		//repu1sion: doesn't seem like used somewhere
	hwzlib_wwrite,
	hwzlib_wclose
};
