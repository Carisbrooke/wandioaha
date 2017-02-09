
#include "config.h"
#include "wandio.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <blosc.h>

#define BUF_IN_SIZE 1024*1024*2

#define DEBUG

#ifdef DEBUG
 #define debug(x...) printf(x)
#else
 #define debug(x...)
#endif

/* Libwandio IO module implementing a blosc reader */

enum err_t {
	ERR_OK	= 1,
	ERR_EOF = 0,
	ERR_ERROR = -1
};

struct blosc_t {
	char inbuff[BUF_IN_SIZE];
	char *next_in;
	int avail_in;
	char *next_out;
	int avail_out;
	io_t *parent;
	enum err_t err;
	//int outoffset; - not used here
        size_t sincelastend;
};

extern io_source_t blosc_source; 

#define DATA(io) ((struct blosc_t *)((io)->data))
#define min(a,b) ((a)<(b) ? (a) : (b))

io_t *blosc_open(io_t *parent)
{
	io_t *io;
	if (!parent)
		return NULL;
	io = malloc(sizeof(io_t));
	io->source = &blosc_source;
	io->data = malloc(sizeof(struct blosc_t));

        blosc_init();


	DATA(io)->parent = parent;
	DATA(io)->next_in = NULL;
	DATA(io)->avail_in = 0;
	DATA(io)->next_out = NULL;
	DATA(io)->avail_out = 0;
	DATA(io)->err = ERR_OK;
        DATA(io)->sincelastend = 1;

	return io;
}

//we provide some empty buffer with len, so inflate() will put some data into it
//we usually have 1Mb empty buffer
static int64_t blosc_read(io_t *io, void *buffer, int64_t len)
{
	int dsize;
	int bytes_read = 0;

	if (DATA(io)->err == ERR_EOF)
		return 0; /* EOF */
	if (DATA(io)->err == ERR_ERROR) {
		errno = EIO;
		return -1; /* ERROR! */
	}

	debug("[wandio] %s() ENTER. buf: %p , len: %ld \n", __func__,
		buffer, len);
	DATA(io)->avail_out = (int)len;
	DATA(io)->next_out = (char*)buffer;

	//while we have some empty space in OUT buffer for decompressed data
	while (DATA(io)->err == ERR_OK && DATA(io)->avail_out > 0) 
	{	//if we have no more input data (compressed one)
		while (DATA(io)->avail_in <= 0) 
		{	//zeroing inbuff for blosc_decompress()
			memset(DATA(io)->inbuff, 0x0, sizeof(DATA(io)->inbuff));
			//fill inbuff with compressed data read from file
			bytes_read = wandio_read(DATA(io)->parent, DATA(io)->inbuff,
						     sizeof(DATA(io)->inbuff));
			debug("[wandio] fill INBUF from file. bytes read: %d \n", bytes_read);
			if (bytes_read == 0) 
			{
                                /* If we get EOF immediately after a 
                                 * Z_STREAM_END, then we assume we've reached 
                                 * the end of the file. If there was data 
                                 * between the Z_STREAM_END and the EOF, the
                                 * file has more likely been truncated.
                                 */
//XXX - probably we don't need sincelastend thing
#if 0
                                if (DATA(io)->sincelastend > 0) {
                                        fprintf(stderr, "Unexpected EOF while reading compressed file -- file is probably incomplete\n");
                                        errno = EIO;
                                        DATA(io)->err = ERR_ERROR;
                                        return -1;
                                }
#endif

                                /* EOF */
				if (DATA(io)->avail_out == (int)len) 
				{
                                        DATA(io)->err = ERR_EOF;
					return 0;
				}
				/* Return how much data we've managed to read so far. */
				return len-DATA(io)->avail_out;
			}
			if (bytes_read < 0) /* Error */
			{
				/* errno should be set */
				DATA(io)->err = ERR_ERROR;
				/* Return how much data we managed to read ok */
				if (DATA(io)->avail_out != (int)len)
					return len-DATA(io)->avail_out;
				/* Now return error */
				return -1;
			}
			DATA(io)->next_in = DATA(io)->inbuff;
			DATA(io)->avail_in = bytes_read;
                        DATA(io)->sincelastend += bytes_read; //XXX - do we need it?
		}

		/* Decompress  (src, dest, dest_size) */
		dsize = blosc_decompress(DATA(io)->next_in, DATA(io)->next_out, DATA(io)->avail_out);
		if (dsize < 0) 
		{
			printf("Decompression error.  Error code: %d\n", dsize);
			return dsize;
		}
		else
		{
			debug("[wandio] successfully decompressed %d bytes \n", dsize);
			DATA(io)->next_in += bytes_read;
			DATA(io)->avail_in -= bytes_read;
			DATA(io)->avail_out -= dsize;
			DATA(io)->next_out += dsize;
		}

#if 0
		/* Decompress some data into the output buffer */
		int err=inflate(&DATA(io)->strm, 0);
		switch(err) {
			case Z_OK:
				DATA(io)->err = ERR_OK;
				break;
			case Z_STREAM_END:
				/* You would think that an "EOF" on the stream would mean we'd
 				 * want to pass on an EOF?  Nope.  Some tools (*cough* corel *cough*)
 				 * annoyingly close and reopen the gzip stream leaving Z_STREAM_END
 				 * mines for us to find.
 				 */
				inflateEnd(&DATA(io)->strm);
				inflateInit2(&DATA(io)->strm, 15 | 32);
				DATA(io)->err = ERR_OK;
                                DATA(io)->sincelastend = 0;
				break;
			default:
				errno=EIO;
				DATA(io)->err = ERR_ERROR;
		}
#endif
	}
	/* Return the number of bytes decompressed */
	return len - DATA(io)->avail_out;
}

static void blosc_close(io_t *io)
{
	debug("[wandio] %s() \n", __func__);

	//wandio_write() should be here? XXX 
	wandio_destroy(DATA(io)->parent);
	free(io->data);
	free(io);

	blosc_destroy();
}

io_source_t blosc_source = {
	"blosc",
	blosc_read,
	NULL,	/* peek */
	NULL,	/* tell */
	NULL,	/* seek */
	blosc_close
};
