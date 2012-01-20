/* Copyright 2012 K2Informatics GmbH, Root Laengenbold, Switzerland
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */ 
#pragma once

// Protocol spec
#define PKT_LEN_BYTES	4
#define MAX_ARGS		10

typedef enum _ERL_CMD {
    CREATE_SESSION_POOL	= 0,
    GET_SESSION			= 1,
    RELEASE_SESSION		= 2,
    EXEC_SQL			= 3,
    FETCH_ROWS			= 4,
    R_DEBUG_MSG			= 5,
    FREE_SESSION_POOL	= 6,
    QUIT				= 7,
} ERL_CMD;

typedef enum _ERL_DEBUG {
    DBG_FLAG_OFF	= 0,
    DBG_FLAG_ON		= 1,
} ERL_DEBUG;

typedef enum _ROW_FETCH {
    ERROR,
    MORE,
    DONE
} ROW_FETCH;

typedef enum _DATA_DIR {
    DIR_IN		= 0,
    DIR_OUT		= 1,
    DIR_INOUT	= 2,
} DATA_DIR;

typedef enum _DATA_TYPES {
    NUMBER	= 0,
    STRING	= 1,
} DATA_TYPES;

typedef enum _INTF_RET {
    SUCCESS				= 0,
    CONTINUE_WITH_ERROR	= 1,
    FAILURE				= 2,
} INTF_RET;

typedef struct inp_t {
    struct inp_t * next;
    void		 * bndp;
    int			value_sz;
    DATA_TYPES	dty;
    DATA_DIR	dir;
    void		*valuep;
} inp_t;

// Debug Levels
#define DBG_0			0	// All Debug (Including binaries for Rx and Tx)
#define DBG_1			1	// Function Entry Exit
#define DBG_2			2	//
#define DBG_3			3	//
#define DBG_4			4	//
#define DBG_5			5	// Debug is off

#define DEBUG			DBG_0

//#define MAX_RESP_SIZE 0xFFFFFFF0
#define MAX_RESP_SIZE 0x00010000

#ifndef __WIN32__
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#define memcpy_s(_dest, _noelms, _src, _count)	memcpy((_dest), (_src), (_count))
#define sprintf_s(_a, _b, _c, ...)				sprintf((_a), (_c), __VA_ARGS__)
#define strncpy_s(_a, _b, _c, _d)               strncpy((_a), (_c), (_d))
#define vsprintf_s(_a, _b, _c, _d)              vsprintf((_a), (_c), (_d))
#define REMOTE_LOG(_str, ...)		if (log_flag) erl_log((_str), ##__VA_ARGS__)
#else
#define REMOTE_LOG(_str, ...)		if (log_flag) erl_log((_str), __VA_ARGS__)
#endif

/*
 * Externs variables
 */
extern char			gerrbuf[2048];
extern int			gerrcode;
extern char			session_pool_name[2048];
extern bool			log_flag;

#if DEBUG < DBG_5
extern FILE			*fp_log;
#endif

extern bool init_marshall(void);
extern inp_t * map_to_bind_args(void *);
extern void * build_term_from_bind_args(inp_t *);
extern void * read_cmd(void);
extern int write_resp(void * resp_term);
extern void erl_log(const char *log_str, ...);
extern void log_args(int, void *, const char *);

extern void			oci_init(void);
extern void			oci_cleanup(void);
extern void			get_last_error(char *, int &);
extern bool			oci_create_tns_seesion_pool(const unsigned char *, const int, const unsigned char *, const int, const unsigned char *, const int, const unsigned char *, const int);
extern bool			oci_free_session_pool(void);
extern void*		oci_get_connection_from_pool(void);
extern bool			oci_return_connection_to_pool(void *);
extern INTF_RET		oci_exec_sql(const void *, unsigned long &, const unsigned char *, int, inp_t *, void *);
extern ROW_FETCH	oci_produce_rows(void *, void *);

extern unsigned int calculate_resp_size(void * resp);
extern void append_list_to_list(const void * sub_list, void * list);
extern void append_int_to_list(const int integer, void * list);
extern void append_string_to_list(const char * string, void * list);
extern void append_coldef_to_list(const char * col_name, const char * data_type, const unsigned int max_len, void * list);

// ThreadPool
extern bool InitializeThreadPool(void);
extern void CleanupThreadPool(void);
extern bool ProcessCommand(void *);

// Erlang Interface Macros
#define ARG_COUNT(_command)				(erl_size(_command) - 1)
#define MAP_ARGS(_command, _target)		{for(int _i=0;_i<ARG_COUNT(_command); ++_i)(_target)[_i] = erl_element(_i+2, _command);}

#if DEBUG <= DBG_3
#define LOG_ERROR(_cmd, _err_str)		fprintf(fp_log, "CMD: "_cmd" ERROR - %s", (_err_str));
#define LOG_ARGS(_count,_args,_str)	    log_args((_count),(_args),(_str))
#else
#define LOG_ARGS(_count,_args,_str)
#define LOG_ERROR(_cmd, _err_str)
#endif
