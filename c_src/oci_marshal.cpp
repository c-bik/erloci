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
#include "stdafx.h"

#include <iostream>

#include "oci_marshal.h"
#include "erl_interface.h"

#ifdef __WIN32__
#include <windows.h>
#else
#include <pthread.h>
#endif

using namespace std;

#if DEBUG <= DBG_1
#define ENTRY()	{fprintf(fp_log, "[%s:%d] Entry\n", __FUNCTION__, __LINE__); fflush(fp_log);}
#define EXIT()	{fprintf(fp_log, "[%s:%d] Exit\n", __FUNCTION__, __LINE__);  fflush(fp_log);}
#else
#define ENTRY()
#define EXIT()
#endif

// 32 bit packet header
typedef union _pack_hdr {
    char len_buf[4];
    u_long len;
} pkt_hdr;

void * build_term_from_bind_args(inp_t * bind_var_list_head)
{
    if (bind_var_list_head == NULL)
        return NULL;

    inp_t * param_t = NULL;
    ETERM * resp_args = erl_mk_empty_list();
    for(inp_t *param = bind_var_list_head; param != NULL;) {
        if(param->dir == DIR_OUT || param->dir == DIR_INOUT) {
            switch(param->dty) {
            case NUMBER:
                resp_args = erl_cons(erl_mk_int(*(int*)param->valuep), resp_args);
                break;
            case STRING:
                resp_args = erl_cons(erl_mk_string((char*)param->valuep), resp_args);
                break;
            }
        }
        param_t = param;
        param = param->next;
        delete param_t;
    }

    return resp_args;
}

inp_t * map_to_bind_args(void * _args)
{
    ETERM * args = (ETERM *)_args;
    if(!ERL_IS_LIST(args) || ERL_IS_EMPTY_LIST(args))
        return NULL;

    int num_vars = erl_length(args),
        idx = 0;
    inp_t * bind_var_list_head	= NULL,
             * bind_var_cur		= NULL,
                   * bind_var_t			= NULL;

    ETERM * item = NULL;
    ETERM * arg = NULL;
    DATA_TYPES dty;
    DATA_DIR dir;
    do {
        if ((item = erl_hd(args)) == NULL	||
            !ERL_IS_TUPLE(item)				||
            erl_size(item) != 3)
            break;

        if ((arg = erl_element(1, item)) == NULL || !ERL_IS_INTEGER(arg))
            break;
        dty = (DATA_TYPES)ERL_INT_VALUE(arg);

        if ((arg = erl_element(2, item)) == NULL || !ERL_IS_INTEGER(arg))
            break;
        dir = (DATA_DIR)ERL_INT_VALUE(arg);

        if ((arg = erl_element(3, item)) == NULL)
            break;
        else {
            bool error = false;
            switch(dty) {
            case NUMBER:
                if (!ERL_IS_INTEGER(arg)) error = true;
                bind_var_t = (inp_t*) new unsigned char[sizeof(inp_t)+sizeof(int)];
                bind_var_t->value_sz = sizeof(int);
                bind_var_t->valuep = (((char *)bind_var_t) + sizeof(inp_t));
                *(int*)(bind_var_t->valuep) = ERL_INT_VALUE(arg);
                break;
            case STRING:
                if (!ERL_IS_BINARY(arg)) error = true;
                bind_var_t = (inp_t*) new unsigned char[sizeof(inp_t)+ERL_BIN_SIZE(arg)+1];
                bind_var_t->value_sz = ERL_BIN_SIZE(arg) + 1;
                bind_var_t->valuep = (((char *)bind_var_t) + sizeof(inp_t));
                strncpy_s((char*)(bind_var_t->valuep), bind_var_t->value_sz, (const char *) ERL_BIN_PTR(arg), ERL_BIN_SIZE(arg));
                ((char*)(bind_var_t->valuep))[ERL_BIN_SIZE(arg)] = '\0';
                break;
            default:
                break;
            };

            if (!error && bind_var_t != NULL) {
                bind_var_t->dty = dty;
                bind_var_t->dir = dir;
                bind_var_t->next = NULL;
                if (idx == 0)
                    bind_var_list_head = bind_var_cur = bind_var_t;
                else {
                    bind_var_cur->next = bind_var_t;
                    bind_var_cur = bind_var_t;
                }
                bind_var_t = NULL;
            } else
                break;
        }

        args = erl_tl(args);
        ++idx;
    } while (args != NULL && !ERL_IS_EMPTY_LIST(args));

    if (idx != num_vars) {
        while(bind_var_list_head != NULL) {
            bind_var_t = bind_var_list_head;
            bind_var_list_head = bind_var_list_head->next;
            delete bind_var_t;
        }
        bind_var_list_head = NULL;
    }
    return bind_var_list_head;
}

unsigned int calculate_resp_size(void * resp)
{
    return (unsigned int)erl_term_len(*(ETERM**)resp);
}

#define MAX_FORMATTED_STR_LEN 1024
void erl_log(const char *fmt, ...)
{
    char log_str[MAX_FORMATTED_STR_LEN];
    va_list arguments;
    va_start(arguments, fmt);

    vsprintf_s(log_str, MAX_FORMATTED_STR_LEN, fmt, arguments);
    ETERM * log = erl_format((char*)"{~i,log,~s}", R_DEBUG_MSG, log_str);
    write_resp(log);
    erl_free_compound(log);

    va_end(arguments);
}

void log_args(int argc, void * argv, const char * str)
{
    int sz = 0, idx = 0;
    char *arg = NULL;
    ETERM **args = (ETERM **)argv;
    fprintf(fp_log, "CMD: %s Args(", str);
    for(idx=0; idx<argc; ++idx) {
        if (ERL_IS_BINARY(args[idx])) {
            sz = ERL_BIN_SIZE(args[idx]);
            arg = new char[sz+1];
            memcpy_s(arg, sz+1, ERL_BIN_PTR(args[idx]), sz);
            arg[sz] = '\0';
            fprintf(fp_log, "%s,", arg);
            if (arg != NULL) delete arg;
        } else if (ERL_IS_INTEGER(args[idx]))			    fprintf(fp_log, "%d,",	ERL_INT_VALUE(args[idx]));
        else if (ERL_IS_UNSIGNED_INTEGER(args[idx]))	fprintf(fp_log, "%u,",	ERL_INT_UVALUE(args[idx]));
        else if (ERL_IS_FLOAT(args[idx]))				fprintf(fp_log, "%lf,",	ERL_FLOAT_VALUE(args[idx]));
        else if (ERL_IS_ATOM(args[idx]))				fprintf(fp_log, "%.*s,", ERL_ATOM_SIZE(args[idx]), ERL_ATOM_PTR(args[idx]));
    }
    fprintf(fp_log, ")\n");
    fflush(fp_log);
}

void append_list_to_list(const void * sub_list, void * list)
{
    if (list == NULL || sub_list == NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

    container_list = erl_cons(erl_format((char*)"~w", (ETERM*)sub_list), container_list);
    erl_free_compound((ETERM*)sub_list);

    (*(ETERM**)list) = container_list;
}

void append_int_to_list(const int integer, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

    container_list = erl_cons(erl_format((char*)"~i", integer), container_list);
    (*(ETERM**)list) = container_list;
}

void append_string_to_list(const char * string, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

    container_list = erl_cons(erl_format((char*)"~s", string), container_list);
    (*(ETERM**)list) = container_list;
}

void append_coldef_to_list(const char * col_name, const char * data_type, const unsigned int max_len, void * list)
{
    if (list==NULL)
        return;

    ETERM *container_list = (ETERM *)(*(ETERM**)list);
    if (container_list == NULL)
        container_list = erl_mk_empty_list();

    container_list = erl_cons(erl_format((char*)"{~s,~a,~i}", col_name, data_type, max_len), container_list);

    (*(ETERM**)list) = container_list;
}

void * read_cmd(void)
{
    pkt_hdr hdr;
    unsigned int rx_len;
    ETERM *t;
    char * rx_buf;

    ENTRY();

    // Read and convert the length to host Byle order
    cin.read(hdr.len_buf, sizeof(hdr.len_buf));
    if((unsigned)cin.gcount() < sizeof(hdr.len_buf)) {
        EXIT();
        return NULL;
    }
    rx_len = ntohl(hdr.len);

#if DEBUG <= DBG_2
    fprintf(fp_log, "[%s:%d] RX Packet length %d\n", __FUNCTION__, __LINE__, rx_len);
    fflush(fp_log);
#endif

    // Read the Term binary
    rx_buf = new char[rx_len];
    if (rx_buf == NULL) { // Memory allocation error
        EXIT();
        return NULL;
    }

    cin.read(rx_buf, rx_len);
    if((unsigned int) cin.gcount() < rx_len) {
        // Unable to get Term binary
        delete rx_buf;
        EXIT();
        return NULL;
    }

#if DEBUG <= DBG_0
    // Printing the packet
    fprintf(fp_log, "[%s:%d] RX:\n-------------\n", __FUNCTION__, __LINE__);
    for(unsigned int j=0; j<rx_len; ++j) {
        fprintf(fp_log, "%d ", (unsigned char)rx_buf[j]);
        if(j>0 && (j+1)%16==0)
            fprintf(fp_log, "\n");
    }
    if((rx_len-1)%16)
        fprintf(fp_log, "\n");
    fprintf(fp_log, "-------------\n");
    fflush(fp_log);
#endif

    t = erl_decode((unsigned char*)rx_buf);
    if (t == NULL) {
        // Term de-marshaling failed
        delete rx_buf;
        EXIT();
        return NULL;
    }

#if DEBUG <= DBG_0
    fprintf(fp_log, "[%s:%d] RX: term\n-------------\n", __FUNCTION__, __LINE__);
    erl_print_term(fp_log, t);
    fprintf(fp_log, "\n-------------\n");
    fflush(fp_log);
#endif

    if(NULL != rx_buf)
        delete rx_buf;

    EXIT();
    return t;
}


#ifdef __WIN32__
static HANDLE write_mutex;
#else
static pthread_mutex_t write_mutex;
#endif
bool init_marshall(void)
{
#ifdef __WIN32__
    write_mutex = CreateMutex(NULL, FALSE, NULL);
    if (NULL == write_mutex) {
        REMOTE_LOG("Write Mutex creation failed");
        return false;
    }
#else
    if(pthread_mutex_init(&write_mutex, NULL) != 0) {
        REMOTE_LOG("Write Mutex creation failed");
        return false;
    }
#endif

    return true;
}

int write_resp(void * resp_term)
{
    int tx_len;
    int pkt_len = -1;
    pkt_hdr *hdr;
    unsigned char * tx_buf;
    ETERM * resp = (ETERM *)resp_term;

    if (resp == NULL) {
        pkt_len = -1;
        goto error_exit;
    }

#if DEBUG <= DBG_0
    fprintf(fp_log, "[%s:%d] TX: term\n-------------\n", __FUNCTION__, __LINE__);
    erl_print_term(fp_log, resp);
    fprintf(fp_log, "\n-------------\n");
    fflush(fp_log);
#endif

    tx_len = erl_term_len(resp);				// Length of the required binary buffer
    pkt_len = tx_len+PKT_LEN_BYTES;

#if DEBUG <= DBG_2
    fprintf(fp_log, "[%s:%d] TX Packet length %d\n", __FUNCTION__, __LINE__, tx_len);
    fflush(fp_log);
#endif

    // Allocate temporary buffer for transmission of the Term
    tx_buf = new unsigned char[pkt_len];
    hdr = (pkt_hdr *)tx_buf;
    hdr->len = htonl(tx_len);		// Length adjusted to network byte order

    erl_encode(resp, tx_buf+PKT_LEN_BYTES);			// Encode the Term into the buffer after the length field

#if DEBUG <= DBG_0
    // Printing the packet
    fprintf(fp_log, "[%s:%d] TX:\n-------------\n", __FUNCTION__, __LINE__);
    for(int j=PKT_LEN_BYTES; j<pkt_len; ++j) {
        fprintf(fp_log, "%d ", (unsigned char)tx_buf[j]);
        if(j>0 && (j+1)%16==0)
            fprintf(fp_log, "\n");
    }
    if((pkt_len-1)%16)
        fprintf(fp_log, "\n");
    fprintf(fp_log, "-------------\n");
    fflush(fp_log);
#endif

    if(
#ifdef __WIN32__
        WAIT_OBJECT_0 == WaitForSingleObject(write_mutex,INFINITE)
#else
        0 == pthread_mutex_lock(&write_mutex)
#endif
    ) {
        cout.write((char *) tx_buf, pkt_len);
        cout.flush();
#ifdef __WIN32__
        ReleaseMutex(write_mutex);
#else
        pthread_mutex_unlock(&write_mutex);
#endif
    }

    // Free the temporary allocated buffer
    delete tx_buf;

    if (cout.fail()) {
        pkt_len = -1;
        goto error_exit;
    }

    EXIT();

error_exit:
    erl_free_compound(resp);
    return pkt_len;
}
