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
#include <string.h>
#include <stdlib.h>

#include "oci.h"
#include "oci_marshal.h"

typedef struct column_info {
    ub2  dtype;
    ub4	 dlen;
} column_info;

/*
 * Global variables
 */
static OCIEnv		*envhp		= NULL;
static OCIError		*errhp		= NULL;
static OCISPool		*spoolhp	= NULL;

static char			*poolName	= NULL;
static ub4			poolNameLen	= 0;
static ub4			stmt_cachesize;
static column_info	*columns = NULL;
static ub4			num_cols = 0;

char				session_pool_name[2048];
char				gerrbuf[2048];
sb4					gerrcode = 0;

/* constants */
#define POOL_MIN	0
#define POOL_MAX	10
#define POOL_INCR	0
#define MAX_COLUMS	1000

/* Error checking functions and macros */
void checkerr0(void *, ub4, sword, const char * function_name, int line_no);
#define checkerr(errhp, status) checkerr0((errhp), OCI_HTYPE_ERROR, (status), __FUNCTION__, __LINE__)
#define checkenv(envhp, status) checkerr0((envhp), OCI_HTYPE_ENV, (status), __FUNCTION__, __LINE__)

static INTF_RET function_success = SUCCESS;

void get_last_error(char *buf, int & len)
{
    len = strlen(gerrbuf);
    if(buf != NULL)
        strncpy_s(buf, len+1, gerrbuf, len);
}

void oci_init(void)
{
	function_success = SUCCESS;
    checkenv(envhp, OCIEnvCreate(&envhp,				/* returned env handle */
                                 OCI_DEFAULT,			/* initilization modes */
                                 NULL, NULL, NULL, NULL,/* callbacks, context */
                                 (size_t) 0,			/* optional extra memory size: optional */
                                 (void**) NULL));		/* returned extra memeory */

    checkenv(envhp, OCIHandleAlloc(envhp,				/* environment handle */
                                   (void **) &errhp,	/* returned err handle */
                                   OCI_HTYPE_ERROR,		/* typ of handle to allocate */
                                   (size_t) 0,			/* optional extra memory size */
                                   (void **) NULL));	/* returned extra memeory */

    if(function_success != SUCCESS)
        exit(1);
}

void oci_cleanup(void)
{
    if (columns != NULL)
        free(columns);
    oci_free_session_pool();
    if (errhp != NULL) {
        OCIHandleFree(errhp, OCI_HTYPE_ERROR);
        errhp = NULL;
    }
}

bool oci_free_session_pool(void)
{
    function_success = SUCCESS;

    if (spoolhp != NULL) {
        checkerr(errhp, OCISessionPoolDestroy(spoolhp, errhp, OCI_SPD_FORCE));
        if(function_success != SUCCESS)return false;

        if(OCI_SUCCESS != OCIHandleFree(spoolhp, OCI_HTYPE_SPOOL))
            return false;
        spoolhp = NULL;
    }

    return true;
}

bool oci_create_tns_seesion_pool(const unsigned char * connect_str, const int connect_str_len,
                                 const unsigned char * user_name, const int user_name_len,
                                 const unsigned char * password, const int password_len,
                                 const unsigned char * options, const int options_len)
{
    function_success = SUCCESS;
    poolName = NULL;
    poolNameLen = 0;

    oci_free_session_pool();

    /* allocate session pool handle
     * note: for OCIHandleAlloc() we check error on environment handle
     */
    checkenv(envhp, OCIHandleAlloc(envhp, (void **)&spoolhp,
                                   OCI_HTYPE_SPOOL, (size_t) 0, (void **) NULL));

    /* set the statement cache size for all sessions in the pool
     * note: this can also be set per session after obtaining the session from the pool
     */
    checkerr(errhp, OCIAttrSet(spoolhp, OCI_HTYPE_SPOOL,
                               &stmt_cachesize, 0, OCI_ATTR_SPOOL_STMTCACHESIZE, errhp));

    checkerr(errhp, OCISessionPoolCreate(envhp, errhp,
                                         spoolhp,
                                         (OraText **) &poolName, &poolNameLen,
                                         (OraText *) connect_str, connect_str_len,
                                         POOL_MIN, POOL_MAX, POOL_INCR,
                                         (OraText*)user_name, user_name_len,			/* homo pool user specified */
                                         (OraText*)password, password_len,			/* homo pool password specified */
                                         OCI_SPC_STMTCACHE));	/* modes */
    if(function_success != SUCCESS)return false;

    sprintf_s(session_pool_name, sizeof(session_pool_name), "%.*s", poolNameLen, (char *)poolName);

    ub1 spoolMode = OCI_SPOOL_ATTRVAL_NOWAIT;
    checkerr(errhp, OCIAttrSet(spoolhp, OCI_HTYPE_SPOOL,
                               (void*)&spoolMode, sizeof(ub1),
                               OCI_ATTR_SPOOL_GETMODE, errhp));
    if(function_success != SUCCESS)return false;

    return true;
}

void * oci_get_connection_from_pool()
{
    function_success = SUCCESS;

    /* get the database connection */
    OCISvcCtx	*svchp = NULL;
    checkerr(errhp, OCISessionGet(envhp, errhp,
                                  &svchp,		/* returned database connection */
                                  NULL,		/* initialized authentication handle */
                                  /* connect string */
                                  (OraText *) poolName, poolNameLen,
                                  /* session tagging parameters: optional */
                                  NULL, 0, NULL, NULL, NULL,
                                  OCI_SESSGET_SPOOL));/* modes */

    if(function_success != SUCCESS)
        return NULL;

    return svchp;
}

bool oci_return_connection_to_pool(void * connection_handle)
{
    function_success = SUCCESS;

    OCISvcCtx *svchp = (OCISvcCtx *)connection_handle;
    if (svchp != NULL)
        checkerr(errhp, OCISessionRelease(svchp, errhp, NULL, 0, OCI_DEFAULT));

    if(function_success != SUCCESS)
        return false;

    return true;
}

#define MAX_CONNECT_STR_LEN 1024
bool oci_create_seesion_pool(const unsigned char * host_str, const int host_len, const unsigned int port,
                             const unsigned char * srv_str, const int srv_len,
                             const unsigned char * user_name, const int user_name_len,
                             const unsigned char * password, const int password_len,
                             const unsigned char * options, const int options_len)
{
    char connect_str[MAX_CONNECT_STR_LEN];
    sprintf_s(connect_str, MAX_CONNECT_STR_LEN,
              "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp) (HOST=%.*s) (PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%.*s)))",
              host_len, host_str,
              port,
              srv_len, srv_str);
    return oci_create_tns_seesion_pool((unsigned char *)connect_str, strlen(connect_str),
                                       user_name, user_name_len,
                                       password, password_len,
                                       options, options_len);
}

INTF_RET oci_exec_sql(const void *conn_handle, unsigned long & stmt_handle, const unsigned char * query_str, int query_str_len, inp_t *params_head, void * column_list)
{
    function_success = SUCCESS;

#if DEBUG < DBG_5
    fprintf(fp_log, "Executing \"%.*s;\"\n", query_str_len, query_str);
#endif

    OCISvcCtx *svchp = (OCISvcCtx *)conn_handle;
    OCIStmt *stmthp	= NULL;
    stmt_handle = 0;

    /* Get a prepared statement handle */
    checkerr(errhp, OCIStmtPrepare2(svchp,
                                    &stmthp,					/* returned statement handle */
                                    errhp,						/* error handle */
                                    (OraText *) query_str,		/* the statement text */
                                    query_str_len,				/* length of the text */
                                    NULL, 0,					/* tagging parameters: optional */
                                    OCI_NTV_SYNTAX, OCI_DEFAULT));
    if(function_success != SUCCESS) return function_success;

    /* Bind variables */
    ub2 type = SQLT_INT;
    int idx = 1;
    for(inp_t *param = params_head; param != NULL; param = param->next) {
        switch (param->dty) {
        case NUMBER:
            type = SQLT_INT;
            break;
        case STRING:
            type = SQLT_STR;
            break;
        }
        param->bndp = NULL;
        checkerr(errhp, OCIBindByPos(stmthp, (OCIBind **)&(param->bndp), errhp, idx,
                                     (dvoid *) param->valuep, (sword) param->value_sz, type,
                                     (dvoid *) 0, (ub2 *) 0, (ub2 *) 0, (ub4) 0, (ub4 *) 0, OCI_DEFAULT));
        if(function_success != SUCCESS) return function_success;
        idx++;
    }

    ub4 stmt_typ = OCI_STMT_SELECT;
    ub4 itrs = 0;
    checkerr(errhp, OCIAttrGet((dvoid*) stmthp, (ub4) OCI_HTYPE_STMT,
                               (dvoid*) &stmt_typ, (ub4 *)NULL, (ub4)OCI_ATTR_STMT_TYPE, errhp));
    if(function_success != SUCCESS) return function_success;
    if(stmt_typ != OCI_STMT_SELECT)
        itrs = 1;

    /* execute the statement and commit */
    checkerr(errhp, OCIStmtExecute(svchp, stmthp, errhp, itrs, 0,
                                   (OCISnapshot *)NULL, (OCISnapshot *)NULL,
                                   OCI_COMMIT_ON_SUCCESS));
    if(function_success != SUCCESS) return function_success;

    if(stmt_typ == OCI_STMT_SELECT) {
        OCIParam	*mypard;
        num_cols	= 1;
        sb4         parm_status;

        /* Request a parameter descriptor for position 1 in the select-list */
        parm_status = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (dvoid **)&mypard,
                                  (ub4) num_cols);
        checkerr(errhp, parm_status);
        if(function_success != SUCCESS) return function_success;

        /* Loop only if a descriptor was successfully retrieved for
         * current position, starting at 1
         */
        text *col_name;
        ub4 len = 0;
        char * data_type = NULL;
        if (columns != NULL)
            free(columns);
        columns = NULL;
        while (parm_status == OCI_SUCCESS) {
            columns = (column_info *)realloc(columns, num_cols * sizeof(column_info));
            column_info * cur_clm = &(columns[num_cols-1]);

            /* Retrieve the data type attribute */
            len = 0;
            checkerr(errhp, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                       (dvoid*) &len, (ub4 *)0, (ub4)OCI_ATTR_DATA_TYPE,
                                       errhp));
            if(function_success != SUCCESS) return function_success;
            cur_clm->dtype = len;

            switch (len) {
            case SQLT_NUM:
            case SQLT_VNU:
            case SQLT_LNG:
                data_type = (char*)"number";
                break;
            case SQLT_AVC:
            case SQLT_AFC:
            case SQLT_CHR:
            case SQLT_STR:
            case SQLT_VCS:
                data_type = (char*)"string";
                break;
            case SQLT_INT:
            case SQLT_UIN:
                data_type = (char*)"integer";
                break;
            case SQLT_DAT:
                data_type = (char*)"date";
                break;
            case SQLT_FLT:
                data_type = (char*)"double";
                break;
            default:
                data_type = (char*)"undefined";
                break;
            }

            /* Retrieve the data size attribute */
            len = 0;
            checkerr(errhp, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                       (dvoid*) &len, (ub4 *)0, (ub4)OCI_ATTR_DATA_SIZE,
                                       errhp));
            if(function_success != SUCCESS) return function_success;
            cur_clm->dlen = len;

            /* Retrieve the column name attribute */
            len = 0;
            checkerr(errhp, OCIAttrGet((dvoid*) mypard, (ub4) OCI_DTYPE_PARAM,
                                       (dvoid**) &col_name, (ub4 *) &len, (ub4) OCI_ATTR_NAME,
                                       errhp));
            if(function_success != SUCCESS) return function_success;
            char * column_name = new char[len+1];
            sprintf_s(column_name, len+1, "%.*s", len, col_name);
            append_coldef_to_list(column_name, data_type, cur_clm->dlen, column_list);
            delete column_name;
            col_name = NULL;

            /* Increment counter and get next descriptor, if there is one */
            if(OCI_SUCCESS != OCIDescriptorFree(mypard, OCI_DTYPE_PARAM))
                return FAILURE;
            num_cols++;
            parm_status = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (dvoid **)&mypard,
                                      (ub4) num_cols);
        }
        --num_cols;

        if(function_success != SUCCESS)
            return function_success;
        REMOTE_LOG("Port: Returning Columns");
        stmt_handle = (unsigned long)stmthp;
    } else {
        if(stmthp != NULL) {
            checkerr(errhp, OCIStmtRelease(stmthp, errhp, NULL, 0, OCI_DEFAULT));
            if(function_success != SUCCESS) return function_success;
        }
        REMOTE_LOG("Port: Executed non-select statement!");
    }

    return function_success;
}

ROW_FETCH oci_produce_rows(void * stmt_handle, void * row_list)
{
    function_success = SUCCESS;
    OCIStmt *stmthp	= (OCIStmt *)stmt_handle;

    if (columns == NULL || stmthp == NULL)
        return ERROR;

    sword res = OCI_NO_DATA;

    OCIDefine **defnhp = (OCIDefine **)malloc(num_cols * sizeof(OCIDefine *));
    void ** data_row = NULL;
    data_row = (void **) calloc(num_cols, sizeof(void *));

    /*
     * Fetch the data
     */
    unsigned int num_rows = 0;
    ub4 i = 0;
    void * row = NULL;

    /* Bind appropriate variables for data based on the column type */
    for (i = 0; i < num_cols; ++i)
        switch (columns[i].dtype) {
        case SQLT_NUM:
        case SQLT_CHR: {
            data_row[i] = (text *) malloc((columns[i].dlen + 1) * sizeof(text));
            checkerr(errhp, OCIDefineByPos(stmthp, &(defnhp[i]), errhp, i+1, (dvoid *) (data_row[i]),
                                           (sword) columns[i].dlen + 1, SQLT_STR, (dvoid *) 0, (ub2 *)0,
                                           (ub2 *)0, OCI_DEFAULT));
            if(function_success != SUCCESS) return ERROR;
        }
        break;
        default:
            break;
        }

    /* Fetch data by row */
    do {
        ++num_rows;
        res = OCIStmtFetch(stmthp, errhp, 1, 0, 0);
        row = NULL;
        for (i = 0; i < num_cols; ++i)
            if (res != OCI_NO_DATA)
                switch (columns[i].dtype) {
                case SQLT_NUM:
                case SQLT_CHR:
                    append_string_to_list((char*)data_row[i], &row);
                    break;
                }
        append_list_to_list(row, row_list);
    } while (res != OCI_NO_DATA &&
             calculate_resp_size(row_list) < MAX_RESP_SIZE);

    /* Release the bound variables memeory */
    for (i = 0; i < num_cols; ++i)
        if(data_row[i] != NULL)
            free(data_row[i]);
    free(data_row);

    /* cleanup only if data fetch is finished */
    if (res == OCI_NO_DATA)
        checkerr(errhp, OCIStmtRelease(stmthp, errhp, (OraText *) NULL, 0, OCI_DEFAULT));

    free(defnhp);

    if(function_success != SUCCESS)
        return ERROR;

    REMOTE_LOG("Port: Returning Rows...");

    return (res != OCI_NO_DATA ? MORE : DONE);
}

/*
 * checkerr0: This function prints a detail error report.
 *			  Used to "warp" invocation of OCI calls.
 * Parameters:
 *	handle (IN)	- can be either an environment handle or an error handle.
 *				  for OCI calls that take in an OCIError Handle:
 *				  pass in an OCIError Handle
 *
 *				  for OCI calls that don't take an OCIError Handle,
 *                pass in an OCIEnv Handle
 *
 * htype (IN)   - type of handle: OCI_HTYPE_ENV or OCI_HTYPE_ERROR
 *
 * status (IN)  - the status code returned from the OCI call
 *
 * Notes:
 *				  Note that this "exits" on the first
 *                OCI_ERROR/OCI_INVALID_HANDLE.
 *				  CUSTOMIZE ACCORDING TO YOUR ERROR HANDLING REQUIREMNTS
 */
void checkerr0(void *handle, ub4 htype, sword status, const char * function_name, int line_no)
{
    /* a buffer to hold the error message */
    text errbuf[2048];
    gerrcode = 0;
    function_success = SUCCESS;

    switch (status) {
    case OCI_SUCCESS:
#ifdef TRACE
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Ok - OCI_SUCCESS\n", function_name, line_no);
#endif
        break;
    case OCI_SUCCESS_WITH_INFO:
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_SUCCESS_WITH_INFO\n", function_name, line_no);
        break;
    case OCI_NEED_DATA:
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_NEED_DATA\n", function_name, line_no);
        break;
    case OCI_NO_DATA:
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_NO_DATA\n", function_name, line_no);
        break;
    case OCI_ERROR:
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_ERROR\n", function_name, line_no);
        if (handle) {
            (void) OCIErrorGet(handle, 1, (text *) NULL, &gerrcode,
                               errbuf, (ub4)sizeof(errbuf), htype);
            (void) sprintf_s(gerrbuf, sizeof(gerrbuf), " - %.*s\n", sizeof(errbuf), errbuf);
	        function_success = CONTINUE_WITH_ERROR;
        } else {
            (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] NULL Handle\n", function_name, line_no);
            (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Unable to extract detailed diagnostic information\n", function_name, line_no);
	        function_success = FAILURE;
        }
        break;
    case OCI_INVALID_HANDLE:
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_INVALID_HANDLE\n", function_name, line_no);
        break;
    case OCI_STILL_EXECUTING:
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_STILL_EXECUTING\n", function_name, line_no);
        break;
    case OCI_CONTINUE:
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Error - OCI_CONTINUE\n", function_name, line_no);
        break;
    default:
        (void) sprintf_s(gerrbuf, sizeof(gerrbuf), "[%s:%d] Unknown - %d\n", function_name, line_no, status);
        break;
    }
}
