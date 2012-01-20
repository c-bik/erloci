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

#include "cmd_processors.h"

static const struct cmdtable g_cmdtable[] = {
    {CREATE_SESSION_POOL, 4, "Create TNS session pool"},
    {GET_SESSION,	 0, "Get a session from the TNS session pool"},
    {RELEASE_SESSION, 1, "Return a previously allocated connection back to the pool"},
    {EXEC_SQL,			 3, "Execute a SQL query"},
    {FETCH_ROWS,		 2,	"Fetches the rows of a previously executed SELECT query"},
    {R_DEBUG_MSG,		 1, "Remote debugging turning on/off"},
    {FREE_SESSION_POOL,		 0, "Release Session Pool"},
    {QUIT,				 0, "Exit the port process"},
};

#define CMD_ARGS_COUNT(_cmd) g_cmdtable[_cmd].arg_count

bool change_log_flag(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(R_DEBUG_MSG)];
    ETERM *resp;

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(R_DEBUG_MSG))
        goto error_exit;

    MAP_ARGS(command, args);

    // Args : {log, DBG_FLAG_OFF/DBG_FLAG_ON}
    if(ERL_IS_INTEGER(args[0])) {

        REMOTE_LOG("Port: RX CMD - logging %d", ERL_INT_UVALUE(args[0]));

        switch(ERL_INT_UVALUE(args[0])) {
        case DBG_FLAG_OFF:
            REMOTE_LOG("Port: Disabling logging...");
            log_flag = false;
            REMOTE_LOG("Port: This line will never show up!!");
            resp = erl_format((char*)"{~i,ok,log_disabled}", R_DEBUG_MSG);
            break;
        case DBG_FLAG_ON:
            log_flag = true;
            resp = erl_format((char*)"{~i,ok,log_enabled}", R_DEBUG_MSG);
            REMOTE_LOG("Port: Enabled logging...");
            break;
        default:
            resp = erl_format((char*)"{~i,error,badarg}", R_DEBUG_MSG);
            break;
        }
    } else {
error_exit:
        resp = erl_format((char*)"{~i,error,badarg}", R_DEBUG_MSG);
    }

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    delete args;
    return ret;
}

bool cmd_create_tns_ssn_pool(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(CREATE_SESSION_POOL)];
    ETERM *resp;

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(CREATE_SESSION_POOL))
        goto error_exit;

    MAP_ARGS(command, args);

    // Args : Connection String, User name, Password, Options
    if(ERL_IS_BINARY(args[0]) &&
       ERL_IS_BINARY(args[1]) &&
       ERL_IS_BINARY(args[2]) &&
       ERL_IS_BINARY(args[3])) {

        REMOTE_LOG("Port: RX CMD - create session pool with TNS string.");

        LOG_ARGS(ARG_COUNT(command), args, "Create Pool");

        if (oci_create_tns_seesion_pool(
                ERL_BIN_PTR(args[0]), ERL_BIN_SIZE(args[0]),	// Connect String
                ERL_BIN_PTR(args[1]), ERL_BIN_SIZE(args[1]),	// User Name String
                ERL_BIN_PTR(args[2]), ERL_BIN_SIZE(args[2]),	// Password String
                ERL_BIN_PTR(args[3]), ERL_BIN_SIZE(args[3])))	// Options String
            resp = erl_format((char*)"{~i,ok,~s}", CREATE_SESSION_POOL,session_pool_name);
        else {
            int err_str_len = 0;
            char * err_str;
            get_last_error(NULL, err_str_len);
            err_str = new char[err_str_len+1];
            get_last_error(err_str, err_str_len);
#if DEBUG <= DBG_3
            fprintf(fp_log, "CMD: Connect ERROR - %s", err_str);
#endif
            resp = erl_format((char*)"{~i,error,~s}", CREATE_SESSION_POOL, err_str);
        }
    } else {
error_exit:
        resp = erl_format((char*)"{~i,error,badarg}", CREATE_SESSION_POOL);
    }

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    delete args;
    return ret;
}

bool cmd_free_ssn_pool(ETERM * command)
{
    bool ret = false;
    ETERM *resp = NULL;

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(FREE_SESSION_POOL))
        goto error_exit;

    // Args : none
    REMOTE_LOG("Port: RX CMD - free session pool.");

    if (oci_free_session_pool())
        resp = erl_format((char*)"{~i,ok}", FREE_SESSION_POOL);
    else {
        int err_str_len = 0;
        char * err_str;
        get_last_error(NULL, err_str_len);
        err_str = new char[err_str_len+1];
        get_last_error(err_str, err_str_len);
#if DEBUG <= DBG_3
        fprintf(fp_log, "CMD: Connect ERROR - %s", err_str);
#endif
        resp = erl_format((char*)"{~i,error,~s}", FREE_SESSION_POOL, err_str);
    }

error_exit:
    if(resp == NULL)
        resp = erl_format((char*)"{~i,error,badarg}", FREE_SESSION_POOL);

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    return ret;
}

bool cmd_get_conn(ETERM * command)
{
    bool ret = false;
    ETERM *resp;

    if(ARG_COUNT(command) == 0) {
        void * conn_handle = NULL;

        REMOTE_LOG("Port: RX CMD - get connection from session pool");

        if ((conn_handle = oci_get_connection_from_pool()) != NULL)
            resp = erl_format((char*)"{~i,ok,~i}", GET_SESSION, (unsigned long)conn_handle);
        else {
            int err_str_len = 0;
            char * err_str;
            get_last_error(NULL, err_str_len);
            err_str = new char[err_str_len+1];
            get_last_error(err_str, err_str_len);
            LOG_ERROR("Connect", err_str);
            resp = erl_format((char*)"{~i,error,~s}", GET_SESSION, err_str);
        }
    } else
        resp = erl_format((char*)"{~i,error,badarg}", GET_SESSION);

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    return ret;
}

bool cmd_release_conn(ETERM * command)
{
    bool ret = false;
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(RELEASE_SESSION)];
    ETERM * resp;

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(RELEASE_SESSION))
        goto error_exit;

    MAP_ARGS(command, args);

    if(ERL_IS_INTEGER(args[0])) {
        void * conn_handle = (void *)ERL_INT_UVALUE(args[0]);;

        REMOTE_LOG("Received Command release connection to session pool");

        if (oci_return_connection_to_pool(conn_handle))
            resp = erl_format((char*)"{~i,~i,{ok}}", RELEASE_SESSION, (unsigned long)conn_handle);
        else {
            int err_str_len = 0;
            char * err_str;
            get_last_error(NULL, err_str_len);
            err_str = new char[err_str_len+1];
            get_last_error(err_str, err_str_len);
            LOG_ERROR("Connect Return", err_str);
            resp = erl_format((char*)"{~i,~i,{error,~s}}", RELEASE_SESSION, (unsigned long)conn_handle, err_str);
            delete err_str;
        }
    } else {
error_exit:
        resp = erl_format((char*)"{~i,error,badarg}", RELEASE_SESSION);
    }

    if(write_resp(resp) < 0)
        ret = true;

    erl_free_compound(command);

    delete args;
    return ret;
}

bool cmd_exec_sql(ETERM * command)
{
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(EXEC_SQL)];
    ETERM * resp;

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(EXEC_SQL))
        goto error_exit_pre;

    MAP_ARGS(command, args);

    // Args: Conn Handle, Sql Statement
    if(ERL_IS_INTEGER(args[0])	&&
       ERL_IS_BINARY(args[1])	&&
       ERL_IS_LIST(args[2])) {

        REMOTE_LOG("Port: RX CMD - SQL");
        LOG_ARGS(ARG_COUNT(command), args, "Execute SQL");

        void * connection_handle = (void *)ERL_INT_UVALUE(args[0]);
        inp_t * bind_args = map_to_bind_args(args[2]);
        unsigned long statement_handle = 0;

        /* Transfer the columns */
        ETERM *columns = NULL;
        switch(oci_exec_sql(connection_handle, statement_handle, ERL_BIN_PTR(args[1]), ERL_BIN_SIZE(args[1]), bind_args, &columns)) {
			case SUCCESS:
				if (columns == NULL && bind_args != NULL) {
					resp = erl_format((char*)"{~i,~i,{executed,~w}}", EXEC_SQL, (unsigned long)connection_handle, build_term_from_bind_args(bind_args));
				} else if (columns == NULL && bind_args == NULL)
					resp = erl_format((char*)"{~i,~i,{executed,no_ret}}", EXEC_SQL, (unsigned long)connection_handle);
				else
					resp = erl_format((char*)"{~i,~i,{columns,~i,~w}}", EXEC_SQL, (unsigned long)connection_handle, statement_handle, columns);
				if(write_resp(resp) < 0) goto error_exit;
				break;
			case CONTINUE_WITH_ERROR: {
					int err_str_len = 0;
					char * err_str;
					get_last_error(NULL, err_str_len);
					err_str = new char[err_str_len+1];
					get_last_error(err_str, err_str_len);
					LOG_ERROR("Execute SQL", err_str);
					resp = erl_format((char*)"{~i,~i,{error,~s}}", EXEC_SQL, (unsigned long)connection_handle, err_str);
					delete err_str;
					write_resp(resp);
				}
				break;
			case FAILURE: {
					int err_str_len = 0;
					char * err_str;
					get_last_error(NULL, err_str_len);
					err_str = new char[err_str_len+1];
					get_last_error(err_str, err_str_len);
					LOG_ERROR("Execute SQL", err_str);
					resp = erl_format((char*)"{~i,~i,{error,~s}}", EXEC_SQL, (unsigned long)connection_handle, err_str);
					delete err_str;
					write_resp(resp);
					goto error_exit;
				}
        }
#if 0
        /* Transfer the rows */
        if (columns != NULL) {
            ROW_FETCH row_fetch = MORE;
            ETERM *rows = NULL;
            do {
                rows = NULL;
                row_fetch = oci_produce_rows((void *)statement_handle, &rows);
                if (row_fetch != ERROR) {
                    if (rows == NULL) {
                        resp = erl_format((char*)"{~i,~i,{{rows,[]},done}}", EXEC_SQL, (unsigned long)connection_handle);
                        if(write_resp(resp) < 0) goto error_exit;
                        break;
                    }
                    resp = erl_format((char*)"{~i,~i,{{rows,~w},~a}}", EXEC_SQL, (unsigned long)connection_handle, rows, (row_fetch == MORE ? "more" : "done"));
                    if(write_resp(resp) < 0) goto error_exit;
                }
            } while (row_fetch == MORE);
        }
#endif
    } else {
error_exit_pre:
        resp = erl_format((char*)"{~i,error,badarg}", EXEC_SQL);
        if(write_resp(resp) < 0) goto error_exit;
    }
    erl_free_compound(command);
    delete args;
    return false;

error_exit:
    erl_free_compound(command);
    delete args;
    return true;
}

bool cmd_fetch_rows(ETERM * command)
{
    ETERM **args = new ETERM*[CMD_ARGS_COUNT(FETCH_ROWS)];
    ETERM * resp;

    if(ARG_COUNT(command) != CMD_ARGS_COUNT(FETCH_ROWS))
        goto error_exit_pre;

    MAP_ARGS(command, args);

    // Args: Connection Handle, Statement Handle
    if(ERL_IS_INTEGER(args[0]) &&
       ERL_IS_INTEGER(args[1])) {

        REMOTE_LOG("Port: RX CMD - Fetch Rows");

        void * connection_handle = (void *)ERL_INT_UVALUE(args[0]);
        void * statement_handle = (void *)ERL_INT_UVALUE(args[1]);

        /* Transfer the rows */
        if (statement_handle != NULL) {
            ETERM *rows = NULL;
            ROW_FETCH row_fetch = oci_produce_rows(statement_handle, &rows);
            if (row_fetch != ERROR) {
                if (rows == NULL) {
                    resp = erl_format((char*)"{~i,~i,{{rows,[]},done}}", FETCH_ROWS, (unsigned long)connection_handle);
                    if(write_resp(resp) < 0) goto error_exit;
                } else {
                    resp = erl_format((char*)"{~i,~i,{{rows,~w},~a}}", FETCH_ROWS, (unsigned long)connection_handle, rows, (row_fetch == MORE ? "more" : "done"));
                    if(write_resp(resp) < 0) goto error_exit;
                }
            }
        }
    } else {
error_exit_pre:
        resp = erl_format((char*)"{~i,error,badarg}", FETCH_ROWS);
        if(write_resp(resp) < 0) goto error_exit;
    }
    erl_free_compound(command);
    delete args;
    return false;

error_exit:
    erl_free_compound(command);
    delete args;
    return true;
}

bool cmd_processor(ETERM * command)
{
    ETERM *cmd = erl_element(1, command);

    if(ERL_IS_INTEGER(cmd)) {
        switch(ERL_INT_VALUE(cmd)) {
        case CREATE_SESSION_POOL:
#if DEBUG <= DBG_3
            fprintf(fp_log, "CMD: Create session with TNS.\n");
            fflush(fp_log);
#endif
            return cmd_create_tns_ssn_pool(command);
        case GET_SESSION:
#if DEBUG <= DBG_3
            fprintf(fp_log, "CMD: Get connection from the pool.\n");
            fflush(fp_log);
#endif
            return cmd_get_conn(command);
        case RELEASE_SESSION:
#if DEBUG <= DBG_3
            fprintf(fp_log, "CMD: Release connection to the pool.\n");
            fflush(fp_log);
#endif
            return cmd_release_conn(command);
        case EXEC_SQL:
#if DEBUG <= DBG_3
            fprintf(fp_log, "CMD: Execute SQL.\n");
            fflush(fp_log);
#endif
            return cmd_exec_sql(command);
        case FETCH_ROWS:
            return cmd_fetch_rows(command);
        case R_DEBUG_MSG:
            return change_log_flag(command);
        case FREE_SESSION_POOL:
            return cmd_free_ssn_pool(command);
        case QUIT:
        default:
            return true;
        }
    }
    return true;
}
