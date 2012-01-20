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

#include "oci_marshal.h"

#include "erl_interface.h"
#include "ei.h"

#include "cmd_processors.h"

#ifdef __WIN32__
#include <io.h>
#include <fcntl.h>
#endif

typedef unsigned char byte;

#define THREAD

#if DEBUG < DBG_5
// Global logger handle
FILE * fp_log = stdout;
#endif

bool log_flag;
bool exit_loop = false;

#ifdef __WIN32__
int _tmain(int argc, _TCHAR* argv[])
#else
int main(int argc, char * argv[])
#endif
{
    bool threaded = false;
    ETERM *cmd_tuple;

#ifdef __WIN32__
    _setmode( _fileno( stdout ), _O_BINARY );
    _setmode( _fileno( stdin  ), _O_BINARY );
#endif

#if DEBUG < DBG_5
#ifdef __WIN32__
    fopen_s(&fp_log, "oci.log", "w");
#else
    fp_log = fopen("oci.log", "w");
#endif
#endif

    erl_init(NULL, 0);
    log_flag = false;

    if (argc == 2) {
        if (
#ifdef __WIN32__
            wcscmp(argv[1], L"true") == 0
#else
            strcmp(argv[1], "true") == 0
#endif
        ) log_flag = true;
    }

    init_marshall();

    REMOTE_LOG("Port: OCI Process started...");

#ifdef THREAD
    threaded = InitializeThreadPool();
    if(threaded)
        REMOTE_LOG("Port: Thread pool created...");
#endif

    oci_init();
    REMOTE_LOG("Port: Initialized Oracle OCI");

#if DEBUG < DBG_5
    fprintf(fp_log, "Started...\n");
    fflush(fp_log);
#endif

    while(!exit_loop && (cmd_tuple = (ETERM *)read_cmd()) != NULL) {
#ifdef THREAD
        if(threaded && ProcessCommand(cmd_tuple)) {
            REMOTE_LOG("Port: Command sumitted to thread-pool for processing...");
        }
#else
        exit_loop = cmd_processor(cmd_tuple);
#endif

#if DEBUG < DBG_5
        fflush(fp_log);
#endif
    }

    REMOTE_LOG("Port: Process oci terminating...");
    oci_cleanup();

#ifdef THREAD
    REMOTE_LOG("Port: Thread pool destroyed...");
    CleanupThreadPool();
#endif

#if DEBUG < DBG_5
    fprintf(fp_log, "Finished!\n");
    fflush(fp_log);
    fclose(fp_log);
#endif

    return 0;
}
