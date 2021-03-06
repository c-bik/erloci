%% Copyright 2012 K2Informatics GmbH, Root Laengenbold, Switzerland
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% Process
-define(PORT_TIMEOUT, 			15000).
-define(WAIT_RESULT_TIMEOUT, 	15000).

%% Exec name
-define(EXE_NAME, "erloci").

%% Interface commands
-define(CREATE_SESSION_POOL,    0).
-define(GET_SESSION,		    1).
-define(RELEASE_SESSION,        2).
-define(EXEC_SQL,			    3).
-define(FETCH_ROWS,			    4).
-define(R_DEBUG_MSG,		    5).
-define(RELEASE_SESSION_POOL,	6).
-define(QUIT,				    7).

% Argument Types
-define(ARG_DIR_IN,			0).
-define(ARG_DIR_OUT,		1).
-define(ARG_DIR_INOUT,		2).

% Datatype Codes
-define(NUMBER,				0).
-define(STRING,				1).

-define(APP_START_TIME, 	1000).

% Debug flags
-define(DBG_FLAG_OFF, 		0).
-define(DBG_FLAG_ON, 		1).

%% types
-type session() :: {oci_session, pid()}.  %% parameterized Module
-type statement() :: {oci_session, any(), pid()}. %% parameterized Module
-type ora_error() :: string().            %% Oracle Error String
