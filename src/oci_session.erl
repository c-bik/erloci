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

-module(oci_session).
-behaviour(gen_server).

-include("oci.hrl").

%% API
-export([
    execute_sql/4,
    get_rows/1,
    open/2,
    close/1,
    get_columns/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    status=closed,
    port,
    pool,
    session_id,
    result_set = [],
    statement,
    max_rows,
    columns = []
}).

%% External API

%% @doc open new session
-spec open(PortPid::pid(), PoolPid::pid()) -> {ok, SessionPid::pid()} | {error, Error::term()}.
open(Port, Pool) ->
    gen_server:start(?MODULE, [Port, Pool], []).

%% @doc closes a session, can be used in a parameterized fashion Session:close()
-spec close(Session::session()) -> stopped.
close({?MODULE, Pid}) ->
    gen_server:call(Pid, stop).

%% @doc Executes an UPDATE/DELETE/INSERT SQL query, can be used in a parameterized fashion Session:execute_sql(...)
-spec execute_sql(Query :: string()|binary(),
    Params :: [{
            Type :: atom(),
            Dir :: atom(),
            Value :: integer() | float() | string()
        }],
    MaxRows :: integer(),
    Session :: session()) ->
    {ok, StatementHandle :: integer()} |
    {ok, RetArgs :: [integer()|float()|string()]} |
    {error, Reason :: ora_error()}.
execute_sql(Query, Params, MaxRows, {?MODULE, Pid}) when is_list(Query) ->
    gen_server:call(Pid, {execute_sql, Query, Params, MaxRows}).

%% @doc Fetches the resulting rows of a previously executed sql statement, can be used in a parameterized fashion Session:get_rows()
-spec get_rows(Session::session()) ->  [Row::tuple()] | {error, Reason::ora_error()}.
get_rows({?MODULE, Pid}) ->
    gen_server:call(Pid, get_rows).

%% @doc Fetches the column information of the last executed query, can be used in a parameterized fashion Session:get_columns()
-spec get_columns(Session::session()) -> [{ColumnName::string(), Type::atom(), Length::integer()}].
get_columns({?MODULE, Pid}) ->
    gen_server:call(Pid, get_columns).

%% Callbacks
init([Port, Pool]) ->
    {ok, SessionId} = oci_port:call(Port, {?GET_SESSION}),
    {ok, #state{port=Port, pool=Pool, session_id=SessionId, status=open}}.

handle_call(get_columns, _From, #state{columns=Columns} = State) ->
    {reply, {ok, Columns}, State};

handle_call({execute_sql, Query, Params, MaxRows}, _From, #state{session_id=SessionId, port=Port, result_set=[]} = State) ->
    CorrectedParams =
    case re:run(Query, "([:])", [global]) of
        {match, ArgList} ->
            if length(Params) /= length (ArgList) ->
                    {error, badarg};
                true ->
                    correct_params(Params, [])
            end;
        nomatch ->
            []
    end,
    {reply, Reply, NewState} = exec(State, Port, SessionId, Query, CorrectedParams),
    {reply, Reply, NewState#state{result_set=[], max_rows=MaxRows}};

handle_call(get_rows, _From, #state{max_rows=MaxRows, result_set=ResultSet, statement=undefined} = State) when length(ResultSet) =< MaxRows ->
    {reply, ResultSet, State#state{result_set=[]}};
handle_call(get_rows, _From, #state{max_rows=MaxRows, result_set=ResultSet} = State) when length(ResultSet) > MaxRows ->
    {Ret, RestRows} = lists:split(MaxRows, ResultSet),
    {reply, Ret, State#state{result_set=RestRows}};
handle_call(get_rows, _From, #state{max_rows=MaxRows, result_set=ResultSet0, port=Port, statement=Statement, session_id=SessionId} = State) when Statement /= undefined->
    case oci_port:call(Port, SessionId, {?FETCH_ROWS, SessionId, Statement}) of
        {{rows, Rows}, QStatus} ->
            ResultSet = ResultSet0 ++ Rows,
            {ReturnedRes, Rest} =
            if length(ResultSet) > MaxRows ->
                lists:split(MaxRows, ResultSet);
            true ->
                {ResultSet, []}
            end,
            case QStatus of
                more ->
                    {reply, ReturnedRes, State#state{result_set=Rest}};
                done ->
                    {reply, ReturnedRes, State#state{result_set=Rest, statement=undefined}}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State#state{result_set=[], statement=undefined}}
    end;
handle_call(get_rows, _From, #state{result_set=[], statement=undefined} = State) ->
    {reply, {error, no_statement}, State};

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call(Request, _Froqm, State) ->
    io:format("unknown Request ~p",[{Request, State}]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{port = Port, session_id = SessionId}) ->
    case oci_port:call(Port, SessionId, {?RELEASE_SESSION, SessionId}) of
        {ok} ->
            ok;
        {error, _Reason} ->
            ok
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
correct_params([], Acc) ->
    Acc;
correct_params([{Type,Dir,Val}|Params], Acc0) when is_atom(Type), is_atom(Dir), is_list(Acc0)->
    Acc1 = correct_params(Params, Acc0),
    NewType =
    case Type of
        sql_number ->
            ?NUMBER;
        sql_string ->
            ?STRING
    end,
    NewDir =
    case Dir of
        in ->
            ?ARG_DIR_IN;
        out ->
            ?ARG_DIR_OUT;
        inout ->
            ?ARG_DIR_INOUT
    end,
    NewVal = if is_list(Val) -> list_to_binary(Val);
        true -> Val
    end,
    [{NewType,NewDir,NewVal} | Acc1].

exec(State, Port, SessionId, Query, Params) ->
    case oci_port:call(Port, SessionId, {?EXEC_SQL, SessionId, binary:list_to_bin(Query), Params}) of
        {executed, no_ret} ->
            {reply, ok, State#state{statement=undefined}};
        {executed, RetArgs} ->
            {reply, {ok, RetArgs}, State#state{statement=undefined}};
        {columns, StatementHandle, Columns} ->
            {reply, ok, State#state{statement=StatementHandle, columns=Columns}};
        {error, Reason} ->
            {reply, {error, Reason}, State#state{statement=undefined}}
    end.
