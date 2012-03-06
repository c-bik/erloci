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
    execute_sql/5,
    next_rows/1,
    prev_rows/1,
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
    statements = []
}).
-record(statement, {
    ref,
    handle,
    max_rows,
    columns,
    results,
    cache_size = 0,
    use_cache = false,
    cursor = {0, {}, more}


}).

%% External API

%% @doc open new session
-spec open(PortPid::pid(), PoolPid::pid()) -> {ok, SessionPid::pid()} | {error, Error::term()}.
open(Port, Pool) ->
    gen_server:start(?MODULE, [Port, Pool], []).

%% @doc closes a session, can be used in a parameterized fashion Session:close() or Statement:close()
-spec close(session() | statement()) -> stopped|ok.
close({?MODULE, Pid}) ->
    gen_server:call(Pid, stop);
close({?MODULE, StatementRef, Pid}) ->
    gen_server:call(Pid, {close_statement, StatementRef}).

%% @doc Executes an UPDATE/DELETE/INSERT SQL query, can be used in a parameterized fashion Session:execute_sql(...)
-spec execute_sql(Query :: string()|binary(),
    Params :: [{
            Type :: atom(),
            Dir :: atom(),
            Value :: integer() | float() | string()
        }],
    MaxRows :: integer(),
    Session :: session()) ->
    {statement, statement()} |
    {ok, RetArgs :: [integer()|float()|string()]} |
    {error, Reason :: ora_error()}.
execute_sql(Query, Params, MaxRows, {?MODULE, Pid}) when is_list(Query) ->
    execute_sql(Query, Params, MaxRows, false, {?MODULE, Pid}).
execute_sql(Query, Params, MaxRows, UseCache, {?MODULE, Pid}) when is_list(Query) ->
    gen_server:call(Pid, {execute_sql, Query, Params, MaxRows, UseCache}).

%% @doc Fetches the resulting rows of a previously executed sql statement, can be used in a parameterized fashion Session:get_rows()
-spec next_rows(Statement::statement()) ->  [Row::tuple()] | {error, Reason::ora_error()}.
next_rows({?MODULE, StatementRef, Pid}) ->
    gen_server:call(Pid, {next_rows, StatementRef}).

prev_rows({?MODULE, StatementRef, Pid}) ->
    gen_server:call(Pid, {prev_rows, StatementRef}).

%% @doc Fetches the column information of the last executed query, can be used in a parameterized fashion Session:get_columns()
-spec get_columns(Statement::statement()) -> [{ColumnName::string(), Type::atom(), Length::integer()}].
get_columns({?MODULE, StatementRef, Pid}) ->
    gen_server:call(Pid, {get_columns, StatementRef}).

%% Callbacks
init([Port, Pool]) ->
    {ok, SessionId} = oci_port:call(Port, {?GET_SESSION}),
    {ok, #state{port=Port, pool=Pool, session_id=SessionId, status=open}}.

handle_call({get_columns, StatementRef}, _From, #state{statements=Statements} = State) ->
    S = proplists:get_value(StatementRef, Statements),
    {reply, {ok, S#statement.columns}, State};

handle_call({execute_sql, Query, Params, MaxRows, UseCache}, _From, #state{session_id=SessionId, port=Port} = State) ->
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
    exec(State, Port, SessionId, Query, CorrectedParams, MaxRows, UseCache);
handle_call({prev_rows, StatementRef}, _From, #state{statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        results=Results,
        use_cache=UseCache,
        cursor={Offset, Cursor, QStatus},
        max_rows=MaxRows
    } = Statement,

    case UseCache of
        true when Offset > 0 ->
            {NewOffset, Keys} =
            case Offset - MaxRows of
                R when R > 0 ->
                    K = get_keys(Offset-MaxRows-MaxRows+1, Offset-MaxRows),
                    {Offset-length(K), K};
                R when R < 0 ->
                    K = get_keys(1, Offset),
                    {0, K};
                0 ->
                    {0, []}
            end,
            Rows = [ets:lookup_element(Results, K1, 2)||K1<-Keys],
            BinRef =
            case Cursor of
                {_,_,_,_,B,_,_,_} ->
                    B;
                {'$end_of_table', B} ->
                    B
            end,
            NewCursor = {Results, NewOffset, [], MaxRows, BinRef, [], 0, 0},
            NewStatement = Statement#statement{
                cursor={NewOffset, NewCursor, QStatus}},
            NewStatements = proplists:delete(StatementRef, Statements) ++ [{StatementRef, NewStatement}],
            {reply, Rows, State#state{statements=NewStatements}};
        _ ->
            {reply, [], State}
    end;

handle_call({next_rows, StatementRef}, _From, #state{port=Port, session_id=SessionId, statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        handle=StatementHandle,
        results=Results,
        cache_size=CacheSize,
        use_cache=UseCache,
        cursor={Offset, Cursor, QStatus},
        max_rows=MaxRows
    } = Statement,

    {NewQStatus, NrOfNewCacheEntries} =
    case
        ((size(Cursor) > 2) or (Cursor == {}))and
        (QStatus == more) and
        ((CacheSize - Offset) =< MaxRows)
    of
        true ->
            fetch(Port, SessionId, StatementHandle, Results, CacheSize);
        false ->
            {QStatus, 0}
    end,

    {Rows, NewCursor} =
    case Cursor of
        {'$end_of_table', BinRef} ->
            {[], {'$end_of_table', BinRef}};
        {} ->
            %% first call to next_rows
            match_object(ets:match_object(Results, '$1', MaxRows), undefined);
        Cursor when (UseCache == false) and (NrOfNewCacheEntries > 0) ->
            {_,_,_,_,BinRef,_,_,_} = Cursor,
            match_object(ets:match_object(Results, '$1', MaxRows), BinRef);
        Cursor ->
            {_,_,_,_,BinRef,_,_,_} = Cursor,
            match_object(ets:match_object(Cursor), BinRef)
    end,

    %% delete results if no_cache
    case UseCache of
        false ->
            [ets:delete(Results, I) || {I,_} <- Rows];
        true ->
            ok
    end,

    NewStatement = Statement#statement{
        cache_size=CacheSize + NrOfNewCacheEntries,
        cursor={Offset + length(Rows), NewCursor, NewQStatus}},
    NewStatements = proplists:delete(StatementRef, Statements) ++ [{StatementRef, NewStatement}],
    RetRows = [R || {_,R} <-Rows],
    {reply, RetRows, State#state{statements=NewStatements}};

handle_call({close_statement, StatementRef}, _From, #state{statements=Statements} = State) ->
    Statement = proplists:get_value(StatementRef, Statements),
    #statement{
        results=Results
    } = Statement,
    ets:delete(Results),
    NewStatements = proplists:delete(StatementRef, Statements),
    {reply, ok, State#state{statements=NewStatements}};

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

exec(State, Port, SessionId, Query, Params, MaxRows, UseCache) ->
    case oci_port:call(Port, SessionId, {?EXEC_SQL, SessionId, binary:list_to_bin(Query), Params}) of
        {executed, no_ret} ->
            {reply, ok, State};
        {executed, RetArgs} ->
            {reply, {ok, RetArgs}, State};
        {columns, StatementHandle, Columns} ->
            StatementRef = make_ref(),
            Results = ets:new(results, [ordered_set]),
            Statement = #statement{
                ref=StatementRef,
                handle=StatementHandle,
                columns=Columns,
                max_rows=MaxRows,
                results=Results,
                use_cache=UseCache
            },
            Statements = State#state.statements ++ [{StatementRef, Statement}],
            {reply, {statement, {?MODULE, StatementRef, self()}}, State#state{statements=Statements}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

fetch(Port, SessionId, StatementHandle, ResultsTid, CacheSize) ->
    case oci_port:call(Port, SessionId, {?FETCH_ROWS, SessionId, StatementHandle}) of
        {{rows, Rows}, NewStatus} ->
            NrOfRows = length(Rows),
            ets:insert(ResultsTid, [{I, R} || {I,R} <- lists:zip(lists:seq(CacheSize + 1, CacheSize + NrOfRows), Rows)]),
            {NewStatus, NrOfRows};
        {error, Reason} ->
            %% maybe we should close the statement here (and gc resuls)
            {error, Reason}
    end.

get_keys(Start, End) when (Start > 0) and (End > 0) ->
    lists:seq(Start, End);
get_keys(Start, End) when (Start =< 0) ->
    get_keys(1, End);
get_keys(Start, End) when (End =< 0) ->
    get_keys(Start, 1).

match_object('$end_of_table', CursorBinRef) ->
    {[], {'$end_of_table', CursorBinRef}};
match_object({Rows, '$end_of_table'}, CursorBinRef) ->
    {Rows, {'$end_of_table', CursorBinRef}};
match_object(Rows, _CursorBinRef) ->
    Rows.
