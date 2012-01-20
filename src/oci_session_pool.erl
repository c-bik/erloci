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

-module(oci_session_pool).
-behaviour(gen_server).

-include("oci.hrl").

%% API
-export([
    start_link/6,
    start_link/4,
    stop/1,
    get_session/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% test exports
-export([
    integration_test/5,
    mock_test/0]).

-record(state, {
        port,
        pool_name
    }).

%% External API
%% @doc see create_session_pool/4
-spec start_link(Host::string(),
    Port :: integer(),
    Service :: {sid, ServiceId::string()} | {service, Service::string()},
    Username::string(),
    Password :: string(),
    Options :: [{atom(), boolean()|integer()}]
) -> {ok, SessionPoolName::string()} | {error, Reason::ora_error()}.
start_link(Host, Port, {sid, Sid}, UserName, Password, Options)
        when is_list(Host), is_integer(Port), is_list(Sid), is_list(UserName), is_list(Password), is_list(Options) ->
    TnsNameStr = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp) (HOST=" ++ Host ++ ") (PORT=" ++ integer_to_list(Port) ++ "))(CONNECT_DATA=(SID=" ++ Sid ++ ")))",
    start_link(TnsNameStr, UserName, Password, Options);
start_link(Host, Port, {service, Service}, UserName, Password, Options)
        when is_list(Host), is_integer(Port), is_list(Service), is_list(UserName), is_list(Password), is_list(Options) ->
    TnsNameStr = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp) (HOST=" ++ Host ++ ") (PORT=" ++ integer_to_list(Port) ++ "))(CONNECT_DATA=(SERVICE_NAME=" ++ Service ++ ")))",
    start_link(TnsNameStr, UserName, Password, Options).

%% @doc Spawns an erlang control process that will open a port
%%      to a c-process that uses the ORACLE OCI API to open a connection
%%      to the database.  ``ConnectionStr'' can contain
%%      "user/password@database" connection string. Following options are
%%      currently supported:
%%          {autocommit, boolean()}
%%          {max_rows, integer()}
%%          {query_cache_size, integer()}
%%
%%      ``autocommit'' will automatically commit the query, ``max_rows'' option limits
%%      the max number of elements in the list of rows returned by a select
%%      statement.  ``query_cache_size'' controls the size of the driver's cache
%%      that stores prepared query statements.  Its default value is 50.
%%      The port program can be started in debug mode by specifying
%%      {mod, {ora, [{debug, true}]}} option in the ora.app file.
-spec start_link(ConnectionStr::string(),
    Username::string(),
    Password :: string(),
    Options :: [{atom(), boolean()|integer()}]
) -> {ok, SessionPoolPid::pid()} | {error, Error::term()}.
start_link(TnsNameStr, UserName, Password, Options) when is_list(TnsNameStr) and is_list(Options) ->
    gen_server:start_link(?MODULE, [TnsNameStr, UserName, Password, Options], []).

%% @doc stops the connection pool process
-spec stop(SessionPoolPid::pid()) -> ok.
stop(SessionPoolPid) ->
    gen_server:call(SessionPoolPid, stop).

%% @doc get a new Session from the SessionPool specified. We return you a parameterized module
-spec get_session(SessionPoolPid::pid()) -> Session::session().
get_session(SessionPoolPid) ->
    gen_server:call(SessionPoolPid, get_session).

%% TODO
%%log(SessionPoolPid, enable) ->
%%    gen_server:call(SessionPoolPid, enable_log);
%%log(SessionPoolPid, disable) ->
%%    gen_server:call(SessionPoolPid, disable_log).

%% Callbacks
init([TnsNameStr, UserName, Password, Options]) ->
    PortOptions = proplists:get_value(port_options, Options),
    PoolOptions = proplists:delete(port_options, Options),
    {ok, PortPid} = oci_port:start_link(PortOptions),
    {ok, PoolName} = oci_port:call(PortPid, {
            ?CREATE_SESSION_POOL,
            binary:list_to_bin(TnsNameStr),
            binary:list_to_bin(UserName),
            binary:list_to_bin(Password),
            binary:list_to_bin(PoolOptions)}),
    {ok, #state{port=PortPid, pool_name=PoolName}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call(get_session, _From, #state{port=PortPid} = State) ->
    {ok, SessionPid} = oci_session:open(PortPid, self()),
    %% we return a parameterized module here, we don't use
    %% oci_session:new() though, we use the tuple notation
    %% directly. this lets you call every function in oci_session
    %% by giving the SessionPid as a last parameter, therefore:
    %% Session:get_rows() == oci_session:get_rows(SessionPid)
    {reply, {oci_session, SessionPid}, State};

handle_call(enable_log, _From, #state{port=PortPid} = State) ->
    oci_port:call(PortPid, {?R_DEBUG_MSG, ?DBG_FLAG_ON}),
    {reply, ok, State};
handle_call(disable_log, _From, #state{port=PortPid} = State) ->
    oci_port:call(PortPid, {?R_DEBUG_MSG, ?DBG_FLAG_OFF}),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{port=PortPid}) ->
    case is_process_alive(PortPid) of
        true ->
            oci_port:call(PortPid, {?RELEASE_SESSION_POOL});
        _ ->
            ok
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal Functions

%% Test
integration_test(Host, Port, Service, UserName, Password) ->
    {ok, Pool} = oci_session_pool:start_link(Host, Port, Service, UserName, Password,""),
    Session = oci_session_pool:get_session(Pool),
    Session:execute_sql("drop table test_erloci", [], 10),
    ok = Session:execute_sql("create table test_erloci(pkey number,  publisher varchar2(100), rank number, hero varchar2(100), real varchar2(100), votes number, votes_first_rank number)", [], 10),
    insert_rows(Session),
    ok = Session:execute_sql("select * from test_erloci", [], 10),
    [true] = sets:to_list(sets:from_list([Session:get_rows() /= [] || _ <- lists:seq(1,10)])),
    [] = Session:get_rows(),
    stopped = Session:close(),
    stopped = oci_session_pool:stop(Pool).

mock_test() ->
    {ok, Pool} = oci_session_pool:start_link("127.0.0.1", 1521, {service, "db.local"}, "dba", "supersecret", [{port_options, [{mock_port, oci_port_mock}]}]),
    Session = oci_session_pool:get_session(Pool),
    ok = Session:execute_sql("select * from test_erloci", [], 10),
    [true] = sets:to_list(sets:from_list([Session:get_rows() /= [] || _ <- lists:seq(1,10)])),
    [] = Session:get_rows(),

    %% odd max_num
    ok = Session:execute_sql("select * from test_erloci", [], 3),
    [true] = sets:to_list(sets:from_list([Session:get_rows() /= [] || _ <- lists:seq(1,34)])),
    [] = Session:get_rows(),
    stopped = Session:close(),
    stopped = oci_session_pool:stop(Pool).

insert_rows(Session) ->
    [
        ok =
        Session:execute_sql(
            binary_to_list(iolist_to_binary([
                        "insert into test_erloci values(",
                        integer_to_list(PKey), ",",
                        "'", Publisher, "',",
                        integer_to_list(Rank), ",",
                        "'", Hero, "',",
                        "'", Real, "',",
                        integer_to_list(Votes), ",",
                        integer_to_list(VotesFirstRank),
                        ")"])), [], 10)
        || [PKey, Publisher, Rank, Hero, Real, Votes, VotesFirstRank] <- port_mock:rows()].
