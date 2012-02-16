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

-module(oci_port).
-behaviour(gen_server).

-include("oci.hrl").

%% API
-export([
    start_link/1,
    stop/0,
    call/2,
    call/3]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {
    port,
    port_mod = erlang,
    status = disconnected,
    logging = ?DBG_FLAG_ON,
    refs = ets:new(oci_refs, [])
}).

%% External API
start_link(Options) ->
    case Options of
        undefined ->
            gen_server:start_link(?MODULE, [erlang], []);
        Options when is_list(Options)->
            case proplists:get_value(mock_port, Options) of
                undefined ->
                    gen_server:start_link(?MODULE, [erlang], []);
                Mock  ->
                    gen_server:start_link(?MODULE, [Mock], [])
            end
    end.

stop() ->
    gen_server:call(?MODULE, stop).

call(PortPid, Msg) ->
    gen_server:call(PortPid, {port_synch_call, Msg}, ?PORT_TIMEOUT).

call(PortPid, SessionId, Msg) ->
    gen_server:call(PortPid, {port_call, SessionId, Msg}, ?PORT_TIMEOUT).

%% Callbacks
init([PortMod]) ->
    case os:find_executable(?EXE_NAME, "./priv/") of
        false ->
            case os:find_executable(?EXE_NAME, "./deps/erloci/priv/") of
                false -> {stop, bad_executable};
                Executable -> start_exe(PortMod, Executable)
            end;
        Executable -> start_exe(PortMod, Executable)
    end.

start_exe(PortMod, Executable) ->
    PortOptions = [{packet, 4}, binary, exit_status, use_stdio, {args, [ "true" ]}],
    case (catch PortMod:open_port({spawn_executable, Executable}, PortOptions)) of
        {'EXIT', Reason} ->
            io:fwrite("oci could not open port: ~p~n", [Reason]),
            {stop, Reason};
        Port ->
            io:fwrite("oci:init opened new port: ~p~n", [PortMod:port_info(Port)]),
            %% TODO -- Loggig it turned after port creation for the integration tests too run
            PortMod:port_command(Port, term_to_binary({?R_DEBUG_MSG, ?DBG_FLAG_OFF})),
            {ok, #state{status=connected, port=Port, port_mod=PortMod}}
    end.
%% We force some Port commands to be executed in a
%% synchronous manner, the main reason for this is
%% that the current port do not pass an request
%% identifier such as e.g. session_id  to every
%% response. Without the identifier we are not able
%% to map the response to the request in the
%% handle_info callback.
receive_loop(Port, L) ->
    receive
        {Port, {data, Data}} ->
            Res = binary_to_term(Data),
            Reply = handle_synch_result(L, Res),
            case tuple_to_list(Res) of
                [Cmd | _] when (Cmd =:= ?CREATE_SESSION_POOL)
                or (Cmd =:= ?GET_SESSION)
                or (Cmd =:= ?RELEASE_SESSION)
                or (Cmd =:= ?RELEASE_SESSION_POOL)
                ->
                    {ok, Reply};
                _ ->
                    receive_loop(Port, L)
            end
    end.

handle_call({port_synch_call, Msg}, _From, #state{status=connected, port=Port, port_mod=PMod, logging=L} = State) ->
    true = PMod:port_command(Port, term_to_binary(Msg)),
    {ok, Reply} = receive_loop(Port, L),
    {reply, Reply, State};

handle_call({port_call, SessionId, Msg}, From, #state{status=connected, port=Port, port_mod=PMod, refs=Refs} = State) ->
    ets:insert(Refs, {SessionId, From}),
    true = PMod:port_command(Port, term_to_binary(Msg)),
    {noreply, State}. %% we will reply inside handle_info_result

handle_cast(_Req, State) ->
    {noreply, State}.

%% We got a reply from a previously sent command to the Port.  Relay it to the caller.
handle_info({Port, {data, Data}}, #state{refs=Refs, port=Port} = State) when is_binary(Data) ->
    case handle_result(State#state.logging, binary_to_term(Data)) of
        {ok, {session_id, SessionId}, Result} ->
            [{_, From}] = ets:lookup(Refs, SessionId),
            ets:delete(Refs, SessionId),
            gen_server:reply(From, Result);
        {error, _Reason} ->
            %% TODO: we must get the sessionId also in the error case
            %% that's the only way we can inform the caller about the
            %% error
            ok;
         _ ->
            %% REVISIT: Currently being used by logging only
            ok
    end,
    {noreply, State};
handle_info({Port, {exit_status, Status}}, #state{port = Port} = State) ->
    log(State#state.logging, "port ~p exited with status ~p~n", [Port, Status]),
    case Status of
        0 ->
            {stop, normal, State};
        Other ->
            {stop, {port_exit, Other}, State}
    end;
%% Catch all - throws away unknown messages (This could happen by "accident"
%% so we do not want to crash, but we make a log entry as it is an
%% unwanted behaviour.)
handle_info(Info, State) ->
    error_logger:error_report("ORA: received unexpected info: ~p~n", [Info]),
    {noreply, State}.

terminate(Reason, #state{port=Port, port_mod=PMod}) ->
    io:format("Terminating ~p~n", [Reason]),
    catch PMod:port_close(Port),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Only used in synchronous context, after sending message to port we will wait
%% for the response
handle_synch_result(L, {?RELEASE_SESSION_POOL, ok}) ->
    log(L, "OCI: Session Pool terminated~n", []), ok;
handle_synch_result(L, {?CREATE_SESSION_POOL, ok, SessionPoolName}) ->
    log(L, "OCI: Session Pool ~p instantiated~n", [SessionPoolName]),
    {ok, SessionPoolName};
handle_synch_result(L, {?GET_SESSION, ok, SessionId}) ->
    log(L, "OCI: New Session Opened ~p~n", [SessionId]),
    {ok, SessionId};
handle_synch_result(L, Msg) ->
    handle_result(L, Msg).

%% typically used in an asychronous context... port-message triggers handle_info
handle_result(L, {?R_DEBUG_MSG, log, Log}) ->
    log(L, "OCI: ~p~n", [Log]);
handle_result(L, {?R_DEBUG_MSG, ok, log_disabled}) ->
    log(L, "OCI: Remote logging disabled~n", []);
handle_result(L, {?R_DEBUG_MSG, ok, log_enabled}) ->
    log(L, "OCI: Remote logging enabled~n", []);
handle_result(L, {Cmd, SessionId, Result}) when is_integer(SessionId) ->
    log(L, "OCI: CMD: ~p with Result: ~p~n", [Cmd, Result]),
    {ok, {session_id, SessionId}, Result};
handle_result(L, {Cmd, error, Reason}) ->
    log(L, "OCI: CMD: ~p got error, ~p~n", [Cmd, Reason]),
    {error, Reason}.

log(Flag, Format, Args) ->
    if Flag == ?DBG_FLAG_ON ->
           io:fwrite(Format, Args);
       true -> ok
    end.
