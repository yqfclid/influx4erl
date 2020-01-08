%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2018-09-12 16:14:29
%%%-------------------------------------------------------------------
-module(influx_udp).

-behaviour(gen_server).

%% API
-export([start_link/2]).
-export([start_udp/2]).
-export([write_point/2]).
-export([stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {host, port, udp_port, socket, name}).

%%%===================================================================
%%% API
%%%===================================================================
start_udp(Name, InfluxConf) ->
    case supervisor:start_child(influx_udp_sup, [Name, InfluxConf]) of
        {ok, Pid} ->
            {ok, Pid};
        {ok, Pid, _Info} ->
            {ok, Pid};
        {errror, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.
    
write_point(Pid, Data) ->
    gen_server:cast(Pid, {write, Data}).

stop(Pid) ->
    gen_server:cast(Pid, stop).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, InfluxConf) ->
    gen_server:start_link(?MODULE, [Name, InfluxConf], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the SERVER
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Name, #{host := Host,
              port := Port} = Conf]) ->
    case gen_udp:open(0, [binary, {active, false}, {broadcast, true}]) of
        {ok, Socket} ->
            case inet:port(Socket) of
                {ok, LocalPort} ->
                    ets:insert(influx_workers, {Name, Conf#{pid => self()}}),
                    {ok, #state{host = Host,
                                port = Port,
                                socket = Socket,
                                udp_port = LocalPort,
                                name = Name}};
                {error, Reason} ->
                    lager:warning("parse local udp port failed: ~p", [Reason]),
                    ignore
            end;
        {error, Reason} ->
            lager:warning("Open UDP failed: ~p", [Reason]),
            ignore
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({write, Data}, State) ->
    #state{host = Host,
           port = Port,
           socket = Socket} = State,
    NHost = host_to_formal_type(Host),
    NPort = port_to_formal_type(Port),
    Payload = influx_utils:encode_writen_payload(Data),
    case gen_udp:send(Socket, NHost, NPort, Payload) of
        ok ->
            lager:debug("[~p] udp -> ~p:~p success", 
                [?MODULE, Host, Port]);
        {error, Reason} ->
            lager:warning("[~p] udp -> ~p:~p failed:~p",
                [?MODULE, Host, Port, Reason])
    end,
    {noreply, State};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
host_to_formal_type(Host) when is_binary(Host) ->
    binary_to_list(Host);
host_to_formal_type(Host) when is_list(Host) ->
    Host;
host_to_formal_type(Host) when is_atom(Host) ->
    Host;
host_to_formal_type(Host) ->
    throw({badarg, Host}).

port_to_formal_type(Port) when is_integer(Port) ->
    Port;
port_to_formal_type(Port) when is_list(Port) ->
    list_to_integer(Port);
port_to_formal_type(Port) when is_binary(Port) ->
    binary_to_integer(Port);
port_to_formal_type(Port) ->
    throw({badarg, Port}).