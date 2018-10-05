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
-export([start_link/1]).
-export([write_points/5]).
-export([stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("influx.hrl").

-record(state, {influx_conf,
				udp_port,
				socket
				}).

%%%===================================================================
%%% API
%%%===================================================================
write_points(InfluxConf, Measurement, Tags, Fields, Options) when is_record(InfluxConf, influx_conf) ->
	#influx_conf{name = Name} = InfluxConf,
	write_points(Name, Measurement, Tags, Fields, Options);
write_points(Name, Measurement, Tags, Fields, Options) ->
	PName = influx_utils:process_name(?MODULE, Name),
	gen_server:cast(PName, {write, Measurement, Tags, Fields, Options}).

stop(#influx_conf{name = Name}) ->
	stop(Name);
stop(Name) ->
	PName = influx_utils:process_name(Name),
	gen_server:cast(PName, stop).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(InfluxConf) ->
	#influx_conf{name = Name} = InfluxConf,
	PName = influx_utils:process_name(?MODULE, Name),
    gen_server:start_link({local, PName}, ?MODULE, [InfluxConf], []).


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
init([InfluxConf]) ->
    case gen_udp:open(0, [binary, {active, false}, {broadcast, true}]) of
        {ok, Socket} ->
            case inet:port(Socket) of
                {ok, LocalPort} ->
                    {ok, #state{influx_conf = InfluxConf,
                                socket = Socket,
                                udp_port = LocalPort}};
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
handle_cast({write, Measurement, Tags, Fields, Options}, State) ->
	#state{influx_conf = InfluxConf,
		   socket = Socket} = State,
	#influx_conf{host = Host,
				 port = Port} = InfluxConf,
	NHost = host_to_formal_type(Host),
	NPort = port_to_formal_type(Port),
	Payload = influx_utils:encode_writen_payload(Measurement, Tags, Fields, Options),
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