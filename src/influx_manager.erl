%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2018-09-13 13:34:24
%%%-------------------------------------------------------------------
-module(influx_manager).

-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([all_workers/0,
		 lookup_name/1,
		 lookup_protocol/1]).
-export([start_worker/1,
		 delete_worker/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-include("influx.hrl").

-record(state, {influx_confs}).

%%%===================================================================
%%% API
%%%===================================================================
all_workers() ->
	gen_server:call(?SERVER, confs).

lookup_name(Name) ->
	gen_server:call(?SERVER, {lookup_name, Name}).

lookup_protocol(Protocol) ->
	gen_server:call(?SERVER, {lookup_protocol, Protocol}).

start_worker(#influx_conf{} = InfluxConf) ->
	gen_server:call(?SERVER, {start_worker, InfluxConf});
start_worker(Conf) ->
	InfluxConf = to_influx_conf(Conf),
	start_worker(InfluxConf).

delete_worker(Name) ->
	gen_server:call(?SERVER, {delete, Name}).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(InfluxConfs) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [InfluxConfs], []).


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
init([InfluxConfs]) ->
	process_flag(trap_exit, true),
	NInfluxConfs = 
		lists:foldl(
			fun({Name, Conf}, Acc) ->
				InfluxConf = (to_influx_conf(Conf))#influx_conf{name = Name},
				case do_start_worker(InfluxConf) of
					{ok, _} ->
						maps:put(Name, InfluxConf, Acc);
					{error, Reason} ->
						lager:error("start worker with ~p failed:~p", 
							[InfluxConf, Reason]),
						Acc
				end
		end, #{}, InfluxConfs),
    {ok, #state{influx_confs = NInfluxConfs}}.

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
handle_call(confs, _From, #state{influx_confs = InfluxConfs} = State) ->
	{_, InfluxConfsList} = lists:unzip(maps:to_list(InfluxConfs)),
	{reply, InfluxConfsList, State};

handle_call({lookup_protocol, Procotol}, _From, #state{influx_confs = InfluxConfs} = State) ->
	Reply = maps:fold(
		fun(_Name, InfluxConf, Acc) ->
			#influx_conf{protocol = CPrrtocol} = InfluxConf,
			case CPrrtocol =:= Procotol of
				true ->
					Acc ++ [InfluxConf];
				_ ->
					Acc
			end
		end, [], InfluxConfs),
	{reply, Reply, State};

handle_call({lookup_name, Name}, _From, #state{influx_confs = InfluxConfs} = State) ->
	Reply = 
		case maps:find(Name, InfluxConfs) of
			{ok, InfluxConf} ->
				InfluxConf;
			error ->
				undefined
		end,
	{reply, Reply, State};

handle_call({delete, Name}, _From, #state{influx_confs = InfluxConfs} = State) ->
	NInfluxConfs = maps:remove(Name, InfluxConfs),
	Reply = 
		case maps:find(Name, InfluxConfs) of
			{ok, #influx_conf{protocol = udp} = InfluxConf} ->
				influx_udp:stop(InfluxConf);
			error ->
				ok
		end,
	{reply, Reply, State#state{influx_confs = NInfluxConfs}};

handle_call({start_worker, InfluxConf}, _From, #state{influx_confs = InfluxConfs} = State) ->
	#influx_conf{name = Name} = InfluxConf,
	{Reply, NInfluxConfs} = 
		case maps:find(Name, InfluxConfs) of
			{ok, _} ->
				{{error, {name_exist, Name}}, InfluxConfs};
			error ->
				case do_start_worker(InfluxConf) of
					{ok, _} ->
						{ok, maps:put(Name, InfluxConf, InfluxConfs)};
					{error, Reason} ->
						{{error, Reason}, InfluxConfs}
				end
		end,
	{reply, Reply, State#state{influx_confs = NInfluxConfs}};

handle_call(_Request, _From, State) ->
    Reply = ok,
    lager:warning("Can't handle request: ~p", [_Request]),
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
handle_cast(_Msg, State) ->
    lager:warning("Can't handle msg: ~p", [_Msg]),
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
    lager:warning("Can't handle info: ~p", [_Info]),
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
to_influx_conf(Conf) when is_record(Conf, influx_conf) ->
	Conf;
to_influx_conf(Conf) when is_list(Conf)->
	Protocol = proplists:get_value(protocol, Conf, http),
	Host = proplists:get_value(host, Conf, <<"localhost">>),
	Port = proplists:get_value(port, Conf, 8086),
	UserName = proplists:get_value(username, Conf, undefined),
	Password = proplists:get_value(password, Conf, undefined),
	Database = proplists:get_value(database, Conf, undefined),
	Name = proplists:get_value(name, Conf, default),
	#influx_conf{protocol = Protocol,
				 host = Host,
				 port = Port,
				 username = UserName,
				 password = Password,
				 database = Database,
				 name = Name};
to_influx_conf(Conf) ->
	erlang:throw({error, {bad_config, Conf}}).

do_start_worker(#influx_conf{protocol = udp} = InfluxConf) ->
	case supervisor:start_child(influx_udp_sup, [InfluxConf]) of
        {ok, Pid} ->
        	{ok, Pid};
        {ok, Pid, _Info} ->
        	{ok, Pid};
        {errror, {already_started, Pid}} ->
        	{ok, Pid};
        {error, Reason} ->
        	{error, Reason}
    end;
do_start_worker(#influx_conf{protocol = http}) ->
	{ok, ok};
do_start_worker(#influx_conf{}) ->
	{error, ignore}.