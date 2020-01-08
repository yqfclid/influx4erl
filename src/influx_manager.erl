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
-export([register_worker/2,
         delete_worker/1,
         all_workers/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
register_worker(Name, Conf) when is_map(Conf) ->
    register_worker(Name, maps:to_list(Conf));
register_worker(Name, Conf) when is_list(Conf) ->
    case ets:lookup(influx_workers, Name) of
        [] ->
            Protocol = proplists:get_value(protocol, Conf, http),
            Host = proplists:get_value(host, Conf, <<"localhost">>),
            Port = proplists:get_value(port, Conf, 8086),
            UserName = proplists:get_value(username, Conf, undefined),
            Password = proplists:get_value(password, Conf, undefined),
            Database = proplists:get_value(database, Conf, undefined),
            HttpPool = proplists:get_value(http_pool, Conf, undefined),
            ConfMap = #{protocol => Protocol,
                        host => Host,
                        port => Port,
                        username => UserName,
                        password => Password,
                        database => Database,
                        http_pool => HttpPool},
            case Protocol of
                udp ->
                    influx_udp:start_udp(Name, ConfMap);
                _ ->
                    ets:insert_new(influx_workers, {Name, ConfMap})
            end;
        _ ->
            {error, already_exists}
    end;
register_worker(_, _) ->
    {error, badarg}.

delete_worker(Name) ->
    case ets:lookup(influx_workers, Name) of
        [{Name, #{protocol := udp,
                  pid := Pid}}] when is_pid(Pid)->
            influx_udp:stop(Pid),
            ets:delete(influx_workers, Name);
        _ ->
            ets:delete(influx_workers, Name)
    end.

all_workers() ->
    ets:tab2list(influx_workers).
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
    lists:foreach(
        fun({Name, InfluxConf}) ->
            case register_worker(Name, InfluxConf) of
                true ->
                    ok;
                {error, Reason} ->
                    lager:error("start name ~p failed: ~p", [Reason])
            end
    end, InfluxConfs),
    {ok, #state{}}.

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