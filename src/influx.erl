
%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2018-09-12 16:47:32
%%%-------------------------------------------------------------------
-module(influx).


-export([start/0]).
-export([register_worker/2,
         delete_worker/1,
         all_workers/0]).
-export([bwrite_points/1,
         write_points/2]).
-export([read_points/2,
         read_points/3]).
%%%===================================================================
%%% API
%%%===================================================================
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(influx).

%%% @doc
%%% register_worker
%%% Conf params:
%%%     host  influxdb host
%%%     port  influxdb port
%%%     procotol  protocol connect to influxdb udp/http
%%%     username  influxdb username
%%%     password  influxdb password
%%%     database  influx database only useful when protocol is http
%%%     http_pool  using http pool when protocol is http
%%% @end
-spec register_worker(any(), map()|list()) -> true | {error, term()}.
register_worker(Name, Conf) ->
    influx_manager:register_worker(Name, Conf).

-spec delete_worker(any()) -> true | {error, term()}.
delete_worker(Name) ->
    influx_manager:delete_worker(Name).

-spec all_workers() -> list().
all_workers() ->
    influx_manager:all_workers().


%%% @doc
%%% write_points 
%%% Data params:
%%%     measurement  influxdb measurement that writing data to
%%%     tags  data tags
%%%     fields  data fields
%%%     time  true:client give the writing time
%%%           false:influxdb give the writing time
%%%           Time(integer()):write Time to the measurment as the time writing data
%%%     epoch  h/m/s/ms/us/ns  writing time's unit
%%%     rp  write into influxdb database retenetion policy
%%%     chunk_size  influxdb return data by chunk,  chunk_size is the max size of one chunk
%%%     funs chunk_fun() :: {Mod, Fun} | function()
%%%          once receiving chunked data, execute the functions defined in funs
%%% @end
-spec write_points(any(), list()) -> ok | {error, term()}.
write_points(Name, Data) ->
    case ets:lookup(influx_workers, Name) of
        [{Name, #{protocol := http} = Conf}] ->
            influx_http:write_points(Conf, Data);
        [{Name, #{protocol := udp,
                  pid := Pid}}] when is_pid(Pid) ->
            influx_udp:write_points(Pid, Data);
        [{Name, #{protocol := udp} = Conf}] ->
            case influx_udp:start_udp(Conf) of
                {ok, Pid} ->
                    influx_udp:write_point(Pid, Data);
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, no_worker}
    end.

-spec bwrite_points(list()|map()) -> ok.
bwrite_points(Data) ->
    ets:safe_fixtable(influx_workers, true),
    do_bwrite_points(Data),
    ets:safe_fixtable(influx_workers, false).

do_bwrite_points(Data) ->
    do_bwrite_points(Data, ets:first(influx_workers)).

do_bwrite_points(_Data, '$end_of_table') -> 
    ok;
do_bwrite_points(Data, Name) ->
    case ets:lookup(influx_workers, Name) of
        [{Name, #{protocol := http} = Conf}] ->
            influx_http:write_point(Conf, Data);
        [{Name, #{protocol := udp,
                  pid := Pid}}] when is_pid(Pid) ->
            influx_udp:write_point(Pid, Data);
        _ ->
            ok
    end,
    do_bwrite_points(Data, ets:next(influx_workers, Name)).

-spec read_points(any(), binary()) -> {ok, list()} | {error, term()}.
read_points(Name, Query) ->
    read_points(Name, Query, #{}).

-spec read_points(any(), binary(), map()|list()) -> {ok, list()} | {error, term()}.
read_points(Name, Query, QueryOpt) when is_map(QueryOpt)->
    do_read_points(Name, QueryOpt#{q => Query});
read_points(Name, Query, QueryOpt) when is_list(QueryOpt) ->
    do_read_points(Name, [{q, Query}|QueryOpt]).

do_read_points(Name, QueryOpt) when is_list(QueryOpt) ->
    read_points(Name, maps:from_list(QueryOpt));
do_read_points(Name, QueryOpt) ->
    case ets:lookup(influx_workers, Name) of
        [{Name, #{protocol := http} = Conf}] ->
            influx_http:read_points(Conf, QueryOpt);
        [_] ->
            {error, bad_worker};
        _ ->
            {error, no_worker}
    end.
