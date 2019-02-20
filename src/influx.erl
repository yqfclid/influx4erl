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
-export([write_point/2,
         bwrite_point/1]).
-export([read_points/2,
         read_points/3]).
%%%===================================================================
%%% API
%%%===================================================================
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(influx).

-spec register_worker(any(), map()|list()) -> true | {error, term()}.
register_worker(Name, Conf) ->
    influx_manager:register_worker(Name, Conf).

-spec delete_worker(any()) -> true | {error, term()}.
delete_worker(Name) ->
    influx_manager:delete_worker(Name).

-spec all_workers() -> list().
all_workers() ->
    influx_manager:all_workers().

-spec write_point(any(), map()|list()) -> ok | {ok, any()} | {error, term()}.
write_point(Name, Data) when is_list(Data) ->
    write_point(Name, maps:from_list(Data));
write_point(Name, Data) ->
    case ets:lookup(influx_workers, Name) of
        [{Name, #{protocol := http} = Conf}] ->
            influx_http:write_point(Conf, Data);
        [{Name, #{protocol := udp,
                  pid := Pid}}] when is_pid(Pid) ->
            influx_udp:write_point(Pid, Data);
        [{Name, #{protocol := udp} = Conf}] ->
            case influx_udp:start_udp(Conf) of
                {ok, Pid} ->
                    ets:insert(influx_workers, {Name, Conf#{pid => Pid}}),
                    influx_udp:write_point(Pid, Data);
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, no_worker}
    end.

-spec bwrite_point(list()|map()) -> ok.
bwrite_point(Data) ->
    ets:safe_fixtable(influx_workers, true),
    do_bwrite_point(Data),
    ets:safe_fixtable(influx_workers, false).

do_bwrite_point(Data) ->
    do_bwrite_point(Data, ets:first(influx_workers)).

do_bwrite_point(_Data, '$end_of_table') -> 
    ok;
do_bwrite_point(Data, Name) ->
    case ets:lookup(influx_workers, Name) of
        [{Name, #{protocol := http} = Conf}] ->
            influx_http:write_point(Conf, Data);
        [{Name, #{protocol := udp,
                  pid := Pid}}] when is_pid(Pid) ->
            influx_udp:write_point(Pid, Data);
        _ ->
            ok
    end,
    do_bwrite_point(Data, ets:next(influx_workers, Name)).

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