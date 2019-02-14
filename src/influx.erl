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
-export([read_points/2]).

%%%===================================================================
%%% API
%%%===================================================================
start() ->
    {ok, _} = application:ensure_all_started(influx).

register_worker(Name, Conf) ->
    influx_manager:register_worker(Name, Conf).

delete_worker(Name) ->
    influx_manager:delete_worker(Name).

all_workers() ->
    influx_manager:all_workers().

write_point(Name, Data) ->
    case ets:lookup(influx_workers, Name) of
        [#{protocol := http} = Conf] ->
            influx_http:write_point(Conf, Data);
        [#{protocol := udp,
           pid := Pid}] when is_pid(Pid) ->
            influx_udp:write_point(Pid, Data);
        [#{protocol := udp} = Conf] ->
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
        [#{protocol := http} = Conf] ->
            influx_http:write_point(Conf, Data);
        [#{protocol := udp,
           pid := Pid}] when is_pid(Pid) ->
            influx_udp:write_point(Pid, Data);
        _ ->
            ok
    end,
    do_bwrite_point(Data, ets:next(influx_workers, Name)).

read_points(Name, QueryOpt) ->
    case ets:lookup(influx_workers, Name) of
        [#{protocol := http} = Conf] ->
            influx_http:read_points(Conf, QueryOpt);
        [_] ->
            {error, bad_worker};
        _ ->
            {error, no_worker}
    end.
