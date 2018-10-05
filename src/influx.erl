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
-export([write_points/3, write_points/4, write_points/5,
		 bwrite_points/3, bwrite_points/4, bwrite_points/5,
		 read_points/1, read_points/2, read_points/3]).
-export([start_worker/1, 
		 delete_worker/1,
		 get_worker_by_name/1,
		 get_udp_workers/0,
		 get_http_workers/0,
		 get_workers/0]).

-include("influx.hrl").

%%%===================================================================
%%% API
%%%===================================================================
start() ->
	application:ensure_all_started(influx).

write_points(Measurement, Tags, Fields) ->
	write_points(Measurement, Tags, Fields, []).

write_points(Measurement, Tags, Fields, Options) ->
	case influx_manager:all_workers() of
		[InfluxConf|_] ->
			write_points(InfluxConf, Measurement, Tags, Fields, Options);
		[] ->
			{error, no_config}
	end.

write_points(InfluxConf, Measurement, Tags, Fields, Options) ->
	#influx_conf{protocol = Protocol} = InfluxConf,
	case influx_utils:get_protocol_mod(Protocol) of
		{ok, Mod} ->
			Mod:write_points(InfluxConf, Measurement, Tags, Fields, Options);
		{error, Reason} ->
			{error, Reason}
	end.

bwrite_points(Measurement, Tags, Fields) ->
	bwrite_points(Measurement, Tags, Fields, []).

bwrite_points(Measurement, Tags, Fields, Options) ->
	InfluxConfs = influx_manager:all_workers(),
	lists:foreach(
		fun(InfluxConf) ->
			write_points(InfluxConf, Measurement, Tags, Fields, Options)
	end, InfluxConfs).

bwrite_points(Measurement, Tags, Fields, Options, Protocol) when Protocol =:= udp 
												   		    orelse Protocol =:= http ->
	InfluxConfs = influx_manager:lookup_protocol(Protocol),
	lists:foreach(
		fun(InfluxConf) ->
			write_points(InfluxConf, Measurement, Tags, Fields, Options)
	end, InfluxConfs).

read_points(Query) ->
	read_points(Query, []).

read_points(Query, Options) ->
	case influx_manager:lookup_protocol(http) of
		[InfluxConf|_] ->
			read_points(InfluxConf, Query, Options);
		[] ->
			{error, no_http_config}
	end.

read_points(InfluxConf, Query, Options) ->
	influx_http:read_points(InfluxConf, Query, Options).


start_worker(Options) ->
	influx_manager:start_worker(Options).

delete_worker(Name) ->
	influx_manager:delete_worker(Name).

get_worker_by_name(Name) ->
	influx_manager:lookup_name(Name).

get_udp_workers() ->
	influx_manager:lookup_protocol(udp).

get_http_workers() ->
	influx_manager:lookup_protocol(http).

get_workers() ->
	influx_manager:all_workers().

%%%===================================================================
%%% Internal functions
%%%===================================================================
