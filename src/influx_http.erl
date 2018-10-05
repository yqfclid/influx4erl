%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2018-09-12 17:20:31
%%%-------------------------------------------------------------------
-module(influx_http).

-export([read_points/3,
		 write_points/5]).

-include("influx.hrl").

%%%===================================================================
%%% API
%%%===================================================================
read_points(InfluxConf, Query, Options) ->
	Url = influx_utils:encode_http_path(InfluxConf, read, [{q, Query}|Options]),
	#influx_conf{http_pool = Pool} = InfluxConf,
	lager:debug("url ~p ", [Url]),
	case hackney:request(get, Url, [], <<>>, [{pool, Pool}]) of
    	{ok, StatusCode, _RepHeaders, Ref} ->
    		case erlang:trunc(StatusCode/100) of
    			2 ->
    				case proplists:get_value(chunk_size, Options) of
    					undefined ->
    						read_body(Ref);
    					_ ->
    						Funs = proplists:get_value(funs, Options, []),
    						read_stream_body(Ref, Funs)
    				end;
    			_Other ->
    				hackney:close(Ref),
    				{error, StatusCode}
    		end;
    	{error, Reason} ->
    		{error, Reason}
    end.	

write_points(InfluxConf, Measurement, Tags, Fields, Options) ->
	Url = influx_utils:encode_http_path(InfluxConf, write, Options),
	Payload = influx_utils:encode_writen_payload(Measurement, Tags, Fields, Options),
	#influx_conf{http_pool = Pool} = InfluxConf,
	lager:debug("url ~p payload ~p", [Url, Payload]),
	case hackney:request(post, Url, [], Payload, [{pool, Pool}]) of
    	{ok, StatusCode, _RepHeaders, Ref} ->
    		Reply = 
	    		case StatusCode of
	    			_ when StatusCode =:= 200 
	    			  orelse StatusCode =:= 204->
	    				ok;	    			
	    			_Other ->
	    				{error, StatusCode}
	    		end,
	    	hackney:close(Ref),
	    	Reply;
    	{error, Reason} ->
    		{error, Reason}
    end.		
%%%===================================================================
%%% Internal functions
%%%===================================================================
	
read_body(Ref) ->
	case hackney:body(Ref) of
		{ok, Body} ->
			parse_data(Body);
		{error, Reason} ->
			{error, Reason}
	end.

read_stream_body(Ref, Funs) ->
	case hackney:stream_body(Ref) of
		{ok, Body} ->
			case parse_data(Body) of
				{ok, ParsedData} ->
					lists:foreach(fun(Fun) -> Fun(ParsedData) end, Funs),
					read_stream_body(Ref, Funs);
				{error, Reason} ->
					{error, Reason}
			end;
		done ->
			hackney:close(Ref),
			lists:foreach(fun(Fun) -> Fun({ok, done}) end, Funs);
		{error, Reason} ->
			{error, Reason}
	end.

parse_data(Body) ->
	case decode_json(Body) of
		{ok, ResultMap} ->
			handle_result(ResultMap);
		{error, Reason} ->
			{error, Reason}
	end.


decode_json(<<>>) ->
	{ok, <<>>};
decode_json(Json) ->
	try 
		case jiffy:decode(Json, [return_maps]) of
			ResultMap -> {ok, ResultMap}
		end
	catch _:Exception ->
		{error, {json_parse, Exception}}
	end.

handle_result(#{<<"results">> := Result} = ResultMap) ->
	NResult = 
		lists:foldl(
			fun(#{<<"series">> := Series}, Acc0) ->
				NSeries = 
					lists:map(
						fun(#{<<"columns">> := Columns,
							  <<"name">> := Name,
							  <<"values">> := Values}) ->
							RetPoints = 
								lists:foldl(
									fun(Value, Acc1) ->
										Maped = maps:from_list(lists:zip(Columns, Value)),
										Acc1 ++ [Maped]
								end, [], Values),
							#{<<"name">> => Name,
							  <<"points">> => RetPoints}
					end, Series),
				Acc0 ++ [NSeries];
			   (_, Acc0) ->
			   	Acc0
		end, [], Result),
	{ok, lists:flatten(NResult)};
handle_result(OtherResult) ->
	{error, {unexcepted_result, OtherResult}}.
