%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2018-09-12 17:20:31
%%%-------------------------------------------------------------------
-module(influx_http).

-export([read_points/2,
         write_point/2]).

%%%===================================================================
%%% API
%%%===================================================================
read_points(InfluxConf, QueryOpt) ->
    Url = influx_utils:encode_http_path(InfluxConf, read, QueryOpt),
    Pool = maps:get(http_pool, InfluxConf, default),
    case hackney:request(get, Url, [], <<>>, [{pool, Pool}]) of
        {ok, StatusCode, _RepHeaders, Ref} ->
            case erlang:trunc(StatusCode/100) of
                2 ->
                    case maps:get(chunk_size, QueryOpt, undefined) of
                        undefined ->
                            read_body(Ref);
                        _ ->
                            Funs = maps:get(funs, QueryOpt, []),
                            read_stream_body(Ref, Funs)
                    end;
                _Other ->
                    hackney:close(Ref),
                    {error, StatusCode}
            end;
        {error, Reason} ->
            {error, Reason}
    end.    

write_point(InfluxConf, Data) ->
    Url = influx_utils:encode_http_path(InfluxConf, write, Data),
    Payload = influx_utils:encode_writen_payload(Data),
    Pool = maps:get(http_pool, InfluxConf, default),
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
                    lists:foreach(
                        fun({Mod, Fun}) -> Mod:Fun(ParsedData);
                           (Fun) -> Fun(ParsedData)
                    end, Funs),
                    read_stream_body(Ref, Funs);
                {error, Reason} ->
                    {error, Reason}
            end;
        done ->
            hackney:close(Ref),
            lists:foreach(
                fun({Mod, Fun}) -> Mod:Fun({ok, done});
                   (Fun) -> Fun({ok, done})
            end, Funs);
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
        case jsx:decode(Json, [return_maps]) of
            ResultMap -> {ok, ResultMap}
        end
    catch _:Exception ->
        {error, {json_parse, Exception}}
    end.

handle_result(#{<<"results">> := Result}) ->
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
                                        RAcc1 = lists:reverse(Acc1),
                                        lists:reverse([Maped|RAcc1])
                                end, [], Values),
                            #{<<"name">> => Name,
                              <<"points">> => RetPoints}
                    end, Series),
                RAcc0 = lists:reverse(Acc0),
                lists:reverse([NSeries|RAcc0]);
               (_, Acc0) ->
                Acc0
        end, [], Result),
    {ok, lists:flatten(NResult)};
handle_result(OtherResult) ->
    {error, {unexcepted_result, OtherResult}}.
