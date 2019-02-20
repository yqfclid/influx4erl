%%%-------------------------------------------------------------------
%%% @author Yqfclid 
%%% @copyright  Yqfclid (yqf@blackbird)
%%% @doc
%%%
%%% @end
%%% Created :  2018-09-12 17:40:44
%%%-------------------------------------------------------------------
-module(influx_utils).

-export([encode_writen_payload/1,
         encode_http_path/3]).

-define(EMPTY, <<>>).
-define(QUOTE, <<"\"">>).
-define(SPACE, <<" ">>).
-define(COMMA, <<",">>).
-define(EQUAL, <<"=">>).

%%%===================================================================
%%% API
%%%===================================================================
encode_writen_payload(#{measurement := Measurement,
                        tags := Tags,
                        fields := Fields} = Data) ->
    MeasurementB = to_binary(Measurement),
    TimeB = 
        case maps:get(time, Data, false) of
            true ->
                Precision = maps:get(epoch, Data, ns),
                to_binary(timestamp(Precision));
            false ->
                ?EMPTY;
            Time ->
                to_binary(Time)
        end,
    TagsB = encode_tags(Tags),
    FieldsB = encode_fields(Fields),
    case TimeB of
        ?EMPTY ->
            <<MeasurementB/binary, TagsB/binary, ?SPACE/binary, FieldsB/binary>>;
        _ ->
            <<MeasurementB/binary, TagsB/binary, ?SPACE/binary, FieldsB/binary, ?SPACE/binary, TimeB/binary>>
    end;
encode_writen_payload(_Data) ->
    erlang:throw(badarg).
    
encode_http_path(Conf, Action, Data) ->
    #{host := Host,
      port := Port,
      username := UserName,
      password := Password,
      database := Database} = Conf,
    HostB = to_binary(Host),
    PortB = to_binary(Port),
    ActionB = 
        case Action of
            read ->
                <<"query">>;
            write ->
                <<"write">>;
            _ ->
                erlang:error({bad_action, Action})
        end,
    Path0 = <<"http://", HostB/binary, ":", PortB/binary, "/", ActionB/binary, "?">>,
    F = 
        fun(_, undefined, Acc) ->
                Acc;
           (epoch, Epoch, Acc) ->
            EpochB = to_binary(Epoch),
            <<Acc/binary, "epoch=", EpochB/binary, "&">>;
           (chunk_size, ChunkSize, Acc) ->
            ChunkSizeB = to_binary(ChunkSize),
            <<Acc/binary, "chunked=true&chunk_size=", ChunkSizeB/binary, "&">>;
           (database, DB, Acc) ->
            DBB = to_binary(DB),
            <<Acc/binary, "db=", DBB/binary, "&">>;
           (username, UName, Acc) ->
            UNameB = to_binary(UName),
            <<Acc/binary, "u=", UNameB/binary, "&">>;
           (password, Pw, Acc) ->
            PwB = to_binary(Pw),
            <<Acc/binary, "p=", PwB/binary, "&">>;
           (rp, RP, Acc) ->
            RPB = to_binary(RP),
            <<Acc/binary, "rp=", RPB/binary, "&">>;
           (q, Q, Acc) ->
            QB = to_binary(Q),
            UrlenCodeQB = urlencode(QB),
            <<Acc/binary, "q=", UrlenCodeQB/binary, "&">>;
           (_, _, Acc) ->
            Acc
        end,
    Path1 = maps:fold(F, <<>>, Data#{username => UserName, 
                                     password => Password,
                                     database => Database}),
    NPath1 = remove_trailing(Path1),
    <<Path0/binary, NPath1/binary>>.
%%%===================================================================
%%% Internal functions
%%%===================================================================
urlencode(Bin) ->
    list_to_binary(edoc_lib:escape_uri(binary_to_list(Bin))).

encode_tags(Tags) when is_map(Tags) ->
    maps:fold(fun encode_tag/3, ?EMPTY, Tags);
encode_tags(Tags) when is_list(Tags)->
    lists:foldl(fun encode_tag/2, ?EMPTY, Tags).

encode_fields(Fields) when is_map(Fields) ->
    Encoded = 
        maps:fold(fun encode_field/3, ?EMPTY, Fields),
    remove_trailing(Encoded);
encode_fields(Fields) when is_list(Fields)->
    Encoded = 
        lists:foldl(fun encode_field/2, ?EMPTY, Fields),
    remove_trailing(Encoded).


encode_tag({K, V}, Acc) ->
    encode_tag(K, V, Acc).

encode_tag(K, V, Acc) ->
    NK = to_binary(K),
    NV = to_binary(V),
    <<Acc/binary, ?COMMA/binary, NK/binary, ?EQUAL/binary, NV/binary>>.

encode_field({K, V}, Acc) ->
    encode_field(K, V, Acc).

encode_field(K, V, Acc) ->
    NK = to_binary(K),
    NV = 
        case V of
            _ when is_integer(V)
              orelse is_float(V) ->
                to_binary(V);
            _ ->
                VB = to_binary(V),
                <<?QUOTE/binary, VB/binary, ?QUOTE/binary>>
        end,
    <<Acc/binary, NK/binary, ?EQUAL/binary, NV/binary, ?COMMA/binary>>. 


remove_trailing(?EMPTY) -> ?EMPTY;
remove_trailing(B) -> binary:part(B, {0, byte_size(B) - 1}).


to_binary(S) when is_binary(S) ->
    S;
to_binary(S) when is_atom(S) ->
    to_binary(atom_to_list(S));
to_binary(S) when is_integer(S) ->
    integer_to_binary(S);
to_binary(S) when is_float(S) ->
    float_to_binary(S);
to_binary(S) when is_list(S) ->
    list_to_binary(S);
to_binary(S) ->
    throw({badarg, S}).


timestamp(h) ->
    round(timestamp(u) / (3600 * 1000000));
timestamp(m) ->
    round(timestamp(u) / (60 * 1000000));
timestamp(s) ->
    round(timestamp(u) / 1000000);
timestamp(ns) ->
    round(timestamp(u) * 1000);
timestamp(ms) ->
    round(timestamp(u) / 1000);
timestamp(us) ->
    {MegaSec, Sec, MicroSec} = os:timestamp(),
    (MegaSec * 1000000 + Sec) * 1000000 + MicroSec;
timestamp(Arg) ->
    throw({badarg, Arg}).