## this is influx client for erlang

### you can add deps in rebar.config
```
    {'influx', ".*", {git, "https://github.com/yqfclid/influx4erl", "master"}}
```

### you need start influx client at first
```
influx:start().
```

### then you need register your influx config
```
HttpConf = [{protocol,http},
            {host,<<"localhost">>},
            {port,8086},
            {database,<<"test">>}],
UdpConf = [{protocol, udp},
           {host, <<"localhost">>},
           {port, 8089}],
influx:register_worker(http_name, HttpConf),
influx:register_worker(udp_name, UdpConf).
```


### you can delete your registerd worker by using influx:delete_worker/1
```
> influx:delete_worker(http_name).
true
```

### you can see your all workers by using influx:all_workers()
```
> influx:all_workers().
[{udp_name,#{database => undefined,host => <<"localhost">>,
        http_pool => undefined,password => undefined,
        pid => <0.151.0>,port => 8089,protocol => udp,
        username => undefined}},
 {http_name,#{database => <<"test">>,host => <<"localhost">>,
         http_pool => undefined,password => undefined,port => 8086,
         protocol => http,username => undefined}}]
```

### you can write points by http or udp
```
> influx:write_points(http_name, #{measurement => <<"test">>, tags => [{<<"tag1">>, <<"1">>}], fields => [{<<"test1">>, <<"1">>}]}).     
ok
> influx:write_points(udp_name, #{measurement => <<"test">>, tags => [{<<"tag1">>, <<"1">>}], fields => [{<<"test1">>, <<"1">>}]}). 
ok
```

### you can read points only by http
```
> influx:read_points(http_name, <<"select * from memory_test order by time limit 1;select count(*) from memory_test">>).
{ok,[#{<<"name">> => <<"memory_test">>,
       <<"points">> =>
           [#{<<"atom">> => 339465,<<"atom_used">> => 330097,
              <<"binary">> => 1678792,<<"code">> => 7934256,
              <<"ets">> => 438472,
              <<"node">> => <<"influx_test@localhost">>,
              <<"processes">> => 681643520,
              <<"processes_used">> => 681641144,<<"system">> => 51460824,
              <<"time">> => <<"2019-02-19T03:40:08.775674Z">>,
              <<"total">> => 733104344}]},
     #{<<"name">> => <<"memory_test">>,
       <<"points">> =>
           [#{<<"count_atom">> => 1000002,
              <<"count_atom_used">> => 1000002,
              <<"count_binary">> => 1000002,<<"count_code">> => 1000002,
              <<"count_ets">> => 1000002,<<"count_processes">> => 1000002,
              <<"count_processes_used">> => 1000002,
              <<"count_system">> => 1000002,<<"count_total">> => 1000002,
              <<"time">> => <<"1970-01-01T00:00:00Z">>}]}]}
```

### if you want to init workers while starting application, config like this
```
{influx, [{load_confs, [{http_name, [{protocol, http},
                                      {host, <<"localhost">>},
                                      {port, 8086},
                                      {database, <<"test">>}]},
                         {udp_name, [{protocol, udp},
                                     {host, <<"localhost">>},
                                     {port, 8089}]}]}]}
```