%%%-------------------------------------------------------------------
%% @doc influx top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(influx_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-define(APP, influx).

%%====================================================================
%% API functions
%%====================================================================
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).
%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Before OTP 18 tuples must be used to specify a child. e.g.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
	InfluxConfs = application:get_env(?APP, load_confs, []),
	SupFlags = 
    	#{strategy => one_for_one,
          intensity => 10,
          period => 10},
    Proc1 = 
    	#{id => influx_manager,              
      	  start => {influx_manager, start_link, [InfluxConfs]},
      	  restart => transient,
      	  shutdown => infinity,
      	  type => worker,
      	  modules => [influx_manager]},
    Proc2 = 
    	#{id => influx_udp_sup,              
      	  start => {supervisor, start_link, [{local, influx_udp_sup}, ?MODULE, [influx_udp_sup]]},
      	  restart => transient,
      	  shutdown => infinity,
      	  type => supervisor,
      	  modules => [influx_udp_sup]},
    {ok, { SupFlags, [Proc2, Proc1]} };

init([influx_udp_sup]) ->
	erlang:put(1, 2),
	SupFlags = 
    	#{strategy => simple_one_for_one,
          intensity => 10,
          period => 10},
    Proc1 = 
    	#{id => influx_udp,              
      	  start => {influx_udp, start_link, []},
      	  restart => transient,
      	  shutdown => infinity,
      	  type => worker,
      	  modules => [influx_udp]},
    {ok, { SupFlags, [Proc1]} }.

%%====================================================================
%% Internal functions
%%====================================================================
