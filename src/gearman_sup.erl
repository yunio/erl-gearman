%%%----------------------------------------------------------------------
%%%
%%% @copyright yun.io copyright 2010
%%%
%%% @author richie at yun.io
%%% @doc gearman supervisor behaviour
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(gearman_sup).
-author("richie at yun.io").
-vsn('0.1').
-include("gearman_internal.hrl").
-behaviour(supervisor).

-export([start_link/0]).
-export([start_client/1]).

-export([init/1]).

%% @doc start the main supervisor
start_link() ->
    supervisor:start_link({local, ?GEARMAN_MAIN_SUP}, ?MODULE, 
        {?GEARMAN_MAIN_SUP, []}).

%% @doc start the client sup
%%      Hosts :: [{inet_host(), inet_port()}]
start_client(Hosts) when is_list(Hosts) ->
    {ok, _} = ensure_client_sup_started(),
    ?DEBUG2("start the client connectsions:~p", [Hosts]),
    [begin
        {ok, _} = supervisor:start_child(?GEARMAN_CLIENT_SUP, [Host, Port])
    end || {Host, Port} <- Hosts],
    ok.

%% @doc supervisor callback
init({?GEARMAN_MAIN_SUP, _Args}) -> 
    ?DEBUG2("init main supervisor ~p", [_Args]),
    
    Stragegy = {one_for_one, 10, 10},
    {ok, {Stragegy, []}};
init({?GEARMAN_CLIENT_SUP, _Args}) ->
    ?DEBUG2("init client supervisor ~p", [_Args]),
    Stragegy = {simple_one_for_one, 10, 10},

    Spec = 
    {gearman_client_server, {gearman_client_server, start_link, []},
        transient, 2000, worker, [gearman_client_server]},
    {ok, {Stragegy, [Spec]}}.


%%
%% internal API
%%

%% ensure the client sup is stared
ensure_client_sup_started() ->
    case whereis(?GEARMAN_CLIENT_SUP) of
        undefined ->
            start_client_sup();
        Pid ->
            {ok, Pid}
    end.

%% start the client sup
start_client_sup() ->
    ClientSupArgs = [{local, ?GEARMAN_CLIENT_SUP}, ?MODULE,
        {?GEARMAN_CLIENT_SUP, []}],
    Spec = 
    {?GEARMAN_CLIENT_SUP, {supervisor, start_link, ClientSupArgs},
        permanent, infinity, supervisor, [?MODULE]}, 
    ?DEBUG2("start the client sup", []),
    supervisor:start_child(?GEARMAN_MAIN_SUP, Spec).
