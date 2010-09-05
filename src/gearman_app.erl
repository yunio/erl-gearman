%%%----------------------------------------------------------------------
%%%
%%% @copyright yun.io copyright 2010
%%%
%%% @author richie at yun.io
%%% @doc gearman application behaviour
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(gearman_app).
-author("richie at yun.io").
-vsn('0.1').
-include("gearman_internal.hrl").

-behaviour(application).

-export([start/0]).
-export([start/2, stop/1]).

%% @doc start the application from the erl shell
-spec start() -> 'ok' | {'error', any()}.
start() ->
    ensure_apps(),
    application:start(gearman).

%% @doc the application start callback
-spec start(any(), any()) -> any().
start(_Type, _Args) ->
    ?DEBUG2("start the application", []),
    gearman_sup:start_link().

%% @doc the application  stop callback
stop(_State) ->
    ?DEBUG2("stop the application", []),
    ok.

%%
%% internal API
%%

%% first ensure some apps must start
ensure_apps() ->
    ok.
