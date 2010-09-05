-module(gearman_client_SUITE).
%% Note: This directive should only be used in test suites.
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include("gearman_internal.hrl").
-define(P(F, D), 
    ct:log(default, F, D)).
    
suite() -> [
    {timetrap,{minutes,2}}
    ].

init_per_suite(Config) ->
    code:add_path("../ebin"),
    ok = application:load(gearman),
    ok = application:unload(gearman),
    ok = application:start(gearman),
    Config.

end_per_suite(Config) ->
    application:stop(gearman),
    ok.

init_per_testcase(Name, Config) ->
    io:format("..init ~p~n~p~n", [Name, Config]),
    Config.

end_per_testcase(Name, Config) ->
    io:format("...end ~p~n~p~n", [Name, Config]),
    ok.

all() -> 
    [
        test_setup,
        test_do,
        test_do_bg
    ].

%%-------------------------------------------------------------------------
%% Test cases sthreets here.
%%-------------------------------------------------------------------------

%% test setup client
test_setup(Config) ->
    ok = gearman_sup:start_client([{"127.0.0.1", 4730}]).

test_do(Config) ->
    {ok, TaskId} = gearman_client:do("echo", "hello"),
    ok = receive_data(TaskId),
    {ok, TaskId2} = gearman_client:do("echo", "hello", fun callback/2),
    ok.

%% receive data
receive_data(TaskId) ->
    receive_data(TaskId, init).

receive_data(TaskId, init) ->
    Data =
    receive
        RData ->
            RData
    end,
    receive_data(TaskId, Data);
receive_data(TaskId, {TaskId, #job_created{} = Resp}) ->
    ?P("receive ~p", [Resp]),
    receive_data(TaskId, init);
receive_data(TaskId, {TaskId, #work_data{} = Resp}) ->
    ?P("receive ~p", [Resp]),
    receive_data(TaskId, init);
receive_data(TaskId, {TaskId, #work_status{} = Resp}) ->
    ?P("receive ~p", [Resp]),
    receive_data(TaskId, init);
receive_data(TaskId, {TaskId, #work_complete{} = Resp}) ->
    ?P("receive ~p", [Resp]),
    ok.

%% do callbacks
callback(TaskId, Resp) ->
    ?P("callback id:~b resp:~p\n", [TaskId, Resp]).


test_do_bg(Config) ->
    {ok, Handle} = gearman_client:do_bg("echo", "hello"),
    {ok, Status} = gearman_client:get_status(Handle),
    ok.
