%%%----------------------------------------------------------------------
%%%
%%% @copyright yun.io copyright 2010
%%%
%%% @author richie at yun.io
%%% @doc gearman client interface
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(gearman_client).
-author('richie at yun.io').
-vsn('0.1').
-include("gearman.hrl").
-include("gearman_internal.hrl").

-export([do/2, do/3]).
-export([do_bg/2]).
-export([get_status/1]).

%% @doc do a job, the result is sent to the caller as message
%%  the message:
%%  {task_id(), work_response()}
-spec do(fun_name(), workload()) ->
    {'ok', task_id()} | {'error', any()}.
do(FunName, Workload) ->
    F = 
    fun(Pid) ->
        case gearman_client_server:do(Pid, FunName, Workload) of
            {ok, _} = Resp ->
                Resp;
            _ ->
                try_next
        end
    end,
    do_iter(F).

%% @doc run a job 
-spec do(fun_name(), workload(), work_callback()) ->
    {'ok', task_id()} | {'error', any()}.
do(FunName, Workload, Callback) ->
    F = 
    fun(Pid) ->
        case gearman_client_server:do(Pid, FunName, Workload, Callback) of
            {ok, _} = Resp ->
                Resp;
            _ ->
                try_next
        end
    end,
    do_iter(F).

%% @doc run a job in background
-spec do_bg(fun_name(), workload()) ->
    {'ok', job_handle()} | {'error', any()}.
do_bg(FunName, Workload) ->
    F = 
    fun(Pid) ->
        case gearman_client_server:do_bg(Pid, FunName, Workload) of
            {ok, _} = Resp ->
                Resp;
            _ ->
                try_next
        end
    end,
    do_iter(F).

%% @doc get the status of submitted job 
-spec get_status(job_handle()) ->
    {'ok', status_res()} | {'error', any()}.
get_status(JobHandle) ->
    F = 
    fun(Pid) ->
        case gearman_client_server:get_status(Pid, JobHandle) of
            {ok, _} = Resp ->
                Resp;
            _ ->
                try_next
        end
    end,
    do_iter(F).

%%
%% internal API
%%
do_iter(F) ->
    L = supervisor:which_children(?GEARMAN_CLIENT_SUP),
    N = length(L),
    Started = random:uniform(N),
    {L1, L2} = lists:split(Started, L),
    try_all(L2, L1, F).

try_all([H|T], L2, F) ->
    {_Id, Child, _Type, _Modules} = H,
    case F(Child) of
        try_next ->
            try_all(T, L2, F);
        Resp ->
            Resp
    end;
try_all([], [H|T], F) ->
    {_Id, Child, _Type, _Modules} = H,
    case F(Child) of
        try_next ->
            try_all([], T, F);
        Resp ->
            Resp
    end;
try_all([], [], _F) ->
    {error, try_all}.

%%
%% EUNIT test
%%
-ifdef(EUNIT).
some_test() ->
    ?assert(true).
-endif.
