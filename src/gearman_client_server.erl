%%%----------------------------------------------------------------------
%%%
%%% @copyright yun.io copyright 2010
%%%
%%% @author richie at yun.io
%%% @doc gearman client server
%%% @end
%%%
%%%----------------------------------------------------------------------
-module(gearman_client_server).
-author('richie at yun.io').
-vsn('0.1').
-behaviour(gen_server).
-include("gearman.hrl").
-include("gearman_internal.hrl").

-export([start/2, stop/1]).
-export([start_link/2]).

-export([do/3, do/4]).
-export([do_bg/3]).
-export([get_status/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
                            terminate/2, code_change/3]).
                
%% the waiter item
-record(waiter_item, {
        from,
        callback,
        cmd
    }).
                            
-record(state, {
        port,               % port driver
        waiter              % dict store the waiter for the response
    }).

-define(DRV_NAME, "gearman_drv").
-define(CALLBACK_NULL, '$callback_null').
-define(TIMEOUT, 5000).

%% @doc start the gearman client server
-spec start(inet_host(), inet_port()) -> 
    {'ok', any()} | {'error', any()}.
start(Host, Port) ->
    gen_server:start(?MODULE, 
        {Host, Port, ?GDRV_CLIENT_DEFAULT}, []).

%% @doc stop the client server
stop(Pid) ->
    call(Pid, stop).

%% @doc start the gearman client server
-spec start_link(inet_host(), inet_port()) -> 
    {'ok', any()} | {'error', any()}.
start_link(Host, Port) when is_list(Host), is_integer(Port) ->
    gen_server:start_link(?MODULE, 
        {Host, Port, ?GDRV_CLIENT_DEFAULT}, []).

%% @doc do a job, the result is sent to the caller as message
%%  the message:
%%  {task_id(), work_response()}
-spec do(pid(), fun_name(), workload()) ->
    {'ok', task_id()} | {'error', any()}.
do(Pid, FunName, Workload) ->
    FunName2 = valid_fun_name(FunName),
    WorkLoad2 = valid_workload(Workload),
    call(Pid, {do, FunName2, WorkLoad2}).

%% @doc run a job 
-spec do(pid(), fun_name(), workload(), work_callback()) ->
    {'ok', task_id()} | {'error', any()}.
do(Pid, FunName, Workload, Callback) ->
    FunName2 = valid_fun_name(FunName),
    WorkLoad2 = valid_workload(Workload),
    Callback2 = valid_callback(Callback),
    call(Pid, {do, FunName2, WorkLoad2, Callback2}).

%% @doc run a job in background
-spec do_bg(pid(), fun_name(), workload()) ->
    {'ok', job_handle()} | {'error', any()}.
do_bg(Pid, FunName, Workload) ->
    FunName2 = valid_fun_name(FunName),
    WorkLoad2 = valid_workload(Workload),
    case call(Pid, {do_bg, FunName2, WorkLoad2}) of
        {ok, TaskId} ->
            receive 
                {TaskId, #job_created{handle = Handle}} ->
                    {ok, Handle}
            after
                ?TIMEOUT ->
                    {error, response_timeout}
            end;
        Error ->
            Error
    end.

%% @doc get the status of submitted job 
-spec get_status(pid(), job_handle()) ->
    {'ok', status_res()} | {'error', any()}.
get_status(Pid, JobHandle) ->
    case call(Pid, {get_status, JobHandle}) of
        {ok, TaskId} ->
            receive
                {TaskId, Status = #status_res{}} ->
                    {ok, Status}
            after 
                ?TIMEOUT ->
                    {error, response_timeout}
            end;
        Error ->
            Error
    end.

%%
%% gen_server callbacks
%%
init({InetHost, InetPort, Opt}) ->    
    process_flag(trap_exit, true),
    Waiter = dict:new(),
    erl_ddll:start(),
    PrivDir = get_priv_dir(),
    LibDir1 = filename:join([PrivDir, "lib"]),
    ?DEBUG2("lib dir is ~s", [LibDir1]),
    case erl_ddll:load_driver(LibDir1, gearman_drv) of
	    ok -> 
            ?DEBUG2("open_port for driver ~s", [?DRV_NAME]),
            Port = open_port({spawn_driver, ?DRV_NAME}, []),
            % send command to client
            {ok, _} = 
            ctl_cmd(Port, ?GDRV_CLIENT_OPT, [?int32(Opt)]),
            % set the server
            {ok, _} = 
            ctl_cmd(Port, ?GDRV_CLIENT_ADD_SERVER, 
                server_string(InetHost, InetPort)),
            {ok, #state{port = Port, waiter = Waiter}};
	    {error, Error} ->
            ?ERROR2("load driver error:~s\n", [erl_ddll:format_error(Error)]),
            {stop, Error}
	end.

handle_call({do, FunName, Workload}, {From, _Tag}, State) ->
    {Reply, State2} = do_inter(FunName, Workload, From, ?CALLBACK_NULL, State),
    {reply, Reply, State2};
handle_call({do, FunName, Workload, Callback}, {From, _Tag}, State) ->
    {Reply, State2} = do_inter(FunName, Workload, From, Callback, State),
    {reply, Reply, State2};
handle_call({do_bg, FunName, Workload}, {From, _Tag}, State) ->
    {Reply, State2} = do_bg_inter(FunName, Workload, From, State),
    {reply, Reply, State2};
handle_call({get_status, JobHandle}, {From, _Tag}, State) ->
    {Reply, State2} = get_status_inter(JobHandle, From, State),
    {reply, Reply, State2};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gearman_async, TaskId, Data}, State) ->
    ?DEBUG2("gearman_async handle taskid:~p Data:~p", [TaskId, Data]),
    State2 = handle_response_inter(TaskId, Data, State),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State = #state{port = Port}) ->
    ?DEBUG2("client close~n", []),
    port_close(Port),
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.
    
%%
%% internal API
%%

%% do the SUBMIT_JOB
do_inter(FunName, Workload, From, Callback, State) ->
    FunNameLen = iolist_size(FunName),
    WorkloadLen = iolist_size(Workload),
    Args = [<<FunNameLen:32/big>>, FunName, <<WorkloadLen:32/big>>, Workload],
    async_request(?GDRV_CLIENT_SUBMIT_JOB, Args, From, Callback, State).

%% do the SUBMIT_JOB_BG
do_bg_inter(FunName, Workload, From, State) ->
    FunNameLen = iolist_size(FunName),
    WorkloadLen = iolist_size(Workload),
    Args = [<<FunNameLen:32/big>>, FunName, <<WorkloadLen:32/big>>, Workload],
    async_request(?GDRV_CLIENT_SUBMIT_JOB_BG, Args, From, State).

%% get status
get_status_inter(JobHandle, From, State) ->
    Args = [JobHandle, $\0],
    async_request(?GDRV_GET_STATUS, Args, From, State).

%% handle the gearman_async response
handle_response_inter(TaskId, Resp = {job_created, _Handle}, 
        State = #state{waiter = Waiter}) ->
    {ok, WaiterItem = #waiter_item{cmd = Cmd}} = get_waiter(TaskId, Waiter),
    do_dispatch_response(WaiterItem, TaskId, Resp),
    if Cmd =:= ?GDRV_CLIENT_SUBMIT_JOB_BG;
        Cmd =:= ?GDRV_CLIENT_SUBMIT_JOB_LOW_BG;
        Cmd =:= ?GDRV_CLIENT_SUBMIT_JOB_HIGH_BG ->
            Waiter2 = del_waiter(TaskId, Waiter),
            State#state{waiter = Waiter2};
        true ->
            State
    end;
handle_response_inter(TaskId, Resp = {work_status, _Handle, _Num, _Dnom}, 
        State = #state{waiter = Waiter}) ->
    {ok, WaiterItem} = get_waiter(TaskId, Waiter),
    do_dispatch_response(WaiterItem, TaskId, Resp),
    State;
handle_response_inter(TaskId, Resp = {Type, _Handle,  _Data}, 
        State = #state{waiter = Waiter}) 
    when Type =:= work_data;
        Type =:= work_warning;
        Type =:= work_exception ->
    {ok, WaiterItem} = get_waiter(TaskId, Waiter),
    do_dispatch_response(WaiterItem, TaskId, Resp),
    State;
handle_response_inter(TaskId, Resp = {Type, _Handle,  _Data}, 
        State = #state{waiter = Waiter}) 
    when Type =:= work_fail;
        Type =:= work_complete ->
    {ok, WaiterItem} = get_waiter(TaskId, Waiter),
    do_dispatch_response(WaiterItem, TaskId, Resp),
    Waiter2 = del_waiter(TaskId, Waiter),
    State#state{waiter = Waiter2};
handle_response_inter(TaskId, Resp = {status_res, _Handle,  _, _, _, _}, 
        State = #state{waiter = Waiter}) ->
    {ok, WaiterItem} = get_waiter(TaskId, Waiter),
    do_dispatch_response(WaiterItem, TaskId, Resp),
    Waiter2 = del_waiter(TaskId, Waiter),
    State#state{waiter = Waiter2}.


%% check if the fun name valid
valid_fun_name(FunName) when is_list(FunName) ->
    FunName;
valid_fun_name(FunName) when is_atom(FunName) ->
    atom_to_list(FunName).

valid_workload(Workload) when is_list(Workload) ->
    Workload;
valid_workload(Workload) when is_binary(Workload) ->
    Workload.

valid_callback(Callback) when is_function(Callback, 2) ->
    Callback.

%% add the new waiter
add_waiter(Id, WaiterItem = #waiter_item{}, Waiter) ->
    dict:store(Id, WaiterItem, Waiter).

%% delete the waiter from dict
del_waiter(Id, Waiter) ->
    ?DEBUG2("delete waiter :~b", [Id]),
    dict:erase(Id, Waiter).

%% get the waiter from dict
get_waiter(Id, Waiter) ->
    dict:find(Id, Waiter).


%% check if the waiter exists in dict
%%is_waiter(Id, Waiter) ->
%%    dict:is_key(Id, Waiter).

-ifndef(CTEST).
get_priv_dir() ->
    case code:priv_dir(gearman) of
        {error, _} ->
            Which = code:which(?MODULE),
            Root = filename:dirname(filename:dirname(Which)),
            filename:join([Root, "priv"]);
        Path ->
            Path
    end.
-else.
get_priv_dir() ->
    case code:priv_dir(gearman) of
        {error, _} ->
            {ok, Dir} = file:get_cwd(),
            ?DEBUG2("current work directory:~p", [Dir]),
            Root = filename:dirname(filename:dirname(filename:dirname(Dir))),
            filename:join([Root, "priv"]);
        Path ->
            Path
    end.
-endif.

%% convert the host port to Host:port string
server_string(Host, Port) ->
    Host ++ ":" ++ ?N2S(Port) ++ [$\0].

%% call the gen_server
call(Pid, Cmd) ->
    gen_server:call(Pid, Cmd).

%% send command to port
ctl_cmd(Port, Cmd, Args) ->
    %?DEBUG2("gearman_client_server:ctl_cmd(~p, ~p, ~p)~n", [Port,Cmd,Args]),
    Result =
    try erlang:port_control(Port, Cmd, Args) of
        [?GDRV_REP_OK | Reply] ->
            {ok,Reply};
        [?GDRV_REP_ERROR | Err] -> 
            {error, list_to_atom(Err)}
    catch
        error:_ -> 
            {error,einval}
    end, 
    %?DEBUG2("gearman_client:ctl_cmd() -> ~p~n", [Result]),
    Result.

%% async ctl request
async_request(Cmd, Args, From, State) ->
    async_request(Cmd, Args, From, ?CALLBACK_NULL, State).

async_request(Cmd, Args, From, Callback, State = #state{port = Port, waiter = Waiter}) ->
    case ctl_cmd(Port, Cmd, Args) of
        {ok, [R3, R2, R1, R0]} ->
            TaskId = ?u32(R3, R2, R1, R0),
            WaiterItem = #waiter_item{from = From, 
                callback = Callback,
                cmd = Cmd},
            Waiter2 = add_waiter(TaskId, WaiterItem, Waiter),
            {{ok, TaskId}, State#state{waiter = Waiter2}};
        {error, _} = Error ->
            {Error, State}
    end.


%% dispatch response to caller
do_dispatch_response(#waiter_item{from = From, callback = Callback}, 
        TaskId, Resp) ->
    case Callback of 
        ?CALLBACK_NULL ->
            catch From ! {TaskId, Resp}; 
        _ ->
            catch Callback(TaskId, Resp)
    end.

%%
%% EUNIT test
%%
-ifdef(EUNIT).
some_test() ->
    ?assert(true).
-endif.
