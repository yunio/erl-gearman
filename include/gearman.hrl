%%%----------------------------------------------------------------------
%%%
%%% @copyright yun.io copyright 2010
%%%
%%% @author richie at yun.io
%%% @doc gearman header file included by the client user
%%%
%%%----------------------------------------------------------------------
-ifndef(GEARMAN_HRL).
-define(GEARMAN_HRL, ok).

%% One note on tasks vs jobs. 
%% A task is usually used in the context of a client, 
%% and a job is usually used in the context of the server and worker. 
%% A task can be a job or other client request such as getting job status.

-record(job_created, {
        handle
    }).
-type job_created() :: #job_created{}.

-record(work_data, {
        handle,
        data
    }).
-type work_data() :: #work_data{}.

-record(work_warning, {
        handle,
        data
    }).
-type work_warning() :: #work_warning{}.

-record(work_status, {
        handle,
        numerator,
        denominator
    }).
-type work_status() :: #work_status{}.

-record(work_complete, {
        handle,
        data
    }).
-type work_complete() :: #work_complete{}.

-record(work_fail, {
        handle,
        data
    }).
-type work_fail() :: #work_fail{}.

-record(work_exception, {
        handle,
        data
    }).
-type work_exception() :: #work_exception{}.

-type  work_response() :: work_data()
            | work_warning() 
            | work_status() 
            | work_complete()
            | work_fail()
            | work_exception().

-record(status_res, {
        handle,
        known,
        running,
        numerator,
        denominator
    }).
-type status_res() :: #status_res{}.

-endif. % GEARMAN_HRL
