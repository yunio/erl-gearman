%%%----------------------------------------------------------------------
%%%
%%% @copyright yun.io copyright 2010
%%%
%%% @author richie at yun.io
%%% @doc gearman internal header file
%%%
%%%----------------------------------------------------------------------
-ifndef(GEARMAN_INTERNAL_HRL).
-define(GEARMAN_INTERNAL_HRL, ok).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("gearman.hrl").
-include("gearman_log.hrl").

-define(GEARMAN_MAIN_SUP, 'gearman_sup').
-define(GEARMAN_CLIENT_SUP, 'gearman_client_sup').
-define(GEARMAN_WORK_SUP, 'gearman_work_sup').

%% 
%% about types and records
%%

-type inet_host() :: string() | atom().
-type inet_port() :: 0..65535.
-type client_option() :: pos_integer().
-type fun_name() :: string().
-type workload() :: binary() | string().
-type job_handle() :: binary().
-type task_id() :: integer().

-type work_callback() :: fun((task_id(), work_response()) -> any()).

%%
%% about the gearman client
%%
-define(GDRV_CLIENT_NON_BLOCKING, 2#10).         % (1 << 1),
-define(GDRV_CLIENT_UNBUFFERED_RESULT, 2#100).   % (1 << 3),

-define(GDRV_CLIENT_DEFAULT, 
    (?GDRV_CLIENT_NON_BLOCKING bor 
        ?GDRV_CLIENT_UNBUFFERED_RESULT)).

%% request
-define(GDRV_CLIENT_OPT,                1).
-define(GDRV_CLIENT_ADD_SERVER,         2).

-define(GDRV_CLIENT_SUBMIT_JOB,         3).
-define(GDRV_CLIENT_SUBMIT_JOB_BG,      4).
-define(GDRV_CLIENT_SUBMIT_JOB_HIGH,    5).
-define(GDRV_CLIENT_SUBMIT_JOB_HIGH_BG, 6).
-define(GDRV_CLIENT_SUBMIT_JOB_LOW,     7).
-define(GDRV_CLIENT_SUBMIT_JOB_LOW_BG,  8).

-define(GDRV_GET_STATUS,                9).
-define(GDRV_ECHO_REQ,                  10).


%% reply
-define(GDRV_REP_ERROR,                 0).
-define(GDRV_REP_OK,                    1).

%% Bytes to unsigned
-define(u64(X7,X6,X5,X4,X3,X2,X1,X0), 
    ( ((X7) bsl 56) bor ((X6) bsl 48) bor ((X5) bsl 40) bor 
      ((X4) bsl 32) bor ((X3) bsl 24) bor ((X2) bsl 16) bor 
      ((X1) bsl 8) bor (X0) )). 

-define(u32(X3,X2,X1,X0), 
    (((X3) bsl 24) bor ((X2) bsl 16) bor ((X1) bsl 8) bor (X0))).

-define(u24(X2,X1,X0),
    (((X2) bsl 16) bor ((X1) bsl 8) bor (X0))).

-define(u16(X1,X0),
    (((X1) bsl 8) bor (X0))).

-define(u8(X0), (X0)).

%% int to bytes
-define(int8(X), [(X) band 16#ff]).

-define(int16(X), [((X) bsr 8) band 16#ff, (X) band 16#ff]).

-define(int24(X), [((X) bsr 16) band 16#ff,
           ((X) bsr 8) band 16#ff, (X) band 16#ff]).

-define(int32(X), 
    [((X) bsr 24) band 16#ff, ((X) bsr 16) band 16#ff,
     ((X) bsr 8) band 16#ff, (X) band 16#ff]).


%% syntax similar with '?:' in c
-ifndef(IF).
-define(IF(C, T, F), (case (C) of true -> (T); false -> (F) end)).
-endif.

%% some convert macros
-define(B2S(B), binary_to_list(B)).
-define(S2B(S), list_to_binary(S)).

-define(N2S(N), integer_to_list(N)).
-define(S2N(S), list_to_integer(S)).

-define(IOLIST2B(IO), iolist_to_binary(IO)).

-define(FUN_NULL, '$fnull').
-define(CALL_FUN(V, Fun), 
            case Fun of
                ?FUN_NULL ->
                    V;
                _ ->
                    Fun(V)
            end).

%%
%% misc defines
%%

-define(CONN_TIMEOUT, 5000).
-define(RECV_TIMEOUT, 2000).  
-define(CONN_POOL_DEF, 5).
-define(CONN_POOL_MIN, 1).
-define(CONN_POOL_MAX, 64).


-endif. % GEARMAN_INTERNAL_HRL
