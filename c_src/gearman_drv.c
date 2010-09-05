/*
 * gearman driver
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <error.h>
#include <ctype.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <ei.h>
#include <erl_driver.h>
#include <libgearman/gearman.h>

/*
 *
 * macro defines
 *
 */

/* All platforms fail on malloc errors. */
#define FATAL_MALLOC

/* request */
#define GDRV_CLIENT_OPT                 1
#define GDRV_CLIENT_ADD_SERVER          2

#define GDRV_CLIENT_TASK_BASE           3
#define GDRV_CLIENT_SUBMIT_JOB          3
#define GDRV_CLIENT_SUBMIT_JOB_BG       4
#define GDRV_CLIENT_SUBMIT_JOB_HIGH     5
#define GDRV_CLIENT_SUBMIT_JOB_HIGH_BG  6
#define GDRV_CLIENT_SUBMIT_JOB_LOW      7
#define GDRV_CLIENT_SUBMIT_JOB_LOW_BG   8
#define GDRV_CLIENT_GET_STATUS          9

#define GDRV_WORK_DATA                  10
#define GDRV_WORK_WARNING               11
#define GDRV_WORK_EXCEPTION             12


/* response */
#define GDRV_REP_ERROR                  0
#define GDRV_REP_OK                     1


/* Standard set of integer macros  .. */
#define get_int64(s) (((Uint64)(((unsigned char*) (s))[0]) << 56) | \
                      (((Uint64)((unsigned char*) (s))[1]) << 48) | \
                      (((Uint64)((unsigned char*) (s))[2]) << 40) | \
                      (((Uint64)((unsigned char*) (s))[3]) << 32) | \
                      (((Uint64)((unsigned char*) (s))[4]) << 24) | \
                      (((Uint64)((unsigned char*) (s))[5]) << 16) | \
                      (((Uint64)((unsigned char*) (s))[6]) << 8)  | \
                      (((Uint64)((unsigned char*) (s))[7])))

#define put_int64(i, s) do {((char*)(s))[0] = (char)((Sint64)(i) >> 56) & 0xff;\
                            ((char*)(s))[1] = (char)((Sint64)(i) >> 48) & 0xff;\
                            ((char*)(s))[2] = (char)((Sint64)(i) >> 40) & 0xff;\
                            ((char*)(s))[3] = (char)((Sint64)(i) >> 32) & 0xff;\
                            ((char*)(s))[4] = (char)((Sint64)(i) >> 24) & 0xff;\
                            ((char*)(s))[5] = (char)((Sint64)(i) >> 16) & 0xff;\
                            ((char*)(s))[6] = (char)((Sint64)(i) >> 8)  & 0xff;\
                            ((char*)(s))[7] = (char)((Sint64)(i))       & 0xff;\
                           } while (0) 

#define get_int32(s) ((((unsigned char*) (s))[0] << 24) | \
                      (((unsigned char*) (s))[1] << 16) | \
                      (((unsigned char*) (s))[2] << 8)  | \
                      (((unsigned char*) (s))[3]))

#define put_int32(i, s) do {((char*)(s))[0] = (char)((i) >> 24) & 0xff;   \
                            ((char*)(s))[1] = (char)((i) >> 16) & 0xff;   \
                            ((char*)(s))[2] = (char)((i) >> 8)  & 0xff;   \
                            ((char*)(s))[3] = (char)(i)         & 0xff;} \
                        while (0)

#define get_int16(s) ((((unsigned char*)  (s))[0] << 8) | \
                      (((unsigned char*)  (s))[1]))


#define put_int16(i, s) do {((char*)(s))[0] = (char)((i) >> 8) & 0xff;  \
                            ((char*)(s))[1] = (char)(i)        & 0xff;} \
                        while (0)

#define get_int8(s) ((((unsigned char*)  (s))[0] ))


#define put_int8(i, s) do {((unsigned char*)(s))[0] = (i) & 0xff;} while (0)

#define GEARMAN_INFINITY  0xffffffff  /* infinity value */


static ErlDrvTermData am_ok;
static ErlDrvTermData am_error;
static ErlDrvTermData am_gearman_async;
static ErlDrvTermData am_gearman_reply;

static ErlDrvTermData am_job_created;
static ErlDrvTermData am_work_data;
static ErlDrvTermData am_work_warning;
static ErlDrvTermData am_work_status;
static ErlDrvTermData am_work_complete;
static ErlDrvTermData am_work_fail;
static ErlDrvTermData am_work_exception;
static ErlDrvTermData am_status_res;

#define DEBUGF  debug_msg
inline static void debug_msg(const char * fmt, ...);

/*
 * Malloc wrapper,
 * we would like to change the behaviour for different 
 * systems here.
 */

#ifdef FATAL_MALLOC
static void *alloc_wrapper(size_t size)
{
    void *ret = driver_alloc(size);
    if(ret == NULL) {
        DEBUGF("Out of virtual memory in malloc (%s)", __FILE__);
        exit(ENOMEM);
    }
    return ret;
}
#define ALLOC(X) alloc_wrapper(X)

static void *realloc_wrapper(void *current, size_t size){
    void *ret = driver_realloc(current,size);
    if(ret == NULL) {
        DEBUGF("Out of virtual memory in malloc (%s)", __FILE__);
        exit(ENOMEM);
    }
    return ret;
}
  #define REALLOC(X,Y) realloc_wrapper(X,Y)
  #define FREE(P) driver_free((P))
#else /* FATAL_MALLOC */
  #define ALLOC(X) driver_alloc((X))
  #define REALLOC(X,Y) driver_realloc((X), (Y))
  #define FREE(P) driver_free((P))
#endif /* FATAL_MALLOC */

#define INIT_ATOM(NAME) am_ ## NAME = driver_mk_atom(#NAME)

#define LOAD_ATOM_CNT 2
#define LOAD_ATOM(vec, i, atom) \
  (((vec)[(i)] = ERL_DRV_ATOM), \
  ((vec)[(i)+1] = (atom)), \
  ((i)+LOAD_ATOM_CNT))

#define LOAD_INT_CNT 2
#define LOAD_INT(vec, i, val) \
  (((vec)[(i)] = ERL_DRV_INT), \
  ((vec)[(i)+1] = (ErlDrvTermData)(val)), \
  ((i)+LOAD_INT_CNT))

#define LOAD_UINT_CNT 2
#define LOAD_UINT(vec, i, val) \
  (((vec)[(i)] = ERL_DRV_UINT), \
  ((vec)[(i)+1] = (ErlDrvTermData)(val)), \
  ((i)+LOAD_UINT_CNT))

#define LOAD_PORT_CNT 2
#define LOAD_PORT(vec, i, port) \
  (((vec)[(i)] = ERL_DRV_PORT), \
  ((vec)[(i)+1] = (port)), \
  ((i)+LOAD_PORT_CNT))

#define LOAD_PID_CNT 2
#define LOAD_PID(vec, i, pid) \
  (((vec)[(i)] = ERL_DRV_PID), \
  ((vec)[(i)+1] = (pid)), \
  ((i)+LOAD_PID_CNT))

#define LOAD_BINARY_CNT 4
#define LOAD_BINARY(vec, i, bin, offs, len) \
  (((vec)[(i)] = ERL_DRV_BINARY), \
  ((vec)[(i)+1] = (ErlDrvTermData)(bin)), \
  ((vec)[(i)+2] = (len)), \
  ((vec)[(i)+3] = (offs)), \
  ((i)+LOAD_BINARY_CNT))

#define LOAD_BUF2BINARY_CNT 3
#define LOAD_BUF2BINARY(vec, i, buf, len) \
  (((vec)[(i)] = ERL_DRV_BUF2BINARY), \
  ((vec)[(i)+1] = (ErlDrvTermData)(buf)), \
  ((vec)[(i)+2] = (len)), \
  ((i)+LOAD_BUF2BINARY_CNT))

#define LOAD_STRING_CNT 3
#define LOAD_STRING(vec, i, str, len) \
  (((vec)[(i)] = ERL_DRV_STRING), \
  ((vec)[(i)+1] = (ErlDrvTermData)(str)), \
  ((vec)[(i)+2] = (len)), \
  ((i)+LOAD_STRING_CNT))

#define LOAD_STRING_CONS_CNT 3
#define LOAD_STRING_CONS(vec, i, str, len) \
  (((vec)[(i)] = ERL_DRV_STRING_CONS), \
  ((vec)[(i)+1] = (ErlDrvTermData)(str)), \
  ((vec)[(i)+2] = (len)), \
  ((i)+LOAD_STRING_CONS_CNT))

#define LOAD_TUPLE_CNT 2
#define LOAD_TUPLE(vec, i, size) \
  (((vec)[(i)] = ERL_DRV_TUPLE), \
  ((vec)[(i)+1] = (size)), \
  ((i)+LOAD_TUPLE_CNT))

#define LOAD_NIL_CNT 1
#define LOAD_NIL(vec, i) \
  (((vec)[(i)] = ERL_DRV_NIL), \
  ((i)+LOAD_NIL_CNT))

#define LOAD_LIST_CNT 2
#define LOAD_LIST(vec, i, size) \
  (((vec)[(i)] = ERL_DRV_LIST), \
  ((vec)[(i)+1] = (size)), \
  ((i)+LOAD_LIST_CNT))



/*
 *
 * structs
 *
 */

typedef struct {
    gearman_client_st   client;
    int async_ref;              /* async reference id generator */

    ErlDrvPort  port;           /* the port identifier */
    ErlDrvTermData dport;       /* the port identifier as DriverTermData */
}gearman_drv_st;

typedef struct {
    uint32_t       id;          /* id used to identify reply */
    int            req;         /* Request id (e.g. SUBMIT_JOB) */
    ErlDrvTermData caller;      /* recipient of async reply */
    ErlDrvMonitor monitor;
    gearman_drv_st * drv;
} gearman_async_op;


#define NEW_ASYNC_ID(gdrv) ((gdrv)->async_ref++)

typedef gearman_task_st * (add_task_fn)(gearman_client_st *client,
                                         gearman_task_st *task,
                                         void *context,
                                         const char *function_name,
                                         const char *unique,
                                         const void *workload,
                                         size_t workload_size,
                                         gearman_return_t *ret_ptr);
typedef struct {
    int cmd;
    const char * name;
    add_task_fn * fn;
}add_task_fn_map;

const add_task_fn_map add_task_fns[] = 
{
    {GDRV_CLIENT_SUBMIT_JOB, "SUBMIT_JOB", 
        gearman_client_add_task},
    {GDRV_CLIENT_SUBMIT_JOB_BG, "SUBMIT_JOB_BG", 
        gearman_client_add_task_background},
    {GDRV_CLIENT_SUBMIT_JOB_HIGH, "SUBMIT_JOB_HIGH",
        gearman_client_add_task_high},
    {GDRV_CLIENT_SUBMIT_JOB_HIGH_BG, "SUBMIT_JOB_HIGH_BG",
        gearman_client_add_task_high_background},
    {GDRV_CLIENT_SUBMIT_JOB_LOW, "SUBMIT_JOB_LOW",
        gearman_client_add_task_low},
    {GDRV_CLIENT_SUBMIT_JOB_LOW_BG, "SUBMIT_JOB_LOW_BG",
        gearman_client_add_task_low_background}
};


#define GET_ADD_TASK_FN(Command) \
    (add_task_fns[(Command)-GDRV_CLIENT_TASK_BASE].fn)

#define GET_ADD_TASK_CMDSTR(Command) \
    (add_task_fns[(Command)-GDRV_CLIENT_TASK_BASE].name)


/* Driver interface declarations */
static int gdrv_init(void);
static ErlDrvData gdrv_start(ErlDrvPort port, char* args);
static void gdrv_stop(ErlDrvData data);
static void gdrv_flush(ErlDrvData data);
static void gdrv_ready_input(ErlDrvData data, ErlDrvEvent event);
static void gdrv_ready_output(ErlDrvData data, ErlDrvEvent event);
static int gdrv_control(ErlDrvData data, unsigned int command,
    char* buf, int len, char** rbuf, int rsize);
static void gdrv_timeout(ErlDrvData data);
static void gdrv_process_exit(ErlDrvData data, ErlDrvMonitor * monitor); 
static void gdrv_stop_select(ErlDrvEvent data, void* reserved); 

static int ctl_reply(int rep, char* buf, int len, char** rbuf, int rsize);
static int ctl_error(int err, char** rbuf, int rsize);
static int ctl_error_str(const char * desc, char ** rbuf, int rsize);

static gearman_connection_st * gdrv_find_conn(gearman_client_st * client, int fd);
static short gearman_event_convert(short event);
static gearman_return_t gdrv_watch_event(gearman_connection_st * con,
                short event, void * context);
static gearman_return_t gdrv_client_cb_created(gearman_task_st *task);
static gearman_return_t gdrv_client_cb_data(gearman_task_st *task);
static gearman_return_t gdrv_client_cb_status(gearman_task_st *task);
static gearman_return_t gdrv_client_cb_complete(gearman_task_st *task);
static int gdrv_add_task(gearman_drv_st * gdrv, int command,
        char * buf, int len, gearman_return_t * ret, uint32_t * id);

/* general control reply function */
static int ctl_reply(int rep, char* buf, int len, char** rbuf, int rsize)
{
    char* ptr;

    if ((len+1) > rsize) {
        ptr = ALLOC(len+1);
        *rbuf = ptr;
    } else {
        ptr = *rbuf;
    }
    *ptr++ = rep;
    memcpy(ptr, buf, len);
    return len+1;
}

/* general control error reply function */
static int ctl_error(int err, char** rbuf, int rsize)
{
    char response[256];		/* Response buffer. */
    char* s;
    char* t;

    for (s = erl_errno_id(err), t = response; *s; s++, t++)
        *t = tolower(*s);
    return ctl_reply(GDRV_REP_ERROR, response, t-response, rbuf, rsize);
}

/* general control error reply function */
static int ctl_error_str(const char * desc, char ** rbuf, int rsize)
{
    desc = desc? desc: "unknown";
    return ctl_reply(GDRV_REP_ERROR, (char*)desc, strlen(desc), rbuf, rsize);
}


/* alloc a new gearman_async_op struct */
static gearman_async_op * gearman_async_new(gearman_drv_st * gdrv, int command,
        ErlDrvMonitor *monitorp)
{
    int id = NEW_ASYNC_ID(gdrv);
    gearman_async_op * opp;

    opp = ALLOC(sizeof(gearman_async_op));
    if (!opp) {
        return NULL;
    }

    opp->id = id;
    opp->req = command;
    opp->caller = driver_caller(gdrv->port);
    opp->drv = gdrv;
    if (monitorp != NULL) {
        memcpy(&(opp->monitor),monitorp,sizeof(ErlDrvMonitor));
    }
    return opp;
}

/* dealloc the gearman_async_op struct */
static void gearman_async_free(gearman_async_op * op)
{
    FREE(op);
}

/* send message:
**     {gearman_async, Ref, {job_created, Handle}} 
*/
static int send_async_job_created(ErlDrvPort port, uint32_t Ref, 
        const char * handle, size_t len,
        ErlDrvTermData recipient)
{
    ErlDrvTermData spec[2*LOAD_ATOM_CNT + 
        LOAD_INT_CNT + LOAD_BUF2BINARY_CNT +
        2*LOAD_TUPLE_CNT];
    int i = 0;
    DEBUGF("send the job handle: %s\r\n", handle);
    
    i = LOAD_ATOM(spec, i, am_gearman_async);
    i = LOAD_INT(spec, i, Ref);
    {
        i = LOAD_ATOM(spec, i, am_job_created);
        i = LOAD_BUF2BINARY(spec, i, handle, len);
        i = LOAD_TUPLE(spec, i, 2);
    }
    i = LOAD_TUPLE(spec, i, 3);
    
    assert(i == sizeof(spec)/sizeof(*spec));
    
    return driver_send_term(port, recipient, spec, i);
}

/* 
 * send message:
 *      {gearman_async, Ref, 
 *          {status_res, JobHandle, Known, Running, Numerator, Denominator}} 
 */
static int send_async_status_res(ErlDrvPort port, uint32_t Ref, 
        gearman_task_st * task, ErlDrvTermData recipient)
{
    int i = 0;
    bool known, running;
    int numerator, denominator;
    const char * job_handle;

    ErlDrvTermData spec[2*LOAD_ATOM_CNT 
        + LOAD_BUF2BINARY_CNT 
        + 5*LOAD_INT_CNT 
        + 2*LOAD_TUPLE_CNT];

    job_handle = task->job_handle;
    known = gearman_task_is_known(task);
    running = gearman_task_is_running(task);
    numerator = gearman_task_numerator(task);
    denominator = gearman_task_denominator(task);

    DEBUGF("Send status_res: knonw:%d running:%d nume:%d denom:%d\r\n",
            known, running, numerator, denominator);
    
    i = LOAD_ATOM(spec, i, am_gearman_async);
    i = LOAD_INT(spec, i, Ref);
    {
        i = LOAD_ATOM(spec, i, am_status_res);
        i = LOAD_BUF2BINARY(spec, i, job_handle, strlen(job_handle));
        i = LOAD_INT(spec, i, known);
        i = LOAD_INT(spec, i, running);
        i = LOAD_INT(spec, i, numerator);
        i = LOAD_INT(spec, i, denominator);
        i = LOAD_TUPLE(spec, i, 6);
    }
    i = LOAD_TUPLE(spec, i, 3);
    
    assert(i == sizeof(spec)/sizeof(*spec));
    return driver_send_term(port, recipient, spec, i);
}


/* 
 * send message:
 *      {gearman_async, Ref, 
 *          {work_status, JobHandle, Numerator, Denominator}} 
 */
static int send_async_work_status(ErlDrvPort port, uint32_t Ref, 
        gearman_task_st * task, ErlDrvTermData recipient)
{
    int i = 0;
    int numerator, denominator;
    const char * job_handle;

    ErlDrvTermData spec[2*LOAD_ATOM_CNT 
        + LOAD_BUF2BINARY_CNT 
        + 3*LOAD_INT_CNT 
        + 2*LOAD_TUPLE_CNT];

    job_handle = task->job_handle;
    numerator = gearman_task_numerator(task);
    denominator = gearman_task_denominator(task);

    DEBUGF("Send the work_status: nume:%d denom:%d\r\n",
            numerator, denominator);
    
    i = LOAD_ATOM(spec, i, am_gearman_async);
    i = LOAD_INT(spec, i, Ref);
    {
        i = LOAD_ATOM(spec, i, am_work_status);
        i = LOAD_BUF2BINARY(spec, i, job_handle, strlen(job_handle));
        i = LOAD_INT(spec, i, numerator);
        i = LOAD_INT(spec, i, denominator);
        i = LOAD_TUPLE(spec, i, 4);
    }
    i = LOAD_TUPLE(spec, i, 3);
    
    assert(i == sizeof(spec)/sizeof(*spec));
    return driver_send_term(port, recipient, spec, i);
}

/* 
 * send message:
 *      {gearman_async, Ref, 
 *          {work_xxx, JobHandle, Data}} 
 * used by: work_data, work_warning, work_excepiton, work_complete and work_fail
 */
static int send_async_work_data(ErlDrvPort port, uint32_t Ref, int command, 
        gearman_task_st * task, ErlDrvTermData recipient)
{
    int i = 0;
    const char * job_handle = gearman_task_job_handle(task);
    const void * data = gearman_task_data(task);
    size_t data_size = gearman_task_data_size(task);
    ErlDrvTermData * am_work_xxx = NULL;

    ErlDrvTermData spec[2*LOAD_ATOM_CNT 
        + 2*LOAD_BUF2BINARY_CNT 
        + 1*LOAD_INT_CNT 
        + 2*LOAD_TUPLE_CNT];

    DEBUGF("Send work_xxx: job_handle:%s data:%*s\r\n",
            job_handle, (int)data_size, (const char *)data);

    if (task->recv->command == GEARMAN_COMMAND_WORK_DATA) {
        am_work_xxx = &am_work_data;
    } else if (task->recv->command == GEARMAN_COMMAND_WORK_WARNING) {
        am_work_xxx = &am_work_warning;
    } else if (task->recv->command == GEARMAN_COMMAND_WORK_EXCEPTION) {
        am_work_xxx = &am_work_exception;
    } else if (task->recv->command == GEARMAN_COMMAND_WORK_COMPLETE) {
        am_work_xxx = &am_work_complete;
    } else if (task->recv->command == GEARMAN_COMMAND_WORK_FAIL) {
        am_work_xxx = &am_work_fail;
    }
    
    i = LOAD_ATOM(spec, i, am_gearman_async);
    i = LOAD_INT(spec, i, Ref);
    {
        i = LOAD_ATOM(spec, i, *am_work_xxx);
        i = LOAD_BUF2BINARY(spec, i, job_handle, strlen(job_handle));
        i = LOAD_BUF2BINARY(spec, i, data, data_size);
        i = LOAD_TUPLE(spec, i, 3);
    }
    i = LOAD_TUPLE(spec, i, 3);
    
    assert(i == sizeof(spec)/sizeof(*spec));
    return driver_send_term(port, recipient, spec, i);
}



static gearman_connection_st * gdrv_find_conn(gearman_client_st * client, int fd)
{
    gearman_connection_st * con;
    if (!client) {
        return NULL;
    }
    for (con = client->universal.con_list; con != NULL;
            con = con->next) {
        if (con->fd == fd) {
            return con;
        }
    }
    return NULL;
}

static short gearman_event_convert(short event) 
{
    short mode = 0;
    if (event & POLLIN) {
        mode |= ERL_DRV_READ;
    }

    if (event & POLLOUT) {
        mode |= ERL_DRV_WRITE;
    }
    return mode;
}

static gearman_return_t gdrv_watch_event(gearman_connection_st * con,
                short event, void * context)
{
    int mode;
    gearman_drv_st * gdrv;

    if (con == NULL) {
        DEBUGF("gdrv_watch_event con is NULL\n");
        return GEARMAN_NO_SERVERS;
    }

    DEBUGF("gdrv_watch_event fd(%d) %s:%hu event:%hu\r\n", 
            con->fd, con->host, con->port, event);

    mode = gearman_event_convert(event);
    gdrv = (gearman_drv_st*)context;

    driver_select(gdrv->port, (ErlDrvEvent)con->fd, mode, 1);
    return GEARMAN_SUCCESS;
}

/* the callback for job_created */
static gearman_return_t gdrv_client_cb_created(gearman_task_st *task)
{
    gearman_async_op * opp;
    gearman_drv_st * gdrv;
    const char * job_handle;

    opp = (gearman_async_op *)gearman_task_context(task);
    assert(opp);

    gdrv = opp->drv;
    job_handle = gearman_task_job_handle(task);
    DEBUGF("Created: %s\n", job_handle);

    send_async_job_created(gdrv->port, opp->id, 
            job_handle, strlen(job_handle),
            opp->caller);
    return GEARMAN_SUCCESS;
}

/* the callback for work_data, work_warning and work_exception */
static gearman_return_t gdrv_client_cb_data(gearman_task_st *task)
{
    gearman_async_op * opp;
    gearman_drv_st * gdrv;

    opp = (gearman_async_op *)gearman_task_context(task);
    assert(opp);

    gdrv = opp->drv;
    send_async_work_data(gdrv->port, opp->id, opp->req,
            task, opp->caller);
    return GEARMAN_SUCCESS;
}

/* the callback for work_status and status_res */
static gearman_return_t gdrv_client_cb_status(gearman_task_st *task)
{
    gearman_async_op * opp;
    gearman_drv_st * gdrv;

    opp = (gearman_async_op *)gearman_task_context(task);
    assert(opp);
    gdrv = opp->drv;
    if (opp->req == GDRV_CLIENT_GET_STATUS) {
        send_async_status_res(gdrv->port, opp->id, task, opp->caller);
    } else {
        send_async_work_status(gdrv->port, opp->id, task, opp->caller);
    }
    
    return GEARMAN_SUCCESS;
}

/* the callback for work_complete, work_fail */
static gearman_return_t gdrv_client_cb_complete(gearman_task_st *task)
{
    gearman_async_op * opp;
    gearman_drv_st * gdrv;

    opp = (gearman_async_op *)gearman_task_context(task);
    assert(opp);

    gdrv = opp->drv;
    send_async_work_data(gdrv->port, opp->id, opp->req,
            task, opp->caller);
    gearman_async_free(opp);

    return GEARMAN_SUCCESS;
}

/* add task and run the tasks */
static int gdrv_add_task(gearman_drv_st * gdrv, int command,
        char * buf, int len, gearman_return_t * ret, uint32_t * id) 
{
    char * fun_name;
    void * workload;
    uint32_t name_len, workload_len;
    gearman_task_st * task;
    gearman_async_op * opp;
    const char * cmdstr = GET_ADD_TASK_CMDSTR(command);
    add_task_fn * fn = GET_ADD_TASK_FN(command);

    if (!gdrv || !fn || !ret) return -1;

    if (len <= 8) {
        return EINVAL;
    }
    name_len = (uint32_t)get_int32(buf);
    workload_len = (uint32_t)get_int32(buf+4+name_len);
    fun_name = buf + 4;
    fun_name[name_len] = '\0';

    workload = buf + 4 + name_len + 4;
    DEBUGF("gdrv_add_task %s len:%d name:%s workload len:%d\r\n", 
            cmdstr, len, fun_name, workload_len);

    opp = gearman_async_new(gdrv, command, NULL);
    if (!opp) {
        return ENOMEM;
    }

    /* add the task, the opp as task context */
    task = fn(&gdrv->client, NULL, opp,
            fun_name, NULL, workload, workload_len, ret);

    if (*ret != GEARMAN_SUCCESS) return -1;

    /* try to run the tasks (in non-blocking) */
    *ret = gearman_client_run_tasks(&gdrv->client);
    DEBUGF("gearman_client_run_tasks ret:%d\r\n", *ret);
    if (id) *id = opp->id;

    return 0;
}


/* the debug print msg function */
inline static void debug_msg(const char * fmt, ...)
{
    char buf[64];
    time_t now;
    va_list ap;
    va_start(ap, fmt);
    now = time(NULL);
    strftime(buf, 64, "%F %T", localtime(&now));
    fprintf(stderr, "[%s] ", buf);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
}


/* driver entry implmention */
static int gdrv_init(void)
{
    DEBUGF("gdrv_init\r\n");
    INIT_ATOM(ok);
    INIT_ATOM(error);
    INIT_ATOM(gearman_async);
    INIT_ATOM(gearman_reply);
    
    INIT_ATOM(job_created);
    INIT_ATOM(work_data);
    INIT_ATOM(work_warning);
    INIT_ATOM(work_status);
    INIT_ATOM(work_complete);
    INIT_ATOM(work_fail);
    INIT_ATOM(work_exception);
    INIT_ATOM(status_res);

    return 0;
}

static ErlDrvData gdrv_start(ErlDrvPort port, char* args)
{
    gearman_drv_st * gdrv; 
    gearman_client_st * client;

    DEBUGF("gdrv_start: (%ld) args:%s\r\n", (long)port, args);

    gdrv = ALLOC(sizeof(gearman_drv_st));
    if (gdrv == NULL)
        return NULL;

    gdrv->port = port;
    gdrv->dport = driver_mk_port(port);
    gdrv->async_ref = 1;

    if (gearman_client_create(&gdrv->client) == NULL) {
        DEBUGF("gearman_client_create error!\n");
        FREE(gdrv);
        return NULL;
    }

    client = &gdrv->client;
    /* set client */
    client->universal.event_watch_fn = gdrv_watch_event;
    client->universal.event_watch_context = (void*)gdrv;

    gearman_client_set_created_fn(client, gdrv_client_cb_created);
    gearman_client_set_data_fn(client, gdrv_client_cb_data);
    gearman_client_set_warning_fn(client, gdrv_client_cb_data);
    gearman_client_set_exception_fn(client, gdrv_client_cb_data);
    gearman_client_set_status_fn(client, gdrv_client_cb_status);
    gearman_client_set_complete_fn(client, gdrv_client_cb_complete);
    gearman_client_set_fail_fn(client, gdrv_client_cb_complete);

    return (ErlDrvData)gdrv;
}

static void gdrv_stop(ErlDrvData data)
{
    gearman_drv_st * gdrv = (gearman_drv_st *)data;
    DEBUGF("gdrv_stop \r\n");

    gearman_client_free(&gdrv->client);
    FREE(gdrv);
}

static void gdrv_flush(ErlDrvData data)
{
    DEBUGF("gdrv_flush\n");
}

static void gdrv_ready_input(ErlDrvData data, ErlDrvEvent event)
{
    gearman_return_t ret;
    gearman_drv_st * gdrv = (gearman_drv_st *)data;
    gearman_connection_st * con;
    DEBUGF("gdrv_ready_input fd(%d) \r\n", (int)event);

    con = gdrv_find_conn(&gdrv->client, (int)event);
    if (con == NULL) return;

    con->options.ready = true;
    con->revents = POLLIN;
    con->events &= (short)~POLLIN;

    ret = gearman_client_run_tasks(&gdrv->client);
    DEBUGF("gdrv_ready_input ret:%d\r\n", ret);
}

static void gdrv_ready_output(ErlDrvData data, ErlDrvEvent event)
{
    gearman_return_t ret;
    gearman_drv_st * gdrv = (gearman_drv_st *)data;
    gearman_connection_st * con;
    DEBUGF("gdrv_ready_output fd(%d) \r\n", (int)event);

    con = gdrv_find_conn(&gdrv->client, (int)event);
    if (con == NULL) return;

    con->options.ready = true;
    con->revents = POLLOUT;
    /* don't ask for the POOLLOUT */
    if (!(con->events & POLLOUT)) {
        driver_select(gdrv->port, event, ERL_DRV_WRITE, 0);
    }
    con->events &= (short)~POLLOUT;

    ret = gearman_client_run_tasks(&gdrv->client);
    DEBUGF("gdrv_ready_output ret:%d\r\n", ret);
}

static int gdrv_control(ErlDrvData data, unsigned int command,
    char* buf, int len, char** rbuf, int rsize)
{
    gearman_return_t ret;
    gearman_drv_st * gdrv = (gearman_drv_st *)data;
    switch (command) {
        case GDRV_CLIENT_OPT: /* client options */
        {
            int opt;
            DEBUGF("gdrv_control(%ld): OPT\r\n", 
                    (long)gdrv->port);
            if (len != 4) {
                return ctl_error(EINVAL, rbuf, rsize);
            }
            opt = get_int32(buf);
            gearman_client_set_options(&gdrv->client, opt);
            return ctl_reply(GDRV_REP_OK, NULL, 0, rbuf, rsize);
        }
        case GDRV_CLIENT_ADD_SERVER: /* add server */
        {
            DEBUGF("gdrv_control(%ld): ADD_SERVER %s\r\n", 
                    (long)gdrv->port, buf);
            ret = gearman_client_add_servers(&gdrv->client, buf);
            if (ret != GEARMAN_SUCCESS) {
                return ctl_error(EINVAL, rbuf, rsize);
            }
            return ctl_reply(GDRV_REP_OK, NULL, 0, rbuf, rsize);
        }
        /* submit job */
        case GDRV_CLIENT_SUBMIT_JOB: 
        case GDRV_CLIENT_SUBMIT_JOB_LOW:
        case GDRV_CLIENT_SUBMIT_JOB_HIGH:
        {
            int nret;
            uint32_t taskid;
            char tbuf[4];

            nret = gdrv_add_task(gdrv, command, buf, len, &ret, &taskid);
            if (ret != GEARMAN_SUCCESS && 
                    ret != GEARMAN_IO_WAIT &&
                    ret != GEARMAN_PAUSE) {
                const char * str = gearman_client_error(&gdrv->client);
                return ctl_error_str(str, rbuf, rsize);
            }
            if (nret != 0) {
                return ctl_error(nret, rbuf, rsize);
            }

            /* add job successly return the task id*/
            put_int32(taskid, tbuf);
            return ctl_reply(GDRV_REP_OK, tbuf, 4, rbuf, rsize);
        }
        /* submit job background*/
        case GDRV_CLIENT_SUBMIT_JOB_BG: 
        case GDRV_CLIENT_SUBMIT_JOB_LOW_BG: 
        case GDRV_CLIENT_SUBMIT_JOB_HIGH_BG: 
        {
            int nret;
            uint32_t taskid;
            char tbuf[4];

            nret = gdrv_add_task(gdrv, command, buf, len, &ret, &taskid);
            if (ret != GEARMAN_SUCCESS && 
                    ret != GEARMAN_IO_WAIT &&
                    ret != GEARMAN_PAUSE) {
                const char * str = gearman_client_error(&gdrv->client);
                return ctl_error_str(str, rbuf, rsize);
            }
            if (nret != 0) {
                return ctl_error(nret, rbuf, rsize);
            }

            /* add job successly return the task id*/
            put_int32(taskid, tbuf);
            return ctl_reply(GDRV_REP_OK, tbuf, 4, rbuf, rsize);
        }
        case GDRV_CLIENT_GET_STATUS:
        {
            char * job_handle;
            char tbuf[4];
            gearman_task_st * task;
            gearman_async_op * opp;

            if (len <= 1) {
                return ctl_error(EINVAL, rbuf, rsize);
            }
            job_handle = buf;
            DEBUGF("gdrv_control(%ld): GET_STATUS job_handle:%s\n", 
                    (long)gdrv->port, job_handle);

            opp = gearman_async_new(gdrv, command, NULL);
            if (!opp) {
                return ctl_error(ENOMEM, rbuf, rsize);
            }

            /* add the task, the opp as task context */
            task = gearman_client_add_task_status(
                    &gdrv->client, NULL, opp,
                    job_handle,
                    &ret);

            DEBUGF("gearman_client_add_task_status ret:%d\r\n", ret);
            if (ret != GEARMAN_SUCCESS && 
                    ret != GEARMAN_IO_WAIT &&
                    ret != GEARMAN_PAUSE) {
                const char * str = gearman_client_error(&gdrv->client);
                return ctl_error_str(str, rbuf, rsize);
            }
            /* try to run the tasks (in non-blocking) */
            ret = gearman_client_run_tasks(&gdrv->client);
            if (ret != GEARMAN_SUCCESS && 
                    ret != GEARMAN_IO_WAIT &&
                    ret != GEARMAN_PAUSE) {
                const char * str;
                str = gearman_client_error(&gdrv->client);
                return ctl_error_str(str, rbuf, rsize);
            }
            put_int32(opp->id, tbuf);

            /* add job successly return the task id*/
            return ctl_reply(GDRV_REP_OK, tbuf, 4, rbuf, rsize);
        }
        default:
            return ctl_error(EBADRQC, rbuf, rsize);
    }

    return 0;
}

static void gdrv_timeout(ErlDrvData data)
{
    DEBUGF("gdrv_timeout \r\n");

}

static void gdrv_process_exit(ErlDrvData data, ErlDrvMonitor * monitor)
{
    DEBUGF("gdrv_process_exit \r\n");

}

static void gdrv_stop_select(ErlDrvEvent data, void* reserved)
{
    DEBUGF("gdrv_stop_select \r\n");

}

static ErlDrvEntry gearman_drv_entry = {
    gdrv_init,          /* init */
    gdrv_start,         /* start */
    gdrv_stop,          /* stop */
    NULL,               /* output */
    gdrv_ready_input,   /* ready_input */
    gdrv_ready_output,  /* ready_output */ 
    "gearman_drv",      /* name */
    NULL,               /* finish */
    NULL,               /* handler */
    gdrv_control,       /* control */
    gdrv_timeout,       /* timeout */
    NULL,               /* outputv */
    NULL,               /* ready_async */
    gdrv_flush,         /* flush */
    NULL,               /* call */
    NULL,               /* event */
    ERL_DRV_EXTENDED_MARKER,
    ERL_DRV_EXTENDED_MAJOR_VERSION,
    ERL_DRV_EXTENDED_MINOR_VERSION,
    ERL_DRV_FLAG_USE_PORT_LOCKING|ERL_DRV_FLAG_SOFT_BUSY,
    NULL,               /* handler2 */
    gdrv_process_exit,  /* process_exit */
    gdrv_stop_select    /* stop select */
};


/* 
 * This is the init function called after this driver has been loaded.
 * It must *not* be declared static. Must return the address to 
 * the driver entry.
 */
DRIVER_INIT(gearman_drv)
{
    return &gearman_drv_entry;
}
