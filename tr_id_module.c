#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/shmem.h"

#include <stdio.h>
#include "access/xact.h"
#include "utils/guc_tables.h"

PG_MODULE_MAGIC;

typedef struct {
    LWLock lock;
    size_t num_elem;
    uint64 data[1024];
} FullTransactionIdSharedState;

// signals variables
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

//-----------------------
// Function definition
//-----------------------

// Xact callback func
void FTI_inv_callback(XactEvent event, void* arg);

// logging functions
void stop_logging(void);
void start_logging(void);

// Register func
PG_FUNCTION_INFO_V1(set_new_log);
PG_FUNCTION_INFO_V1(get_top_tr_id);

// Signal handlers
static void tim_sigterm_hadler(SIGNAL_ARGS);
static void tim_sighup_hadler(SIGNAL_ARGS);

// Background Worker main func
void spooler_main(Datum) pg_attribute_noreturn();

// Init Shmem func
void init_shmem(void);

// Module init func
void _PG_init(void);

//-------------------
// Variables
//-------------------
//
static FullTransactionIdSharedState* fti_state = NULL;

static char* log_path = NULL;
static FILE* log_file = NULL;
static int spool_limit;

static FullTransactionId tr_id;

//------------------------
// Start code
//------------------------

void
FTI_inv_callback(XactEvent event, void* arg) {
    if (event != XACT_EVENT_PRE_COMMIT)
        return;

    tr_id = GetCurrentFullTransactionIdIfAny();
    if (!FullTransactionIdIsValid(tr_id))
        return;

    LWLockAcquire(&fti_state->lock, LW_EXCLUSIVE);

    fti_state->data[fti_state->num_elem] = U64FromFullTransactionId(tr_id);
    fti_state->num_elem++;

    LWLockRelease(&fti_state->lock);

    elog(LOG, "TransactionId = %lu", U64FromFullTransactionId(tr_id));
}
void
stop_logging(void) {
    fclose(log_file);

    elog(LOG, "Logging stopped");
}

void
start_logging(void) {
    log_file = fopen(log_path, "a+");

    elog(LOG, "Logging started");
}

Datum
set_new_log(PG_FUNCTION_ARGS) {
    elog(LOG, "Start switching: new path - %s", log_path);

    stop_logging();

    start_logging();
    elog(LOG, "Switch finished");

    PG_RETURN_VOID();
}

Datum
get_top_tr_id(PG_FUNCTION_ARGS) {
    PG_RETURN_UINT64(1);
}

void
init_shmem(void) {
    bool found;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    fti_state = ShmemInitStruct("fri_catch",
                                sizeof(FullTransactionIdSharedState), &found);

    if (!found) {
        LWLockInitialize(&fti_state->lock, LWLockNewTrancheId());
        memset(fti_state->data, 0, sizeof(FullTransactionId) * 50);
        fti_state->num_elem = 0;
    }

    LWLockRelease(AddinShmemInitLock);
    LWLockRegisterTranche(fti_state->lock.tranche, "fri_catch");
}

void
spooler_main(Datum main_arg) {
    uint64* buf;
    size_t size;
    bool need_clean = false;

    pqsignal(SIGTERM, tim_sigterm_hadler);
    pqsignal(SIGHUP, tim_sighup_hadler);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    BackgroundWorkerUnblockSignals();

    elog(LOG, "BGWorker started: spool_limit = %d", spool_limit);

    while (!got_sigterm) {
        if (fti_state->num_elem > spool_limit) {
            need_clean = true;
            LWLockAcquire(&fti_state->lock, LW_EXCLUSIVE);
            buf = palloc(fti_state->num_elem * sizeof(uint64));
            memcpy(buf, fti_state->data, fti_state->num_elem * sizeof(uint64));
            memset(fti_state->data, 0, fti_state->num_elem * sizeof(uint64));
            size = fti_state->num_elem;
            fti_state->num_elem = 0;
            LWLockRelease(&fti_state->lock);
        }
        if (need_clean) {
            for (size_t i = 0; i < size; ++i) {
                fprintf(log_file, "FullTransactionId: %lu\n", buf[i]);
            }
            fflush(log_file);
            need_clean = false;
        }
    }

    proc_exit(0);
}

void
_PG_init(void) {
    BackgroundWorker bg_worker;
    if (!process_shared_preload_libraries_in_progress)
        return;

    if (IsUnderPostmaster) {
        init_shmem();
        return;
    }
    DefineCustomStringVariable("tr_id_module.log_path", "log_path", NULL,
                               &log_path, "/home/postgres/log_tr_id_catch",
                               PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("tr_id_module.spool_limit", "log_path", NULL,
                            &spool_limit, 100, 15, 900, PGC_SIGHUP, 0, NULL,
                            NULL, NULL);

    shmem_startup_hook = init_shmem;
    memset(&bg_worker, 0, sizeof(bg_worker));

    bg_worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    bg_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    snprintf(bg_worker.bgw_library_name, BGW_MAXLEN, "tr_id_module");
    snprintf(bg_worker.bgw_function_name, BGW_MAXLEN, "spooler_main");
    snprintf(bg_worker.bgw_name, BGW_MAXLEN, "tr_id_module worker");
    snprintf(bg_worker.bgw_type, BGW_MAXLEN, "tr_id_module");

    bg_worker.bgw_restart_time = BGW_NEVER_RESTART;
    bg_worker.bgw_notify_pid = 0;
    bg_worker.bgw_main_arg = (Datum)0;

    RegisterBackgroundWorker(&bg_worker);

    RequestAddinShmemSpace(MAXALIGN(sizeof(FullTransactionIdSharedState)));
    start_logging();
    RegisterXactCallback(FTI_inv_callback, NULL);
}

static void
tim_sigterm_hadler(SIGNAL_ARGS) {
    int save_errno = errno;

    got_sigterm = true;

    if (MyProc)
        SetLatch(&MyProc->procLatch);

    errno = save_errno;
}

static void
tim_sighup_hadler(SIGNAL_ARGS) {
    int save_errno = errno;

    got_sighup = true;

    if (MyProc)
        SetLatch(&MyProc->procLatch);

    errno = save_errno;
}

