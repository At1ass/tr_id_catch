#include "postgres.h"

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
    uint64 data[50];
} FullTransactionIdSharedState;

static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static FullTransactionIdSharedState* fti_state = NULL;
static char* default_log_path = "/home/postgres/log.log";
static char* log_path = NULL;
static FILE* log_file = NULL;
static FullTransactionId tr_id;

static void tim_sigterm_hadler(SIGNAL_ARGS);
static void tim_sighup_hadler(SIGNAL_ARGS);

// static BackgroundWorkerHandle* handler;
//
int tr_id_module_worker_start(void);
//
void FTI_inv_callback(XactEvent event, void* arg);
void FTI_inv_callback(XactEvent event, void* arg) {
    if (event != XACT_EVENT_PRE_COMMIT) return;

    tr_id = GetTopFullTransactionId();
    LWLockAcquire(&fti_state->lock, LW_EXCLUSIVE);
    // fti_state->data = tr_id;

    fti_state->data[fti_state->num_elem] = U64FromFullTransactionId(tr_id);
    fti_state->num_elem++;

    LWLockRelease(&fti_state->lock);

    elog(LOG, "TransactionId = %lu", U64FromFullTransactionId(tr_id));
}
//
void stop_logging(void);
void stop_logging(void) {
    fclose(log_file);

    elog(LOG, "Logging stopped");
}

void start_logging(char*);
void start_logging(char* path) {
    log_file = fopen(path, "a+");

    elog(LOG, "Logging started");
}

PG_FUNCTION_INFO_V1(set_new_log);
Datum set_new_log(PG_FUNCTION_ARGS) {
    //    elog(LOG, "Start switching: new path - %s", log_path);

    //    stop_logging();

    //  start_logging(log_path);
    elog(LOG, "Switch finished");
}

PG_FUNCTION_INFO_V1(get_top_tr_id);
Datum get_top_tr_id(PG_FUNCTION_ARGS) { PG_RETURN_UINT64(1); }

void init_shmem(void);
void init_shmem(void) {
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

void spooler_main(Datum) pg_attribute_noreturn();
void spooler_main(Datum main_arg) {
    //
    // ereport(DEBUG1, (errmsg("spooler_main started: Log path: qwe")));

    uint64* buf;
    size_t size;
    bool need_clean = false;

    pqsignal(SIGTERM, tim_sigterm_hadler);
    pqsignal(SIGHUP, tim_sighup_hadler);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    BackgroundWorkerUnblockSignals();

    elog(LOG, "BGWorker started");

    // init_shmem();

    while (!got_sigterm) {
	// tr_id = GetTopFullTransactionId();

	// if (c % 10000 != 0) continue;

	LWLockAcquire(&fti_state->lock, LW_EXCLUSIVE);
	// tr.value = fti_state->data.value;
	if (fti_state->num_elem > 10) {
	    need_clean = true;
	    buf = palloc(fti_state->num_elem * sizeof(uint64));
	    memcpy(buf, fti_state->data, fti_state->num_elem * sizeof(uint64));
	    memset(fti_state->data, 0, fti_state->num_elem * sizeof(uint64));
	    size = fti_state->num_elem;
	    fti_state->num_elem = 0;
	}
	LWLockRelease(&fti_state->lock);
	if (need_clean) {
	    for (size_t i = 0; i < size; ++i) {
		// elog(LOG, "TransactionId from spooler_main: %lu", buf[i]);
		fprintf(log_file, "FullTransactionId: %lu\n", buf[i]);
	    }
	    fflush(log_file);
	    need_clean = false;
	}
	//	if (tr_id.value != old_tr_id.value)
	//	    elog(LOG, "%lu", U64FromFullTransactionId(tr_id));
    }

    //	if (U64FromFullTransactionId(2) % 10 == 2)
    //	    ereport(NOTICE, errmsg("FulltyransactionId: %lu",
    //				   U64FromFullTransactionId(2)));
    proc_exit(0);
}

void _PG_init(void);
void _PG_init(void) {
    if (!process_shared_preload_libraries_in_progress) return;
    BackgroundWorker bg_worker;
    DefineCustomStringVariable("tr_id_module.log_path", "log_path", NULL,
			       &log_path, "", PGC_USERSET, 0, NULL, NULL, NULL);

    //    if (!process_shared_preload_libraries_in_progress) {
    //	elog(LOG, "process_shared_preload_libraries_in_progress =
    // false"); 	return;
    //    }

    //    if (process_shared_preload_libraries_in_progress) {

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

    //    bg_worker
    RegisterBackgroundWorker(&bg_worker);
    //	return;
    //    }
    // return;
    //  }

    RequestAddinShmemSpace(MAXALIGN(sizeof(FullTransactionIdSharedState)));
    //    if (!tr_id_module_worker_start()) {
    //	ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
    //			errmsg("worker not REGISTER"),
    //			errhint("Details in server log")));
    //    return;
    //}

    //    ereport(LOG, errmsg("OK"));
    //    if (!RegisterDynamicBackgroundWorker(&bg_worker, &handler)) {
    //	ereport(ERROR, (errmsg("ALARM")));
    //    }
    if (IsUnderPostmaster) init_shmem();
    start_logging(default_log_path);
    RegisterXactCallback(FTI_inv_callback, NULL);
}

// int tr_id_module_worker_start(void) {
//    BackgroundWorker bgw;
//    BackgroundWorkerHandle* bgh;
//    BgwHandleStatus status;
//    pid_t pid;
//
//    elog(LOG, ("Im HEARE"));
//    memset(&bgw, 0, sizeof(bgw));
//
//    bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
//    bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
//    snprintf(bgw.bgw_library_name, BGW_MAXLEN, "tr_id_module");
//    snprintf(bgw.bgw_function_name, BGW_MAXLEN, "spooler_main");
//    snprintf(bgw.bgw_name, BGW_MAXLEN, "tr_id_module worker");
//    snprintf(bgw.bgw_type, BGW_MAXLEN, "tr_id_module");
//
//    bgw.bgw_restart_time = BGW_NEVER_RESTART;
//    bgw.bgw_notify_pid = MyProcPid;
//    bgw.bgw_main_arg = (Datum)0;
//
//    if (!RegisterDynamicBackgroundWorker(&bgw, &bgh)) return 1;
//
//    status = WaitForBackgroundWorkerStartup(bgh, &pid);
//
//    if (status == BGWH_STOPPED)
//	ereport(ERROR,
//		(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
//		 errmsg("worker not start"), errhint("Details in server log")));
//
//    if (status == BGWH_POSTMASTER_DIED)
//	ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
//			errmsg("worker not start without postmaster"),
//			errhint("Kill all proc and restart DB")));
//
//    Assert(status == BGWH_STARTED);
//
//    return 1;
//}

static void tim_sigterm_hadler(SIGNAL_ARGS) {
    int save_errno = errno;

    got_sigterm = true;

    if (MyProc) SetLatch(&MyProc->procLatch);

    errno = save_errno;
}

static void tim_sighup_hadler(SIGNAL_ARGS) {
    int save_errno = errno;

    got_sighup = true;

    if (MyProc) SetLatch(&MyProc->procLatch);

    errno = save_errno;
}

// void _PG_fini(void);
// void _PG_fini(void) {
//    //   stop_logging();
//
//    TerminateBackgroundWorker(handler);
//    UnregisterXactCallback(FTI_inv_callback, NULL);
//}
//
