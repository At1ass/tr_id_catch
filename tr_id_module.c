#include "postgres.h"

#include "fmgr.h"

#include <stdio.h>
#include "access/xact.h"
#include "postmaster/bgworker.h"
#include "utils/guc_tables.h"

PG_MODULE_MAGIC;

static char* default_log_path = "/home/postgres/log.log";
static char* log_path = NULL;
static FILE* log_file = NULL;
static FullTransactionId tr_id;
static BackgroundWorkerHandle* handler;

void FTI_inv_callback(XactEvent event, void* arg);
void FTI_inv_callback(XactEvent event, void* arg) {
    if (event != XACT_EVENT_PRE_COMMIT) return;

    tr_id = GetTopFullTransactionId();

    elog(LOG, "TransactionId = %lu", U64FromFullTransactionId(tr_id));
}

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
    elog(LOG, "Start switching: new path - %s", log_path);
    stop_logging();

    start_logging(log_path);
    elog(LOG, "Switch finished");
}

PG_FUNCTION_INFO_V1(get_top_tr_id);
Datum get_top_tr_id(PG_FUNCTION_ARGS) {
    PG_RETURN_UINT64(U64FromFullTransactionId(tr_id));
}

void spooler_main(Datum) pg_attribute_noreturn();
void spooler_main(Datum main_arg) {
    ereport(DEBUG1,
	    (errmsg("spooler_main started: Log path: %s", default_log_path)));

    BackgroundWorkerUnblockSignals();
    for (;;) {
	if (U64FromFullTransactionId(tr_id) % 10 == 2)
	    ereport(NOTICE, errmsg("FulltyransactionId: %lu",
				   U64FromFullTransactionId(tr_id)));
    }
}

void _PG_init(void);
void _PG_init(void) {
    BackgroundWorker bg_worker;
    DefineCustomStringVariable("tr_id_module.log_path", "log_path", NULL,
			       &log_path, "", PGC_USERSET, 0, NULL, NULL, NULL);

    tr_id = GetTopFullTransactionId();
    start_logging(default_log_path);

    memset(&bg_worker, 0, sizeof(bg_worker));

    bg_worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    bg_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    snprintf(bg_worker.bgw_library_name, BGW_MAXLEN, "tr_id_module");
    snprintf(bg_worker.bgw_function_name, BGW_MAXLEN, "spooler_main");
    snprintf(bg_worker.bgw_name, BGW_MAXLEN, "Spooler process");
    snprintf(bg_worker.bgw_type, BGW_MAXLEN, "Spooler process");

    bg_worker.bgw_restart_time = 1;
    bg_worker.bgw_notify_pid = 0;
    //    bg_worker.bgw_main_arg = (Datum)0;

    //    bg_worker
    RegisterBackgroundWorker(&bg_worker);
    //    if (!RegisterDynamicBackgroundWorker(&bg_worker, &handler)) {
    //	ereport(ERROR, (errmsg("ALARM")));
    //    }
    RegisterXactCallback(FTI_inv_callback, NULL);
}

void _PG_fini(void);
void _PG_fini(void) {
    stop_logging();

    TerminateBackgroundWorker(handler);
    UnregisterXactCallback(FTI_inv_callback, NULL);
}

