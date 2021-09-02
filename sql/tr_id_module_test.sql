CREATE EXTENSION  tr_id_module;
SET tr_id_module.log_path = "/home/postgres/logging";
SELECT set_new_log();
