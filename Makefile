EXTENSION = tr_id_module        
MODULE_big = tr_id_module
DATA = tr_id_module--1.0.sql   
MODULES = tr_id_module          
OBJS = tr_id_module.o
REGRESS = tr_id_module_test
REGRESS_OPTS = --temp-config $(top_srcdir)../../contrib/tr_id_module/tr_id_module.conf

#PG_CONFIG = pg_config
PG_CONFIG = /usr/local/pgsql/bin/pg_config
#PG_CONFIG = /usr/lib/postgresql/12/bin/pg_config 
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
