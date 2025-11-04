-- +goose Up

-- From http://pgtune.leopard.in.ua/

-- DB Version: 16
-- OS Type: linux
-- DB Type: oltp
-- Total Memory (RAM): 2 GB
-- CPUs num: 2
-- Data Storage: ssd

ALTER SYSTEM SET
 max_connections = '300';
ALTER SYSTEM SET
 shared_buffers = '512MB';
ALTER SYSTEM SET
 effective_cache_size = '1536MB';
ALTER SYSTEM SET
 maintenance_work_mem = '128MB';
ALTER SYSTEM SET
 checkpoint_completion_target = '0.9';
ALTER SYSTEM SET
 wal_buffers = '16MB';
ALTER SYSTEM SET
 default_statistics_target = '100';
ALTER SYSTEM SET
 random_page_cost = '1.1';
ALTER SYSTEM SET
 effective_io_concurrency = '200';
ALTER SYSTEM SET
 work_mem = '1702kB';
ALTER SYSTEM SET
 huge_pages = 'off';
ALTER SYSTEM SET
 min_wal_size = '2GB';
ALTER SYSTEM SET
 max_wal_size = '8GB';

-- +goose Down
ALTER SYSTEM RESET max_connections;
ALTER SYSTEM RESET shared_buffers;
ALTER SYSTEM RESET effective_cache_size;
ALTER SYSTEM RESET maintenance_work_mem;
ALTER SYSTEM RESET checkpoint_completion_target;
ALTER SYSTEM RESET wal_buffers;
ALTER SYSTEM RESET default_statistics_target;
ALTER SYSTEM RESET random_page_cost;
ALTER SYSTEM RESET effective_io_concurrency;
ALTER SYSTEM RESET work_mem;
ALTER SYSTEM RESET huge_pages;
ALTER SYSTEM RESET min_wal_size;
ALTER SYSTEM RESET max_wal_size;
