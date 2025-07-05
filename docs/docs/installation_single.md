Under construction


From the postgres OS account
createdb fusionpipe_dev

psql -U postgres -c "CREATE ROLE carpanes;"

psql -U postgres -d fusionpipe_dev -c "GRANT CONNECT ON DATABASE fusionpipe_dev TO carpanes;"

psql -U postgres -d fusionpipe_dev -c "GRANT ALL ON SCHEMA public TO carpanes;"
psql -U postgres -d fusionpipe_dev -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO carpanes;"
psql -U postgres -d fusionpipe_dev -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO carpanes;"
psql -U postgres -d fusionpipe_dev -c "GRANT CREATE ON DATABASE fusionpipe_dev TO carpanes;"

psql -U postgres -c "ALTER ROLE carpanes CREATEDB;"

createdb fusionpipe_test

psql -U postgres -d fusionpipe_test -c "GRANT ALL PRIVILEGES ON DATABASE fusionpipe_test TO carpanes;"