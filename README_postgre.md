sudo zypper install postgresql-server postgresql-contrib



sudo systemctl enable postgresql
sudo systemctl start postgresql

# Start new database specifying the folder
sudo -u postgres initdb -D /misc/carpanes/fusionpipe/bin


# Create a user with enough priviledges for testing purposes
CREATE USER fusionpipe_test WITH PASSWORD 'testpassword' CREATEDB;
CREATE DATABASE fusionpipe_test OWNER fusionpipe_test;

psql -U postgres -f test_setup.sql