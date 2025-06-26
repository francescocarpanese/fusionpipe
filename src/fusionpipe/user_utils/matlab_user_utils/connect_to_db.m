%CONNECT_TO_DB Connect to a PostgreSQL database using a connection string.
%   CONN = CONNECT_TO_DB(DB_URL) establishes a connection to a PostgreSQL
%   database using the provided DB_URL connection string. The connection string
%   should be in the format:
%
%       'dbname=<yourdb> user=<youruser> password=<yourpassword> host=<host> port=<port>'
%
%   If DB_URL is not provided, the function attempts to read the connection
%   string from the 'DATABASE_URL' environment variable.
%
%   Input:
%       db_url - (optional) Connection string for the PostgreSQL database.
%
%   Output:
%       conn   - Database connection object.
%
%   Example:
%       conn = connect_to_db('dbname=mydb user=me password=secret host=localhost port=5432');
%
%   See also: postgresql
function conn = connect_to_db(db_url)
    % Connect to a PostgreSQL database using the provided db_url.
    % Example db_url: 'dbname=<yourdb> user=<youruser> password=<yourpassword> host=localhost port=<port>'
    if nargin < 1 || isempty(db_url)
        db_url = getenv('DATABASE_URL');
    end
    % Parse db_url into components if needed, or use as connection string
    % Parse the DATABASE_URL environment variable
    tokens = regexp(db_url, 'dbname=(\S+)\s+user=(\S+)\s+password=(\S+)\s+host=(\S+)\s+port=(\d+)', 'tokens');
    if isempty(tokens)
        error('Invalid DATABASE_URL format.');
    end
    tokens = tokens{1};
    dbname = tokens{1};
    user = tokens{2};
    password = tokens{3};
    host = tokens{4};
    port = str2double(tokens{5});

    % Establish the connection
    conn = postgresql(user, password, 'Server', host, ...
        'DatabaseName', dbname, 'PortNumber', port);
end