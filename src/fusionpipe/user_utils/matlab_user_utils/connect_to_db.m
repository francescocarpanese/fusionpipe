function conn = connect_to_db(db_url)
    % Connect to a PostgreSQL database using peer authentication.
    % Example db_url: 'dbname=fusionpipe_prod1 port=5432' User will be taken from the OS user. Connection as peer authentication.
    if nargin < 1 || isempty(db_url)
        db_url = getenv('DATABASE_URL');
    end
    % Parse db_url for dbname, user, and port
    tokens = regexp(db_url, 'dbname=(\S+)\s+port=(\d+)', 'tokens');
    if isempty(tokens)
        error('Invalid DATABASE_URL format. Expected: dbname=<db> user=<user> port=<port>');
    end
    tokens = tokens{1};
    dbname = tokens{1};
    port = str2double(tokens{2});

    % Use peer authentication (no password)
    conn = postgresql('', '', 'DatabaseName', dbname, 'PortNumber', port);
end