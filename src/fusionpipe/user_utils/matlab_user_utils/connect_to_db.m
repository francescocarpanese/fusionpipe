function conn = connect_to_db(db_url)
    % Connect to a PostgreSQL database using peer authentication.
    % Example db_url: 'dbname=fusionpipe_prod1 user=fusionpipeadmin port=5432'
    if nargin < 1 || isempty(db_url)
        db_url = getenv('DATABASE_URL');
    end
    % Parse db_url for dbname, user, and port
    tokens = regexp(db_url, 'dbname=(\S+)\s+user=(\S+)\s+port=(\d+)', 'tokens');
    if isempty(tokens)
        error('Invalid DATABASE_URL format. Expected: dbname=<db> user=<user> port=<port>');
    end
    tokens = tokens{1};
    dbname = tokens{1};
    user = tokens{2};
    port = str2double(tokens{3});

    % Use peer authentication (no password)
    conn = postgresql(user, '', 'DatabaseName', dbname, 'PortNumber', port);
end