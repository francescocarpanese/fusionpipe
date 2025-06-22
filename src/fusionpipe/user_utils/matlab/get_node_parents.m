function parent_ids = get_node_parents(conn, node_id)
    % Retrieve the parent IDs of a given node from the database.
    % conn: Database connection object
    % node_id: ID of the node whose parents are to be retrieved
    query = sprintf('SELECT parent_id FROM node_relation WHERE child_id = ''%s''', node_id);
    data = fetch(conn, query);
    if isempty(data)
        parent_ids = {};
    else
        parent_ids = data{:,1};
        parent_ids = cellstr(parent_ids);
    end
end