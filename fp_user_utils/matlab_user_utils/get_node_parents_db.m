%GET_NODE_PARENTS_DB Retrieve the parent IDs of a given node from the database.
%   parent_ids = GET_NODE_PARENTS_DB(conn, node_id) queries the database using
%   the provided database connection object 'conn' and the node identifier
%   'node_id'. It returns a string array of parent node IDs associated with
%   the specified node. If no parents are found, an empty cell array is returned.
%
%   Inputs:
%       conn    - Database connection object.
%       node_id - ID of the node whose parent IDs are to be retrieved (string or char).
%
%   Outputs:
%       parent_ids - String array of parent node IDs. Returns an empty cell array
%                    if the node has no parents.
%
%   Example:
%       conn = database(...); % Create database connection
%       node_id = '123';
%       parent_ids = get_node_parents_db(conn, node_id);
%
%   See also FETCH, DATABASE
function parent_ids = get_node_parents_db(conn, node_id)
    % Retrieve the parent IDs of a given node from the database.
    % conn: Database connection object
    % node_id: ID of the node whose parents are to be retrieved
    query = sprintf('SELECT parent_id FROM node_relation WHERE child_id = ''%s''', node_id);
    data = fetch(conn, query);
    if isempty(data)
        parent_ids = {};
    else
        parent_ids = data{:,1};
        parent_ids = string(parent_ids);
    end
end