%GET_NODE_FOLDER_PATH_DB Retrieve the folder path for a given node from the database.
%   folderPath = GET_NODE_FOLDER_PATH_DB(conn, node_id) queries the database
%   connection 'conn' for the folder path associated with the specified
%   'node_id' in the 'nodes' table. If the node is found, the function
%   returns the folder path as a string. If the node is not found, it
%   returns an empty array.
%
%   Inputs:
%       conn    - Database connection object.
%       node_id - Identifier of the node (as a string or character array).
%
%   Output:
%       folderPath - Folder path corresponding to the node_id, or empty if
%                    the node_id does not exist in the database.
%
%   Example:
%       conn = database(...); % Create database connection
%       path = get_node_folder_path_db(conn, 'node123');
%
%   See also: FETCH, TABLE2CELL
function folderPath = get_node_folder_path_db(conn, node_id)
    % Execute the SQL query to fetch the folder_path for the given node_id
    query = sprintf('SELECT folder_path FROM nodes WHERE node_id = ''%s''', node_id);
    data = fetch(conn, query);
    if isempty(data)
        folderPath = [];
    else
        folderPath = table2cell(data(1,1));
        folderPath = folderPath{1};
    end
end