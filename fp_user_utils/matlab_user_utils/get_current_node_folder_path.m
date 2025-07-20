function folder_path = get_current_node_folder_path()
%GET_FOLDER_PATH_NODE Get the folder path of the current node.
%   FOLDER_PATH = GET_FOLDER_PATH_NODE() retrieves the folder path associated
%   with the current node. The function determines the node ID based on the
%   current working directory, connects to the database using the URL specified
%   in the 'DATABASE_URL' environment variable, and queries the database for
%   the folder path corresponding to the node ID.
%
%   Output:
%       folder_path - String containing the folder path of the current node.
%
%   Example:
%       path = get_current_node_folder_path();
%
%   See also: GET_NODE_ID, CONNECT_TO_DB, GET_NODE_FOLDER_PATH_DB
node_id = get_current_node_id();
if isempty(node_id)
    error('Node ID could not be determined from the current working directory.');
end
db_url = getenv('DATABASE_URL');
conn = connect_to_db(db_url);
folder_path = get_node_folder_path_db(conn, node_id);

end