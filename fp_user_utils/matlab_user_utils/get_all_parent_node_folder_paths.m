%GET_ALL_PARENT_NODE_FOLDER_PATHS Retrieve all parent node folder paths for a given node ID.
%   PARENT_PATHS = GET_ALL_PARENT_NODE_FOLDER_PATHS(NODE_ID) connects to the database,
%   retrieves all parent node IDs for the specified NODE_ID, and returns a string array
%   containing the folder paths of each parent node. If a parent node does not have a
%   folder path, its entry in the output will remain an empty string.
%
%   Input:
%       NODE_ID - Identifier of the node whose parent folder paths are to be retrieved.
%
%   Output:
%       PARENT_PATHS - String array containing the folder paths of all parent nodes.
%
%   Example:
%       paths = get_all_parent_node_folder_paths(42);
%
%   See also CONNECT_TO_DB, GET_NODE_PARENTS_DB, GET_NODE_FOLDER_PATH_DB
function parent_paths = get_all_parent_node_folder_paths(node_id)
    % Get all parent node folder paths for a given node_id.
    conn = connect_to_db();
    parents = get_node_parents_db(conn, node_id);
    parent_paths = strings(1, numel(parents)); % Initialize with empty strings
    for ii = 1:numel(parents)
        parent_id = parents(ii);
        path = get_node_folder_path_db(conn, parent_id);
        if ~isempty(path)
            parent_paths(ii) = path;
        end
    end
end