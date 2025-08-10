function parents_info = get_info_parents(node_id)
%GET_INFO_PARENTS Get the parents' information for a given node ID.
%
%   Inputs:
%       node_id - ID of the node whose parents' info is to be retrieved (string or char).
%
%   Outputs:
%       parents_info - Struct array with fields:
%           'node_id'    - Parent node ID (string)
%           'node_tag'   - Parent node tag (string)
%           'folder_path'- Parent node folder path (string)
%           'edge_id'    - Edge ID connecting to current node with the parent node (string)

    % Connect to the database
    conn = connect_to_db();

    % Get parent_id/edge_id pairs as struct array
    parent_edge_struct = get_node_parents_and_edge_ids(conn, node_id);

    % Initialize output
    parents_info = struct('node_id', {}, 'node_tag', {}, 'folder_path', {}, 'edge_id', {});

    % Loop through parent/edge pairs and fetch their information
    for i = 1:numel(parent_edge_struct)
        parent_id = parent_edge_struct(i).parent_id;
        edge_id = parent_edge_struct(i).edge_id;
        % Escape quotes to ensure valid SQL
        parent_id_escaped = replace(string(parent_id), "'", "''");
        query = sprintf("SELECT node_tag, folder_path FROM nodes WHERE node_id = '%s'", parent_id_escaped);
        data = fetch(conn, query);
        if ~isempty(data)
            parents_info(end+1).node_id = parent_id; %#ok<AGROW>
            if istable(data)
                parents_info(end).node_tag = string(data.node_tag(1));
                parents_info(end).folder_path = string(data.folder_path(1));
            else
                parents_info(end).node_tag = string(data{1,1});
                parents_info(end).folder_path = string(data{1,2});
            end
            parents_info(end).edge_id = edge_id;
        end
    end

    % Close the database connection
    close(conn);
end