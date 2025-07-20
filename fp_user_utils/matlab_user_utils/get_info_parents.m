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
    %
    %   Example:
    %       node_id = '123';
    %       parents_info = get_info_parents(node_id);

    % Connect to the database
    conn = connect_to_db();
    
    % Initialize output
    parents_info = struct('node_id', {}, 'node_tag', {}, 'folder_path', {});
    
    % Get parent node IDs
    parent_ids = get_node_parents_db(conn, node_id);
    
    % Loop through parent IDs and fetch their information
    for i = 1:numel(parent_ids)
        parent_id = parent_ids(i);
        query = 'SELECT node_tag, folder_path FROM nodes WHERE node_id = ?';
        data = fetch(conn, query, parent_id);
        
        if ~isempty(data)
            parents_info(end+1).node_id = string(parent_id); %#ok<AGROW>
            parents_info(end).node_tag = string(data{1,1});
            parents_info(end).folder_path = string(data{1,2});
        end
    end
    
    % Close the database connection
    close(conn);
end