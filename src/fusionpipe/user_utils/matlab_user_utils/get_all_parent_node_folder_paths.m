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