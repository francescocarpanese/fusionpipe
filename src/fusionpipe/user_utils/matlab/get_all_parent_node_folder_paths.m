function parent_paths = get_all_parent_node_folder_paths(node_id)
    % Get all parent node folder paths for a given node_id.
    conn = connect_to_db();
    parent_paths = {};
    parents = get_node_parents(conn, node_id);
    for i = 1:numel(parents)
        parent_id = parents(i);
        path = get_node_folder_path(conn, parent_id);
        if ~isempty(path)
            parent_paths{end+1} = path;
        end
    end
end