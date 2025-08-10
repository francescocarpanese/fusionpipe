function folder_path = get_parent_folder_path_from_edge_id(edge_id)
% Return the folder path of the parent connected with the current node by the edge with edge_id.

    parent_info = get_parent_info_from_edge_id(edge_id);
    if ~isempty(parent_info)
        folder_path = parent_info.folder_path;
    else
        folder_path = [];
    end
end
