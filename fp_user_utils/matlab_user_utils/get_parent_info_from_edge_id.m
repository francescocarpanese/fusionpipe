function parent_info = get_parent_info_from_edge_id(edge_id)
% Return a struct containing the info of the parent connected with the current node by the edge with edge_id.
% The struct contains the following fields:
%   node_id, node_tag, folder_path, edge_id

    node_id = get_current_node_id();
    info_parents = get_info_parents(node_id);
    parent_info = [];
    for i = 1:length(info_parents)
        if isequal(info_parents(i).edge_id, edge_id)
            parent_info = info_parents(i);
            return;
        end
    end
end
