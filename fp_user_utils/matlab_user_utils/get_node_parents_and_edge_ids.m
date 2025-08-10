function parentEdgeStruct = get_node_parents_and_edge_ids(conn, node_id)
%GET_NODE_PARENTS_AND_EDGE_IDS Retrieve parent node IDs and edge IDs for a given node.
%
%   parentEdgeStruct = GET_NODE_PARENTS_AND_EDGE_IDS(conn, node_id)
%
%   Inputs:
%       conn    - Database connection object (PostgreSQL, from Database Toolbox)
%       node_id - Node ID (string or char)
%
%   Output:
%       parentEdgeStruct - struct array with fields:
%           .parent_id
%           .edge_id

    % Build SQL query (avoid passing node_id as a third arg to fetch, which is
    % treated as rowlimit/options in many MATLAB releases)
    node_id_str = string(node_id);
    % Escape single quotes to avoid SQL errors/injection
    node_id_escaped = replace(node_id_str, "'", "''");
    sqlquery = sprintf("SELECT parent_id, edge_id FROM node_relation WHERE child_id = '%s'", node_id_escaped);

    % Execute query
    data = fetch(conn, sqlquery);

    % Initialize output
    parentEdgeStruct = struct('parent_id', {}, 'edge_id', {});

    if isempty(data)
        return;
    end

    % Fill the struct array (support table or cell output from fetch)
    if istable(data)
        for i = 1:height(data)
            parentEdgeStruct(i).parent_id = string(data.parent_id(i)); %#ok<AGROW>
            parentEdgeStruct(i).edge_id   = string(data.edge_id(i));   %#ok<AGROW>
        end
    else
        % Expect a cell array with two columns: {parent_id, edge_id}
        for i = 1:size(data, 1)
            parentEdgeStruct(i).parent_id = string(data{i, 1}); %#ok<AGROW>
            parentEdgeStruct(i).edge_id   = string(data{i, 2}); %#ok<AGROW>
        end
    end
end