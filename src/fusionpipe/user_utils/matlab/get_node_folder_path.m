function folderPath = get_node_folder_path(conn, node_id)
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