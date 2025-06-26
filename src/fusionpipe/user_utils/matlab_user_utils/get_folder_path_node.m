function folder_path = get_folder_path_node()
%GET_FOLDER_PATH_NODE Get the folder path of the current node.
node_id = get_node_id();
if isempty(node_id)
    error('Node ID could not be determined from the current working directory.');
end
db_url = getenv('DATABASE_URL');
conn = connect_to_db(db_url);
folder_path = get_node_folder_path_db(conn, node_id);

end