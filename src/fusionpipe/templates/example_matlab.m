clear all
close all
clc

addpath(genpath('user_utils/matlab'));

[~,node_id] = fileparts(fileparts(pwd));

conn = connect_to_db();

get_node_folder_path(conn, node_id)

get_node_parents(conn, node_id)

node_parents = get_all_parent_node_folder_paths(node_id);

disp('Node Parents:');
for i = 1:length(node_parents)
    disp(node_parents{i});
end