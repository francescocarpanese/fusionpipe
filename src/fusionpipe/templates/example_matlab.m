clear all
close all
clc

% Add user utils 
addpath(genpath(getenv('USER_UTILS_FOLDER_PATH')));

% Set manually the string with the node id or read from the folder name
node_id = get_current_node_id();

% Get folder paths of all the parents of this node
node_parents = get_all_parent_node_folder_paths(node_id);

% Display the folder path of the parent nodes
disp('Node Parents:');
for i = 1:length(node_parents)
    disp(node_parents{i});
end