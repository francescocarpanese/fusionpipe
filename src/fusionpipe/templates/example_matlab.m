clear all
close all
clc

% Add user utils 
addpath(genpath('user_utils/matlab'));

% Set manually the string with the node id or read from the folder name
[~,node_id] = fileparts(fileparts(pwd));

% Get folder paths of all the parents of this node
node_parents = get_all_parent_node_folder_paths(node_id);

% Display the folder path of the parent nodes
disp('Node Parents:');
for i = 1:length(node_parents)
    disp(node_parents{i});
end