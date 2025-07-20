clear all
close all
clc

% Add user utils 
addpath(genpath(getenv('USER_UTILS_FOLDER_PATH')));

% Set manually the string with the node id or read from the folder name
node_id = get_current_node_id();

% Get folder paths of all the parents of this node
node_info = get_info_parents(node_id);

% Display the folder path of the parent nodes
disp(node_info);