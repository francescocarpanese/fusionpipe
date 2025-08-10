clear all
close all
clc

% Add user utils 
addpath(genpath(getenv('USER_UTILS_FOLDER_PATH')));

% Get the id of the current node here this script is being run
node_id = get_current_node_id();

% Get info of all node parents. Read the documentation get_info_parents.
info_parents = get_info_parents(node_id);
disp("Retrieved information of the node parents:");
disp(info_parents);


% In order to retrive information from a given parent of the current node it is recommend to refer to the parent based on the edge_id
% Get information of a parent node from edge_id. The resulting dictionary will contain:
% {
%     'node_id': <parent_node_id>,
%     'node_tag': <parent_node_tag>,
%     'folder_path': <parent_folder_path>,
%     'edge_id': <edge_id>
% }
edge_id = '01';
parent_info = get_parent_info_from_edge_id(edge_id);
disp("Retrieved information of the parent node from edge_id:");
disp(parent_info);

% You can get directly the folder path of the parent node from the edge_id
edge_id = '01'
parent_folder = get_parent_folder_path_from_edge_id(edge_id);
disp("Retrieved folder path of the parent node from edge_id:");
disp(parent_folder);

% Get the folder path of the current node
data_folder_path = get_current_node_folder_path();
disp("Retrieved folder path of the current node:");
disp(data_folder_path);

% Get the folder path of the data for the current node
data_folder_path = get_current_node_folder_path_data();
disp("Retrieved folder path of the data for the current node:");
disp(data_folder_path);

% Save dummy output file in the data folder of the current node
% This is an example on how you should save output data of your node.
output_file_path = fullfile(data_folder_path, 'dummy_output.txt');
fid = fopen(output_file_path, 'w');
fprintf(fid, 'This is a dummy output file for the current node.\n');
fclose(fid);
disp("Dummy output file saved:");
disp(output_file_path);
