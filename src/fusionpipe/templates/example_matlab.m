clear all
close all
clc

% Add user utils 
addpath(genpath('user_utils/matlab'));

% Set manually the string with the node id or read from the folder name
node_id = get_node_id();

% Get folder paths of all the parents of this node
node_parents = get_all_parent_node_folder_paths(node_id);

% Display the folder path of the parent nodes
disp('Node Parents:');
for i = 1:length(node_parents)
    disp(node_parents{i});
end

% LOAD DATA FROM DEFUSE 
clear all
close all
clc

% Add user utils 
addpath(genpath('user_utils/matlab'));

old_path_code = pwd;
old_path_node = fileparts(pwd);
cd '/home/fbertini/Documents/defuse/'
setup_DEFUSE_paths
addpath(genpath('../mds')) % For now (shouldn't be necessary on defuse01 server)
HH = DATA('TCV');
amp = HH.get_data('Ampl_OddN', 79236);
freq = HH.get_data('freq_OddN', 79236);
amp_freq_data.N11_amplitude = amp;
amp_freq_data.N11_frequency = freq;
json_write(amp_freq_data, [old_path_node, '/data/amp_freq_data.json'])
cd(old_path_code)


