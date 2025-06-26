% Simple example of calling DEFUSE from matlab script within a given node
clear all
close all
clc

% Add user utils for nodes
addpath(genpath(getenv('USER_UTILS_FOLDER_PATH')));

% Get node folder paths
node_id = get_node_id(); 
node_folder_code = get_folder_path_code(); % Get folder path of the node
node_folder_data = get_folder_path_data(); % Get folder path of the node data

% Navigate into the folder containing DEFUSE
path_defuse_folder = '/home/fbertini/Documents/defuse/'; % Change this to your DEFUSE folder path
cd(path_defuse_folder)
setup_DEFUSE_paths
addpath(genpath('../mds')) % For now (shouldn't be necessary on defuse01 server)

% Fetch amplitude and frequency of the mode N=11
shotnum = 79236;
HH = DATA('TCV');
amp = HH.get_data('Ampl_OddN', shotnum);
freq = HH.get_data('freq_OddN', shotnum);
amp_freq_data.N11_amplitude = amp;
amp_freq_data.N11_frequency = freq;

% Write json file in data folder
json_write(amp_freq_data, char(fullfile(node_folder_data, 'amp_freq_data.json')));
cd(node_folder_code)
