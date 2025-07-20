function data_folder_path = get_current_node_folder_path_data()
%GET_FOLDER_PATH_DATA Get the data folder path of the current node.
%   DATA_FOLDER_PATH = GET_FOLDER_PATH_DATA() returns the full path to the
%   'data' subfolder within the current node's folder. The function first
%   determines the node folder path using GET_FOLDER_PATH_NODE(). If the
%   node folder path cannot be determined or the 'data' subfolder does not
%   exist, an error is thrown.
%
%   Output:
%       data_folder_path - Full path to the 'data' subfolder (char array)
%
%   Example:
%       dataPath = get_current_node_folder_path_data();
%
%   See also: GET_FOLDER_PATH_NODE, FULLFILE, EXIST

    node_folder_path = get_current_node_folder_path();
    if isempty(node_folder_path)
        error('Node folder path could not be determined.');
    end

    % Assuming the data folder is a subfolder named 'data' within the node folder
    data_folder_path = fullfile(node_folder_path, 'data');

    if ~exist(data_folder_path, 'dir')
        error('Data folder does not exist: %s', data_folder_path);
    end
end