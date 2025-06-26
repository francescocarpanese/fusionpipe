function data_folder_path = get_folder_path_data()
%GET_FOLDER_PATH_DATA Get the data folder path of the current node.

    node_folder_path = get_folder_path_node();
    if isempty(node_folder_path)
        error('Node folder path could not be determined.');
    end

    % Assuming the data folder is a subfolder named 'data' within the node folder
    data_folder_path = fullfile(node_folder_path, 'data');

    if ~exist(data_folder_path, 'dir')
        error('Data folder does not exist: %s', data_folder_path);
    end
end