function code_folder_path = get_folder_path_code()
%GET_FOLDER_PATH_CODE Get the code folder path of the current node.

    node_folder_path = get_folder_path_node();
    if isempty(node_folder_path)
        error('Node folder path could not be determined.');
    end

    % Assuming the code folder is a subfolder named 'code' within the node folder
    code_folder_path = fullfile(node_folder_path, 'code');

    if ~exist(code_folder_path, 'dir')
        error('Code folder does not exist: %s', code_folder_path);
    end
end