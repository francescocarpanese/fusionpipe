function code_folder_path = get_folder_path_code()
%GET_FOLDER_PATH_CODE Get the code folder path of the current node.
%   code_folder_path = GET_FOLDER_PATH_CODE() returns the full path to the
%   'code' subfolder within the current node's folder. The node folder path
%   is determined by the helper function GET_FOLDER_PATH_NODE(). If the node
%   folder path cannot be determined or the 'code' subfolder does not exist,
%   an error is thrown.
%
%   Output:
%       code_folder_path - Full path to the 'code' subfolder (char array)
%
%   Example:
%       codePath = get_folder_path_code();
%
%   See also: get_folder_path_node

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