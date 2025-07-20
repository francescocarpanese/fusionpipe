function reports_folder_path = get_current_node_folder_path_reports()
%GET_FOLDER_PATH_REPORTS Get the reports folder path of the current node.
%   reports_folder_path = GET_FOLDER_PATH_REPORTS() returns the full path to the
%   'reports' subfolder within the current node's folder. The function first
%   determines the node folder path using get_current_node_folder_path(). If the node
%   folder path cannot be determined or the 'reports' folder does not exist,
%   an error is thrown.
%
%   Output:
%       reports_folder_path - Full path to the 'reports' folder (char array)
%
%   Example:
%       path = get_current_node_folder_path_reports();
%
%   See also: get_current_node_folder_path, fullfile, exist

    node_folder_path = get_current_node_folder_path();
    if isempty(node_folder_path)
        error('Node folder path could not be determined.');
    end

    % Assuming the reports folder is a subfolder named 'reports' within the node folder
    reports_folder_path = fullfile(node_folder_path, 'reports');

    if ~exist(reports_folder_path, 'dir')
        error('Reports folder does not exist: %s', reports_folder_path);
    end
end