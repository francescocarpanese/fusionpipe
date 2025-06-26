function reports_folder_path = get_folder_path_reports()
%GET_FOLDER_PATH_REPORTS Get the reports folder path of the current node.

    node_folder_path = get_folder_path_node();
    if isempty(node_folder_path)
        error('Node folder path could not be determined.');
    end

    % Assuming the reports folder is a subfolder named 'reports' within the node folder
    reports_folder_path = fullfile(node_folder_path, 'reports');

    if ~exist(reports_folder_path, 'dir')
        error('Reports folder does not exist: %s', reports_folder_path);
    end
end