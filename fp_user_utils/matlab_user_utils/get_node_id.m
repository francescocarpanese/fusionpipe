%GET_NODE_ID Get the node id by searching for a '.node_id' file.
%   node_id = GET_NODE_ID() searches for a '.node_id' file in the current
%   directory or its parent directories. If such a file is found, the function
%   returns the node id string from the file content. If no file is found or
%   the file is empty, it returns an empty array.
%
%   Example:
%       % Suppose there's a '.node_id' file in the current directory or a parent
%       % directory containing 'n_20230615123456_0001'
%       node_id = get_current_node_id()
%       % node_id will be 'n_20230615123456_0001'
%
%   Output:
%       node_id - String containing the node id if found, otherwise empty.
%
%   See also: PWD, FILEREAD, EXIST
function node_id = get_current_node_id()
%GET_NODE_ID Get the node id by searching for a '.node_id' file in the current
% directory or its parent directories.

current_dir = pwd;

while true
    node_id_file = fullfile(current_dir, '.node_id');
    
    if exist(node_id_file, 'file')
        try
            file_content = fileread(node_id_file);
            file_content = strtrim(file_content);
            if ~isempty(file_content)
                node_id = file_content;
                return;
            end
        catch
            % If file can't be read, continue searching
        end
    end
    
    parent_dir = fileparts(current_dir);
    if strcmp(parent_dir, current_dir)
        % Reached root directory
        break;
    end
    current_dir = parent_dir;
end

node_id = [];
end