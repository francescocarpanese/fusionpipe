%GET_NODE_ID Get the node id from the current working directory path.
%   node_id = GET_NODE_ID() searches the current working directory (CWD) path
%   for a folder name matching the pattern 'n_<14 digits>_<4 digits>'.
%   If such a folder is found in the path, the function returns the matched
%   node id string. If no match is found, it returns an empty array.
%
%   Example:
%       % Suppose the current working directory is:
%       % '/data/project/n_20230615123456_0001/session'
%       node_id = get_node_id()
%       % node_id will be 'n_20230615123456_0001'
%
%   Output:
%       node_id - String containing the node id if found, otherwise empty.
%
%   See also: PWD, REGEXP
function node_id = get_node_id()
%GET_NODE_ID Get the node id by searching the current working directory path
% for a folder name matching the pattern: n_<14 digits>_<4 digits>.

cwd = pwd;
% Pattern: n_ followed by 14 digits, underscore, 4 digits
expr = 'n_\d{14}_\d{4}';
tokens = regexp(cwd, expr, 'match');

if ~isempty(tokens)
    node_id = tokens{1};
else
    node_id = [];
end

end