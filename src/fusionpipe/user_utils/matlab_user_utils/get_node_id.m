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