function node_id = get_node_id()
%GET_NODE_ID Get the node id, which is the name of the folder two levels up from this file.

current_file = mfilename('fullpath');
two_levels_up = fileparts(fileparts(fileparts(fileparts(current_file))));
node_id = get_basename(two_levels_up);

end

function name = get_basename(pathstr)
% Helper to get the last part of a path (basename)
[~, name, ext] = fileparts(pathstr);
name = [name ext];
end