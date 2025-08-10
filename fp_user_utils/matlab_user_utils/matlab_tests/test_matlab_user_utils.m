function tests = test_matlab_user_utils
	tests = functiontests(localfunctions);
end

function test_get_current_node_id_returns_empty_when_no_file(testCase)
	% Save current directory
	originalDir = pwd;
	tempDir = tempname;
	mkdir(tempDir);
	cd(tempDir);
	cleanup = onCleanup(@() cd(originalDir)); %#ok<NASGU>
	node_id = get_current_node_id();
	verifyEmpty(testCase, node_id);
end

function test_get_current_node_id_reads_file(testCase)
	originalDir = pwd;
	tempDir = tempname;
	mkdir(tempDir);
	cd(tempDir);
	cleanup = onCleanup(@() cd(originalDir)); %#ok<NASGU>
	fid = fopen('.node_id', 'w');
	test_id = 'n_20230810123456_0001';
	fwrite(fid, test_id);
	fclose(fid);
	node_id = get_current_node_id();
	verifyEqual(testCase, node_id, test_id);
end
