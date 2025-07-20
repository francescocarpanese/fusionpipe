
import sys
import os
import types

def test_init_node_kernel_runs_expected_command(monkeypatch, tmp_path):
    # Prepare a dummy get_current_node_id
    dummy_node_id = "dummy_node"
    dummy_dir = tmp_path
    called = {}

    def fake_get_current_node_id():
        return dummy_node_id

    def fake_run(cmd, check, cwd):
        called['cmd'] = cmd
        called['check'] = check
        called['cwd'] = cwd
        return 0

    # Patch sys.modules to inject dummy get_current_node_id
    sys.modules["fp_user_utils.user_api"] = types.SimpleNamespace(get_current_node_id=fake_get_current_node_id)
    monkeypatch.setattr("subprocess.run", fake_run)

    # Patch __file__ to be in dummy_dir
    test_globals = {"__file__": os.path.join(dummy_dir, "init_node_kernel.py")}
    with open(os.path.join(dummy_dir, "init_node_kernel.py"), "w") as f:
        f.write("# dummy")

    # Import and run the script
    import importlib.util
    spec = importlib.util.spec_from_file_location("init_node_kernel", os.path.join(dummy_dir, "init_node_kernel.py"))
    module = importlib.util.module_from_spec(spec)
    # Copy the code from the real script
    with open(os.path.join(os.path.dirname(__file__), "../src/fusionpipe/templates/init_node_kernel.py")) as f:
        code = f.read()
    exec(code, {**test_globals, **module.__dict__})

    # Check that subprocess.run was called with the expected command
    assert called['cmd'][:6] == ["uv", "run", "python", "-m", "ipykernel", "install"]
    assert "--name" in called['cmd']
    assert dummy_node_id in called['cmd']
    assert called['cwd'] == str(dummy_dir)