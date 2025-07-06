# Best Practices: Developing a Reusable Pipeline Package

When building a new pipeline, you'll often want to develop an associated Python package. This allows you to version control your code, share it between pipeline nodes, and reuse it in future projects. This guide outlines the recommended strategy for creating and integrating a reusable package into your pipeline workflow.

## Part 1: Creating Your Python Package

First, set up a new Python package. We recommend using `uv` for initialization.

### 1. Initialize the Package
Run the following command to create a new package:
```bash
uv init --package <mypackage>
```

This command scaffolds a standard Python project structure for you:

```
<mypackage>/
├── src/
│   └── <mypackage>/
│       ├── __init__.py
│       └── your_function.py
└── pyproject.toml
```

- **`pyproject.toml`**: A configuration file where you can list your project's dependencies.
- **`src/<mypackage>`**: The directory where your package's source code lives.

### 2. Set Up Version Control with Git
The `uv init` command also initializes a Git repository. To publish your package, you'll need to create a remote repository on a platform like GitHub or GitLab.

1.  **Create an empty repository** on GitHub or GitLab. Name it `<mypackage>`.

2.  **Navigate into your package directory:**
    ```bash
    cd <mypackage>
    ```

3.  **Link your local repository to the remote one:**
    ```bash
    git remote add origin git@github.com:<your-username>/<mypackage>.git
    ```

4.  **Push your initial code to the remote repository:**
    ```bash
    git push -u origin main
    ```

Your package is now set up for version-controlled development.

!!! tip "Switching Default Branch from `master` to `main`"
    If your remote repository uses `master` as the default branch, you can rename it to `main` for consistency. Run the following commands:

    ```bash
    git branch -m master main
    git push -u origin main
    git symbolic-ref refs/remotes/origin/HEAD refs/remotes/origin/main
    ```

    Then, update the default branch in your repository settings on GitHub or GitLab to `main`, and delete the old `master` branch if desired:

    ```bash
    git push origin --delete master
    ```

## Part 2: Integrating the Package into a Pipeline Node

With your package created, you can now integrate it into a pipeline node.

### 1. Prepare the Node
First, create a new pipeline and a node within it. Then, navigate to the node's code directory:

```bash
cd <node_id>/code
```

### 2. Clone Your Package
Clone your newly created package into this directory:

```bash
git clone <package_url>
```

The directory structure inside your node's `code` folder should now look like this:

```
<node_id>/
├── code/
│   ├── <mypackage>/      # Your cloned package
│   │   ├── src/
│   │   └── pyproject.toml
│   ├── main.py         # The node's main script
│   └── pyproject.toml    # The node's environment
├── data/
└── reports/
```

### 3. Install the Package
To make your package's functions available to the node's `main.py` script, install it in editable mode using `uv`:

```bash
uv pip install --editable <mypackage>
```

!!! info "What is Editable Mode?"
    The `--editable` flag creates a symbolic link from your node's Python environment to your package's source code. This means any changes you make to the package's code are immediately available to the node without needing to reinstall it.

## Part 3: Developing and Using Your Code

Now you can develop your package's functionality and use it within the pipeline.

### 1. Add Code to Your Package
Write reusable functions inside your package's source directory (`<mypackage>/src/<mypackage>`). For example, create a file named `dataset.py` with a function to process data from parent nodes:

```python
# In <mypackage>/src/<mypackage>/dataset.py
def print_parents(parent_folders):
    for folder in parent_folders:
        print(f"Found parent folder: {folder}")
```

### 2. Use Your Package in `main.py`
Import and call your package's functions from the node's `main.py` script.

```python
# In <node_id>/code/main.py

# Import the user API to interact with the pipeline
from python_user_utils.node_api import get_all_parent_node_folder_paths, get_node_id

# Import the function from your package
from mypackage.dataset import print_parents

if __name__ == "__main__":
    node_id = get_node_id()

    # Get the paths of all parent nodes
    parent_folders = get_all_parent_node_folder_paths(node_id=node_id)

    # Use the function from your package to process them
    print_parents(parent_folders)
```

### 3. Commit and Push Your Changes
After developing a new feature, commit and push the changes to your package's repository:

```bash
# Navigate to your package directory
cd <mypackage>

# Stage, commit, and push your changes
git add src/
git commit -m "feat: Add print_parents function"
git push
```

## Part 4: Testing Your Node

Before running the full pipeline, test your node's script in isolation.

1.  **Navigate to the node's code directory:**
    ```bash
    cd <node_id>/code
    ```

2.  **Run the main script:**
    ```bash
    uv run python main.py
    ```

!!! tip "Implement a `preview` Mode for Faster Development"
    When working with large datasets, consider adding a `preview` flag or option to your functions. This allows you to run them on a small subset of data for quick testing and iteration, saving you time.

Once you confirm the script runs correctly, you can execute the entire pipeline from the UI. For more details, see the [Develop a Node guide](user_guide/develop_node.md).

## Part 5: Reusing Your Package in Other Nodes

A key benefit of this approach is reproducibility. Each node has a distinct snapshot of your package. When creating new nodes that depend on your package, you have two main options:

#### a) Duplicate an Existing Node
Duplicate a node that already contains your package. This is the simplest method. After duplicating, navigate to the package directory and pull the latest changes to ensure it's up to date.

```bash
cd <new_node_id>/code/<mypackage>
git pull
```

#### b) Clone the Package into a New Node
Alternatively, clone the package into the new node's `code` directory and install it in editable mode, following the same steps outlined in Part 2.

```bash
cd <new_node_id>/code
git clone <package_url>
uv pip install --editable <mypackage>
```

By keeping a local copy of the package in each node, you ensure that your pipeline remains reproducible, even as your package evolves over time. For a deeper comparison of different dependency management strategies, see [External Python Dependencies](package_management_python.md).

