In the following the guideline the instructions to include external python code in yout pipeline is presented in details.

## Preliminaries
### Difference between python script and python packages
A **Python script** is a single file containing Python code, typically used for small tasks, automation, or quick experiments. Scripts are usually not structured for reuse and are executed directly.

A **Python package** is a collection of modules organized in directories with an `__init__.py` file. Packages are designed for code reuse, distribution, and maintainability, making it easier to share and manage complex codebases across multiple projects. Most of the libraries you install from PyPI (using `pip install ...`) are Python packages, and you typically import them using statements like `from package_name import ...`.

It is highly recommended to package your code into a Python package. Follow the guide in [Best Practices for Pipeline Packages](best_practices_pipeline_package.md). However, in the following instructions are provided for both cases where you python code is a script or python package

### Virtual environment for a node
Every node is initialise with the command `uv init`, which automatically create a dedicated python virtual environment which will contains the dependencies for the node. This can be activate from the 'node_id/code' subfolder

```bash
cd code
source .venv/bic/activate
```
See guideline at page [Delevelop a Node](user_guide/develop_node.md)

## Extarnal code is a python packaged
There can be 2 situations in this case. 

- `Developer`: Your are modifying the package while you develop the pipeline.
- `User`: The package remains the same during the development of the full pipeline.

??? note "Developer"
    In this case the code in the python package evolves while you are developing the pipeline. This is the most common case when you are working on your personal project. Usually your code, is version controller `.git` independently from your pipeline.

    There are two possibile solutions that you can adopt to integrate your code in one node of your pipeline.
    ??? note "a) Every node has an independent snapshot of the external package"
        In this case in every node a snapshot of your code is available. 
        This means that you either copy paste your code from your local location, or you clone the repository from remote `.git`. 

        ```bash
        git clone ./<url/mypackageo>
        ```

        You can then add your repo to the `.venv` of the node with 
        ```bash
        uv add --editable <mypackage>
        ```
        You will be able then to import functions from your package into the `main.py` function as usual `from mypackage import *`.

        The editable flag ensures that changes you make to your external package source code are immediately reflected in the node’s environment without needing to reinstall the package each time. This way you can import a function on the `main.py` from your package, and modification of your package will be immediately available. 

        This step will also automatically install all the dependencies of the external python package in the `.venv` of your node.


        !!! warning 
            Whenever you create a new node, you will need to follow these steps for all the external packages that are needed in your node.
            When you duplicate a node instead, the new node will be ready ready to be used with all the dependencies installed.

        !!! info "Info: Best practise to update the external packages in previously created nodes"
            Suppose that you started developing your pipeline, and your external packages evolves changes while you develop the pipeline. A node that was created and run in the past will have a snapshot of the the external package as on the moment that the node has run. It could happen that you would like to updated the package for an old node, and eventually re-run the node and its children if the changes are expected to affect the output of the node.

            In case you are sure that the changes of your code will not change the outputs of the nodes:
            - Navigate the node folder, `cd node_folder_path`.
            - Navigate your package subfolder.
            - Merge the latest version of your package `git pull`.

            If case the changes of your package can affect the output of the node: 
            - Duplicate the node (without data)
            - Attach the nodes to the same parents as for the original one.
            - Navigate the node folder, `cd node_folder_path`.
            - Merge the latest version of your package `git pull`.
            - Rerun the node.
            - If completed succesfully, swap the node with the one containing the new package in the pipeline.


        !!! Tip "Tip: Create a template node with all your external packages and duplicate it"
            When multiple external packages are needed in the same node, in order to avoid to `git pull` all of them for every node, you can create a node that you will use as a template. In this node you can pull and add all the dependencies. Then, you can duplicate this node instead of creating a new one and add depencies.

        !!! info
            `uv` will not re-download, or duplicate in the disk dependencies if those are already available in the `uv` cache. This is convenient especially for large (>1Gib) packages like `pytorch`,`tensorflow` to avoid wasting disk space.


    ??? note "b) Every node share the same instance of the external package"
        In this case your package is located in 1 single location on your disk. Every node will reference to this package.

        !!! Tip 
            It is recommended to create a node in your pipeline which will only contain the package that will be shared with the other nodes.
            This will allow all the users that are collaborating on your pipeline to have access to the code. Also, this will allow you to have your pipeline be self-contained.

        When you creaet a new node:

        - Navigate the code folder `code_id/node`
        - Add the package using the absolute path to the location on this where the package is located
        ```bash
        uv add --editable <absolute/path/to/package>
        ```

        When you duplicate a node where you referenced an external package, this will be ready to use. No need to re-add the package.
        There are pros and cons with both solutions:

    There are pros and cons with both solutions:

    - **a) Every node has an independent snapshot of the external package**
        - **PROS:**
            - Each node is self-contained and portable.
            - Changes to the package in one node do not affect others.
            - Easier to reproduce results for a specific node at a given point in time.
        - **CONS:**
            - Requires manual updates of the package in each node if changes are made.
            - Can lead to code duplication.
            - More maintenance overhead when managing multiple nodes.

    - **b) Every node shares the same instance of the external package**
        - **PROS:**
            - Centralized management of the package; updates are immediately available to all nodes.
            - Reduces code duplication and saves disk space.
            - Easier to maintain consistency across nodes.
        - **CONS:**
            - Changes to the package can affect all nodes, potentially breaking reproducibility.

    
    !!! tip
        It is recommended to use option **a)** while developing your pipeline. Pipelines often evolve over time, becoming complex as you iterate on your code, and many nodes may require significant compute resources to process full datasets. The extra effort to update packages in old nodes with solution **a)** is usually outweighed by the benefit of avoiding hard-to-debug issues caused by breaking changes and loss of reproducibility that can occur with solution **b)**.
    


??? note "User"
    In this case, the external package does not change while you are developing the pipeline. This is typical when the Python package comes from a stable distribution such as PyPI (e.g., `numpy`) or a remote repository (e.g., GitHub or GitLab).

    To add such a package to your node:

    - Navigate to the node's code folder: `cd node_id/code`
    - Add the package using:
      ```bash
      uv add <package-location>
      ```

    ??? note "Package hosted in PyPI"
        Simply specify the package name. For example:
        ```bash
        uv add numpy
        ```
        To specify a particular version of a package from PyPI, append `==<version>` to the package name. For example, to install version 1.24.0 of numpy:

        ```bash
        uv add numpy==1.24.0
        ```

        This ensures that the specified version is installed in your node's environment.

    ??? note "Package hosted on GitHub/Gitlab or another remote repository"
        You can provide the repository URL directly. For example:
        ```bash
        uv add git+https://github.com/username/repository.git
        ```
        This will install the package from the remote source.

        !!! tip
            For better reproducibility and to ensure all nodes use the same version of the package, it is recommended to:

            1. Create a dedicated node to download and store the package locally.
            2. Download or clone the package into this node.
            3. Add the package to other nodes by providing the absolute path to the local copy. For example:
                ```bash
                uv add --editable /absolute/path/to/package
                ```
            This ensures all nodes reference the same package version, avoiding inconsistencies due to remote updates or changes.

## External code is (only) a collection of Python scripts

!!! tip
    For maximum reproducibility and maintainability, it is best to organize your scripts as a Python package rather than as standalone scripts. Packaging your code ensures consistent imports, easier dependency management, and better compatibility with pipeline tools. See [Best Practices for Pipeline Packages](best_practices_pipeline_package.md) for guidance on structuring your code as a package.
   

There are two main scenarios when including standalone Python scripts (not organized as a package) in your pipeline:

??? note "Developer"
    If you are actively developing or modifying your scripts while building your pipeline, consider the following approaches:

    ??? note "a) Copy scripts into each node"
        - Place your script(s) directly in the `code` subfolder of the node.
        - Import functions or classes from these scripts in your `main.py` using standard Python import statements:
            ```python
            from my_script import my_function
            ```
        - **PROS:** Each node is self-contained and reproducible. Changes to scripts in one node do not affect others.
        - **CONS:** Manual updates are needed if scripts change, and code duplication may occur across nodes.

        !!! tip
            If you update your script and want to propagate changes to other nodes, you will need to manually copy the updated script into each relevant node.

    ??? note "b) Reference scripts from a shared location"
        - Store your scripts in a shared directory outside the node folders.
        - In your `main.py`, add the path to this directory to `sys.path` at runtime:
            ```python
            import sys
            sys.path.append('/absolute/path/to/shared/scripts')
            from my_script import my_function
            ```
        - **PROS:** Centralized management; updates are immediately available to all nodes.
        - **CONS:** Changes to the script affect all nodes, which may impact reproducibility.

        !!! warning
            Be cautious: updating a shared script will affect all nodes that reference it, potentially breaking previous results.

    ??? note "c) Use symbolic links (advanced)"
        - Create a symbolic link in the node's `code` folder pointing to the shared script location:
            ```bash
            ln -s /absolute/path/to/shared/scripts/my_script.py code/my_script.py
            ```
        - **PROS:** Keeps node folders organized and avoids duplication.
        - **CONS:** Like referencing a shared location, changes to the script affect all nodes using the link.

    !!! tip
        For most development workflows, copying scripts into each node is recommended for reproducibility. Use shared references or symbolic links if you prioritize easier maintenance and are aware of the implications for reproducibility.

??? note "User"
    If your scripts are stable and will not change during pipeline development (for example, they are provided by a third party or are finalized):

    - It is recommended to create a dedicated node in your pipeline to store the shared scripts in a read-only location.
    - In each node that requires these scripts, add the shared scripts directory to `sys.path` in your `main.py`:
        ```python
        import sys
        sys.path.append('/absolute/path/to/shared/scripts')
        from my_script import my_function
        ```
    - Alternatively, you may create symbolic links in the node's `code` folder pointing to the shared scripts.

    This approach ensures all nodes reference the same, unchanging version of the script, supporting reproducibility and easier maintenance.

---
If your scripts have dependencies, install them in the node's virtual environment using `uv add <package>` as described in the previous sections.

Alternatively, if your dependencies are listed in a `requirements.txt` file, you can install all of them at once by running:

```bash
uv pip install -r requirements.txt
```

This will install every package specified in the `requirements.txt` file into the node's virtual environment.
