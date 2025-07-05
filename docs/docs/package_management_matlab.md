In the following the guideline the instructions to include external MATLAB code in your pipeline is presented in details.

## Preliminaries
### Difference between MATLAB script and MATLAB packages/toolboxes
A **MATLAB script** is a single `.m` file containing MATLAB code, typically used for small tasks, automation, or quick experiments. Scripts are usually not structured for reuse and are executed directly.

A **MATLAB package** or **toolbox** is a collection of functions and classes organized in directories with a `+` prefix (for packages) or as a structured toolbox. Packages are designed for code reuse, distribution, and maintainability, making it easier to share and manage complex codebases across multiple projects. Most of the toolboxes you install from MATLAB File Exchange or MathWorks are MATLAB packages/toolboxes, and you typically call them using statements like `packagename.functionname()` or directly if they're on the MATLAB path.


## Manage dependencies
There can be 2 situations in this case:

- `Developer`: You are modifying the package/toolbox while you develop the pipeline.
- `User`: The package/toolbox remains the same during the development of the full pipeline.

??? note "Developer"
    In this case the code in the MATLAB package/toolbox evolves while you are developing the pipeline. This is the most common case when you are working on your personal project. Usually your code is version controlled with `.git` independently from your pipeline.

    There are two possible solutions that you can adopt to integrate your code in one node of your pipeline.
    
    ??? note "a) Every node has an independent snapshot of the external package"
        In this case in every node a snapshot of your code is available. 
        This means that you either copy paste your code from your local location, or you clone the repository from remote `.git`.

        ```bash
        git clone ./<url/mypackage>
        ```

        You can then add your package to the MATLAB path of the node by:
        
        1. Placing the package in the node's `code` directory
        2. Adding the package directory to MATLAB's path in your main MATLAB script:
        ```matlab
        addpath(genpath('./mypackage'));
        ```
        
        You will be able then to call functions from your package in your MATLAB code as usual:
        ```matlab
        % For regular functions
        result = myfunction(input);
        
        % For package functions
        result = mypackage.myfunction(input);
        ```

        This approach ensures that changes you make to your external package source code are immediately available when MATLAB is executed in the node.

        !!! warning 
            Whenever you create a new node, you will need to follow these steps for all the external packages that are needed in your node.
            When you duplicate a node instead, the new node will be ready to be used.

        !!! info "Info: Best practice to update the external packages in previously created nodes"
            Suppose you have already started developing your pipeline, and your external package continues to evolve during development. Each node you create and run will contain a snapshot of the external package as it existed at the time the node was executed. If you later want to update the package for an existing (older) node—especially if changes to the package are expected to affect the node’s output—you may need to update the package in that node and re-run it, along with any downstream (child) nodes.

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
            - If completed successfully, swap the node with the one containing the new package in the pipeline.

        !!! Tip "Tip: Create a template node with all your external packages and duplicate it"
            When multiple external packages are needed in the same node, in order to avoid to `git pull` all of them for every node, you can create a node that you will use as a template. In this node you can pull and add all the dependencies. Then, you can duplicate this node instead of creating a new one and add dependencies.


    ??? note "b) Every node shares the same instance of the external package"
        In this case your package is located in 1 single location on your disk. Every node will reference to this package.

        !!! Tip 
            It is recommended to create a node in your pipeline which will only contain the package that will be shared with the other nodes.
            This will allow all the users that are collaborating on your pipeline to have access to the code. Also, this will allow you to have your pipeline be self-contained.

        When you create a new node:

        - Navigate the code folder `node_id/code`
        - Add the package path to your MATLAB script using the absolute path to the location where the package is located:
        ```matlab
        addpath(genpath('/absolute/path/to/package'));
        ```

        When you duplicate a node where you referenced an external package, this will be ready to use. No need to re-add the package path.

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
            - Path conflicts might occur if different nodes require different versions.

    !!! tip
        It is recommended to use option **a)** while developing your pipeline. Pipelines often evolve over time, becoming complex as you iterate on your code, and many nodes may require significant compute resources to process full datasets. The extra effort to update packages in old nodes with solution **a)** is usually outweighed by the benefit of avoiding hard-to-debug issues caused by breaking changes and loss of reproducibility that can occur with solution **b)**.

??? note "User"
    In this case, the external package does not change while you are developing the pipeline. This is typical when the MATLAB package comes from a stable distribution such as MATLAB File Exchange, a released toolbox, or a remote repository (e.g., GitHub or GitLab).

    To add such a package to your node:

    - Navigate to the node's code folder: `cd node_id/code`
    - Add the package using one of the following methods:

    ??? note "Package from MATLAB File Exchange"
        1. Download the package from MATLAB File Exchange
        2. Extract it to your node's `code` directory or a subdirectory
        3. Add the package to MATLAB's path in your script:
        ```matlab
        addpath(genpath('./downloaded_package'));
        ```

    ??? note "Package hosted on GitHub/GitLab or another remote repository"
        You can clone the repository directly into your node. For example:
        ```bash
        cd node_id/code
        git clone https://github.com/username/repository.git
        ```
        Then add it to MATLAB's path:
        ```matlab
        addpath(genpath('./repository'));
        ```

        !!! tip
            For better reproducibility and to ensure all nodes use the same version of the package, it is recommended to:

            1. Create a dedicated node to download and store the package locally.
            2. Clone or download the package into this node at a specific commit/tag.
            3. Reference the package in other nodes by providing the absolute path to the local copy. For example:
                ```matlab
                addpath(genpath('/absolute/path/to/package'));
                ```
            This ensures all nodes reference the same package version, avoiding inconsistencies due to remote updates or changes.


---

If your MATLAB code has dependencies on specific toolboxes or external libraries, document them clearly in your node's README or in comments within your MATLAB scripts. For external libraries (non-MATLAB), consider creating installation scripts or providing clear setup instructions within the node.