When you want to start a to work on a new pipeline, the most common situation is that you would like, together with your pipeline, to develop a package associate with that that you version control in `.git` to be re-use in the future. 

In the following we provide the recommended strategy to do that. 

## Set up new python package
Initialise a new python package, we recomend start your package with `uv`.

```bash
uv init --package <mypackage>
```

This commands will prepare for the folder structure for your project including.

- The folder structure of your package will be
```
mypackage
  src
    mypackage
      __init__.py
      your_function.py
pyproject.toml
```

- `pyproject.toml`: where you can list your dependencies.
- `.git`: the folder is already initialised with git so that you can push your folder to a remote branch.

The create an empty repository in `github`/`gitlab`, and call it with the same name as `<mypackage>`. 

Enter the folder in your OS
```bash
cd <mypackage>
```

and set the remote `gitlab`/`github` to point to the remote folder
```bash
git remote add origin git@github.com:<your-username>/<mypackage>.git
```

Push your repo,
```bash
git push -u origin main
```

You will use your package to track and version control the code that will be re-used inside your pipeline or in other pipeline.

# Develop code for your node.
You are now ready to develop your pipeline and the code associated with that.

- Now, create a new pipeline from the interface.

- Give a tag to your pipeline. 

- Create node

- Right click on a node and navigate the folder directory of a node.

- Enter the code folder

```bash
cd <node_id>/code
```

- Clone yuor  python package

```bash
git clone <package_url>
```

Your node folder path will look like
```bash
<node_id>
    code
        <mypacakge>
            src
                <mypackage>
        main.py
        pyproject.toml
    data
    reports
```


- Any function that you would like to track and reuse, develop it inside `<mypackage>/src/<mypackage>`. For example suppose that you have developed a function to print the parents of a given node the dataset coming from a previous node `<mypackage>/src/<mypackate>/dataset.py` where inside you have the function
```python
def print_parents(parent_folders):
    for folder in parent_folders:
        print(folder)
```

- You can then track this changes in a remote folder
```
git add <myproject>/src/<myproject>dataset.py
git commit -m "some comments"
git push
```

- Install the python pacakge in your node 
```bash
uv add --editable <mypackage>
```

!!! tip
    Use the `--editable` flag allows, when you change the functions of your package, to be immeditely available also to the function 


- In order to run the function as part of the pipeline, you will need to call the function of your package from the `main.py`. You can see the examples that are available as a template in the node folder.

Here how the `main.py` could look like

```python
# Import the user api 
from python_user_utils.node_api import get_all_parent_node_folder_paths, get_node_id, get_folder_path_data, get_folder_path_reports, get_folder_path_code, get_folder_path_node

# Import the function from your package
from mypackage.dataset import print_parents

# Get
if __name__ == "__main__":
    node_id = get_node_id()
    # Get the parent folder where you can read the outputs of the parent nodes
    parent_folders = get_all_parent_node_folder_paths(node_id=node_id)

    # use the function from your package
    print_parents(parent_folders)
```

Test now your function is running as part of the pipeline. 

- Navigate the node folder
```bash
cd code
```

- Test main function in the node is running
```bash
uv run python main.py
```

- Kill the process eventually if that is too long.

!!! tip
    When you are developing a function that processes multiple samples, it is conveninent to implement a `preview` option that run the function only for few samples. This way you can quickly iterate while developing your function.


Now you are sure that your code is running, and you can run your full pipeline from the UI.


More details are available in [user_guide/develop_node.md].


# Create new nodes and iterate on your package.

It is recommended to have a snapshot of your package in every node. This way the pipeline will be always reproducible even when your package will evolve.

To that, you can either duplicate you can:

- a) Duplicate a node that contain your python package


Navigate the node 

Make sure you pull the lastest version of your package in the node

-b) Clone your pacakge and follow the procedure to install the package in your node as above.

Make sure the node is install with 
```bash
uv add --editable <myrepo>
```

After modifying your code in your package, make sure to push the code that you would like to be versioned.

More details on how to handle your external packages are described in [External Dependencies/python](package_management_python.md). In particalr you can fund a comparison with the strategy of having a single code where all node are referencing instead of the proposed solution here.

