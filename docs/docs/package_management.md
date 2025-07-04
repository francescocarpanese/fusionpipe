

# python 


Difference between python scripts and python project


Recommandede solution, using uv

### is packaged

### is not packaged


## developer
You are developing a 


## is packaged


!!! note "adsad"
    asdas asda sda 



### all node shared the same code



### every node has an instance of the code


??? note "Developer"
    You are developing the your python code while building your pipeline. This is the most common case when you are working on an application.
    Usually your code is versioned in `.git` on a given repo "myrepo".

    There are two possibile solutions that you can adopt.
    ??? note "a) Every node has an independent snapshot of the code"
        In this case in every node you keep a snapshot of your code. 
        This means that you either copy paste your code from your local computer, or you clone the code from any remote `.git`. 


        ```bash
        git clone <url/myrepo>
        ```

        You can then add your repo to the `.venv` of the node with 
        ```bash
        uv add --editable <myrepo>
        ```

        This will automatically install your project with its dependencies.

        
        !!! tip 
            `uv` will not re-install these dependecies if those are already in the cache

    ??? note "b) Every node share the same instance of the code"
        adsada

    


## is not packaged



# matlab