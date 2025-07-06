
# Complete list of functions

- **`Project Interaction`**

Different pipelines can be grouped in a project.

`List ids`: use this option to search and select a project by its unique identifier.

`List tags`: use this option to search and select a project by its unique tag.

`Select a project`: this option displays a list of available projects, ordered by either id or tag based on your previous selection. From this list, you can choose the project you wish to open.

`Open selected project panel`: this panel allows you to change the project tag and add notes that may be useful for you and future project users.

`Create project`: select this option to create a new project.
 
`Delete project`: select this option to delete the actual project.

- **`Pipeline Interaction`**:

`List ids`: use this option to search and select a pipeline by its unique identifier.

`List tags`: use this option to search and select a pipeline by its unique tag.

`Select a pipeline`: this option displays a list of available pipelines, ordered by either id or tag based on your previous selection. From this list, you can choose the pipeline you wish to open.

`Open selected pipeline panel`: this panel allows you to change the pipeline tag and add notes that may be useful for you and future pipelines users.

`Create Pipeline`: select this option to create a new pipeline.

`Branch Pipeline from selected node`: select this option to delete the actual pipeline.

`Move Pipeline to project`: use this command to move a pipeline into a different project. Click on `Select a project`, choose the destination project, and then click `Move` to transfer the pipeline.

`Merge pipelines`: use this command to combine multiple pipelines into a single new pipeline. Click on `Select multiple pipelines`, choose the pipelines you wish to merge, and then select `Merge`. The resulting pipeline will include a unique copy of all nodes that are shared between the selected pipelines, as well as all nodes that are unique to each pipeline. This ensures that no data or workflow steps are lost during the merge process and the new pipeline provides a comprehensive view of all included nodes.

`Delete Pipeline`: select this option to delete the actual pipeline.


- **Node Interaction**:

`Open selected node panel`: this panel allows you to change the pipeline tag and add notes that may be useful for you and future pipelines users. You can fill the `Node Parameters (YAML)` box, which will store your parmeters in a YAML file to be used as fixed parameteres inputs in the node code.


`Copy selected node path to clipboard`: select a node and use this option to copy its folder path to the clipboard. Alternatively, you can right-click on the node and choose the "Copy Folder Path" option from the context menu for a quicker solution.

`Create node`:  select this option to create a new node.

`Duplicate selected nodes into this pipeline`: duplicate the selected nodes into the pipeline you are working on with or without data (`Duplicate with data`, `Duplicate without data`).

`Duplicate selected nodes into another pipeline`: duplicate the selected nodes into a different pipeline with or without data (`Duplicate with data`, `Duplicate without data`).

`Reference selected nodes into another pipeline`: reference the selected nodes into a different pipeline.

`Manual set node "completed"`: use this option to manually set the selected node to the <span style="color: green;">_completed_</span> state. This can be useful for marking a node as finished when its output is already available or when you want to bypass execution for testing or troubleshooting purposes.

`Manual set node "stale-data"`: use this option to manually set the selected node to the <span style="color:rgb(253, 186, 3);">_stale-data_</span> state. It can be useful if the user has changed the code in a node. Then it can flag the node to be in <span style="color:rgb(253, 186, 3);">_stale-data_</span> state. This way the pipeline needs to re-run the node and all its children.

`Delete output selected nodes`: use this command to clean the output data folder of a selected node.

`Delete selected edge`: use this option to delete a connection between two nodes.

`Delete selected nodes`: use this command to delete a selected node.


- **Actions**:

`Run selected node`: this function will run the code of a selected node, setting its state to <span style="color: blue;">_running_</span> state and at the end of the process to <span style="color: green;">_completed_</span> or <span style="color: red;">_failed_</span> state.

`Run full pipeline`: use this command to run the full pipeline.

`Run pipeline up to selected node`: select a node and click on this option to run the pipeline up to the selected node.

`Open run panel`: ...

`Kill run selected node`: select a node and choose this option to kill its execution.


- **Layout**:

`Refresh pipeline`: click this command to refresh the state of pipeline nodes.

`Auto reshape`: use this option to automatically arrange the nodes in your pipeline for optimal organization and improved visualization.
