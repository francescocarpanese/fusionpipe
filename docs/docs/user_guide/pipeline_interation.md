

# Basic user interaction


- Create a pipeline

The pipeline has unique identified and a tag. 

Select per tag or ids

- Open the panle to change name, add notes.
Yuo can open a the pioeline pannel to change the tag.


- Creata a node

Node has aunique identifies.

A given node_id can appear only once in a given pipeline.
The same node_id can be repeated in multiple pipelines.


Tag 
Open panel to change tag and add notes.

Right clik on a node to copy the location of the node, and modify you code.

Connection between nodes.

Layout refresh to organise th node sin the space.


Delete nodes
- Select node and delte
- Select node and delete node outputs. (node will stay, code will stay, data folder empty)
- Yon can select multiple nodes with shift and drag drop
- Delete edge 

Status of the node:
- gray: ready run -> the output folder is empty
- green: completed -> node has run and completed succesfully
- blue: running -> The node is currently running the code.
- red: failed -> running the node has failed. Open the node folder and check the `log.txt` for debugging outputs.

- yellow: staledata -> the output of the node might no longer be consisent with the code inside the node or the input of the node. This suggest that the pipeline should be rerun.
REference to a section that explaion all the cases 

# Section on stale-data
This can happen in the following situation:
- The user has changed the code in a node. Then it can flag the node to be in "stale-data". This way the pipeline needs to re-run the node and all its children.
- A node was in state completed, but the user changes the inputs of the node.
- If you have multiple connected nodes,  which are completed, and you delete the output of one of these nodes. All the children nodes will be in the in stale data
- If you have multiple node, connected, which are completed, and you delete the edge of a node. All the children of this edge enters the state stale-data.
- When you duplicate a node, with data, the duplicated node by default is in the status stale-data.


Actions
- Select a node and run.
This can only run of node has no parents, or all the parents of the node are completed

- Run full pipeline
Run all the nodes following the three structure. (Sequentially)

- Run pipelione up to a node
Run a pipeline up to a node

- Kill running node
The process of the node that is running is stoped.


# Advanced user interaction 

- Select one or more nodes. Duplicate the nodes.

- Duplicate node in this pipeline
   -- with data 
   -- without data 

- Duplicate node in another pipeline
   -- with data 
   -- without data 

- Reference node in another pipeline
In this case the node is not duplicated . The same node id is references in a different pipeline and the node_id is set in the status locked.
This means that if you change the code or the data in this node, it will changed in all the pipelines that reference this node.



# Session dedicated to locked nodes
- When the node is in the status "locked" it means that the same node is referenced in multiple pipelines. This means that if you change the code inside this node, you will have 
all the pipelines that references this node will have an updated code and data.

This can brake the workflow of other pipeline. In general, if you want to iterate on a node which is locked, by changing its code, you should first duplicate the "locked" node in the same pipeline, and then run the new one. Doing that, the duplicated node has a new "node_id", and a separated folder, and you will not risk to break other pipelines. 


# Complete list of functions 

