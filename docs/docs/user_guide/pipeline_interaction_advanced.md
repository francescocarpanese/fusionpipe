
## Advanced user interaction 

### Duplicate or reference nodes

You can select one or more nodes with `shift + right click`. Once you have selected the desired nodes, you can duplicate them using several available options. Open the `Node interaction` panel to see three different options:

`Duplicate selected nodes in this pipeline`: this command is used to duplicate the nodes in the same pipeline. You can choose to copy the node with or without data (`Duplicate with data`, `Duplicate without data`). The new node will have a new unique identifier, but the same tag as the original node.

`Duplicate selected nodes into another pipeline`: duplicate the node into a different pipeline (select the pipeline from the option `Select a pipeline...`) with or without data (`Duplicate with data`, `Duplicate without data`). The new node will have a new unique identifier, but the same tag as the original node.

`Reference selected nodes into another pipeline`: the node is not duplicated. Select the pipeline with `Select a pipeline...`, then click on `Reference nodes`. The same `node_id` is referenced in a different pipeline and the `node` is set in the status locked. This means that if you change the code or the data in this node, it will be changed in all the pipelines that reference this node. A lock symbol (<img src="../../images/Lock_.jpg" alt="lock" style="width: 1.3em; height: 1.3em; display: inline; vertical-align: middle;">) will appear in that node and all of its references in the other pipelines. This is especially convenient when you want to use the output of the node in a different pipeline, without duplicating the data.

!!! info
    We remind the user that a `node_id` is unique within a given pipeline. But the same `node_id` can appear in multiple pipeline after a reference operation.


## Locked nodes <img src="../../images/Lock_.jpg" alt="lock" style="width: 1.2em; height: 1.2em; display: inline; vertical-align: middle;">

When a node is in "locked" status (<img src="../../images/Lock_.jpg" alt="lock" style="width: 1.3em; height: 1.3em; display: inline; vertical-align: middle;">), it indicates that this node is referenced by multiple pipelines. Any changes made to the code or data within a locked node will automatically propagate to all pipelines that reference it.

!!! warning
    In general, you should NOT modify the code or outputs of a `locked` node, as this may unintentionally affect other pipelines or colleagues who also reference this node. If you need to make changes to a `locked` node within your pipeline, it is recommended to first duplicate the node and then modify the duplicated version.
