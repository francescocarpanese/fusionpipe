#!/bin/bash
# filepath: /home/fusionpipeadmin/Documents/fusionpipe/scratch/grant_project_access.sh

# Script to grant the unix group permission to a project folder
# Example call ./grant_project_permission_to_group.sh <group_name> <project_folder_path>
# <project_folder_path> is the full path to the project folder

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <group_name> <project_folder_path>"
    exit 1
fi

GROUP_NAME="$1"
PROJECT_PATH="$2"

# Check if group exists
if ! getent group "$GROUP_NAME" > /dev/null; then
    echo "Group '$GROUP_NAME' does not exist."
    exit 2
fi

# Print users of the group
GROUP_INFO=$(getent group "$GROUP_NAME")
IFS=':' read -r _ _ _ USERS <<< "$GROUP_INFO"
echo "Users in group '$GROUP_NAME':"
if [ -z "$USERS" ]; then
    echo "  (No users in this group)"
else
    echo "  $USERS"
fi

# Set permissions.
# User and group has rwx permission
# AFCL is set to ensure new files and folders inherit the same permissions
echo "Setting permissions for $PROJECT_PATH with group $GROUP_NAME..."
sudo chown -R :"$GROUP_NAME" "$PROJECT_PATH"
sudo chmod -R 2770 "$PROJECT_PATH"

sudo setfacl -R -m g::rwx "$PROJECT_PATH"
sudo setfacl -R -m m::rwx "$PROJECT_PATH"
sudo setfacl -R -m o::--- "$PROJECT_PATH"

sudo setfacl -R -d -m g::rwx "$PROJECT_PATH"
sudo setfacl -R -d -m m::rwx "$PROJECT_PATH"
sudo setfacl -R -d -m o::--- "$PROJECT_PATH"

echo "Done."