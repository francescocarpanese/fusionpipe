#!/bin/bash

# This script creates a file in the /<username>/.bash_fusionpipeenv with the environment variables needed for fusionpipe
# It also checks if the .bashrc sources this file, and if not, it adds the sourcing line.
# !requires sudo

# Example call ./set_usr_env.sh <username>

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <username>"
    exit 1
fi

USERNAME="$1"
USER_HOME=$(eval echo "~$USERNAME")

if [ ! -d "$USER_HOME" ]; then
    echo "User home directory for '$USERNAME' does not exist."
    exit 2
fi

ENV_FILE="$USER_HOME/.bash_fusionpipeenv"
BASHRC_FILE="$USER_HOME/.bashrc"

# Write environment file
# Read directly the ENV variables if they are set
cat > "$ENV_FILE" <<EOF
# User specific environment and startup programs
export USER_UTILS_FOLDER_PATH="${USER_UTILS_FOLDER_PATH:-/home/fusionpipeadmin/Documents/fusionpipe/fp_user_utils}"
export DATABASE_URL="${DATABASE_URL:-dbname=fusionpipe_prod4 port=5432}"
export FP_MATLAB_RUNNER_PATH="${FP_MATLAB_RUNNER_PATH:-/data/codes/MATLAB/R2025a/bin/matlab}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/data/defuse/fusionpipe-data-1/.cache/uv_cache}"
export UV_PYTHON_INSTALL_DIR="${UV_PYTHON_INSTALL_DIR:-/home/fusionpipeadmin/.local/share/uv/python}"
EOF

chown "$USERNAME" "$ENV_FILE"

# Check and add sourcing to .bashrc if not present and add in case.
SOURCE_BLOCK='# Source environment variables for fusionpipe
if [ -f ~/.bash_fusionpipeenv ]; then
    . ~/.bash_fusionpipeenv
fi'

if ! grep -Fxq "if [ -f ~/.bash_fusionpipeenv ]; then" "$BASHRC_FILE"; then
    echo -e "\n$SOURCE_BLOCK" >> "$BASHRC_FILE"
    chown "$USERNAME" "$BASHRC_FILE"
    echo "Added fusionpipe environment sourcing to $BASHRC_FILE"
else
    echo "Fusionpipe environment sourcing already present in $BASHRC_FILE"
fi

echo "Done."