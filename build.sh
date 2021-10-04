#!/usr/bin/env bash

PATH=/usr/local/bin/:$PATH

# Set this directory to be the base directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${DIR}"

# Determine if running in jenkins server (test for jenkins WORKSPACE variable)
if [ -z ${WORKSPACE+x} ]; then
    # Production/Dev server
    echo "Running build on Production/Dev Server"
    PROJECT_ROOT=$(pwd)
else
    # Jenkins Server
    echo "Running build on Jenkins Server: $JENKINS_URL"
    PROJECT_ROOT=$WORKSPACE
fi

PYENV_HOME="$PROJECT_ROOT/.virtualenv"

# Delete previously built virtualenv
if [ -d "$PYENV_HOME" ]; then
    rm -rf "$PYENV_HOME"
fi

alias python=python3.7
alias pip=pip3.7

# Create virtualenv and install necessary packages
pip install --upgrade virtualenv
virtualenv $PYENV_HOME
source "$PYENV_HOME/bin/activate"
pip install --quiet mock
pip install --quiet pylint

python "$PROJECT_ROOT/setup.py" install --user
#pylint -f parseable "$PROJECT_ROOT/$ROOT_CLASS_FOLDER/" | tee pylint.out
python "$PROJECT_ROOT/setup.py" develop --user