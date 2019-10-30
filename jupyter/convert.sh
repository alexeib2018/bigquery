#!/bin/bash

if [ $1 ]
then

IPYNB_FILE=$1
PY_FILE=${IPYNB_FILE/\.ipynb/\.py}
echo Convert $IPYNB_FILE to $PY_FILE
../.venv3/bin/jupyter nbconvert --to python --execute $1
sed -i 's/#\!\/usr\/bin\/env\ python/#\!\.\.\/\.venv3\/bin\/python/' $PY_FILE
chmod +x $PY_FILE
mv $PY_FILE ../cgi/

fi
