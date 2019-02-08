#!/bin/bash -x

################################
#
# Prep
#
################################

export CASSANDRA_DIR=$1

if [ "${CASSANDRA_DIR}" = "" ]; then
    echo "Specify Cassandra source directory"
    exit
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export PYTHONIOENCODING="utf-8"
export PYTHONUNBUFFERED=true
export CASS_DRIVER_NO_EXTENSIONS=true
export CASS_DRIVER_NO_CYTHON=true
export CCM_MAX_HEAP_SIZE="2048M"
export CCM_HEAP_NEWSIZE="200M"
export CCM_CONFIG_DIR=${SCRIPT_DIR}/.ccm
export NUM_TOKENS="32"

# Loop to prevent failure due to maven-ant-tasks not downloading a jar..
for x in $(seq 1 3); do
#    ant -buildfile ${CASSANDRA_DIR}/build.xml realclean jar
    echo 'hi'
    RETURN="$?"
    if [ "${RETURN}" -eq "0" ]; then
        break
    fi
done
# Exit, if we didn't build successfully
if [ "${RETURN}" -ne "0" ]; then
    echo "Build failed with exit code: ${RETURN}"
    exit ${RETURN}
fi

# Set up venv with dtest dependencies
set -e # enable immediate exit if venv setup fails
virtualenv --python=python2 --no-site-packages venv
source venv/bin/activate
pip install -r ${SCRIPT_DIR}/requirements.txt
pip freeze

if [ "$cython" = "yes" ]; then
    pip install "Cython>=0.20,<0.25"
    cd ${SCRIPT_DIR}; python setup.py build_ext --inplace
fi

################################
#
# Main
#
################################

ccm remove test || true # in case an old ccm cluster is left behind
ccm create test -n 1 --install-dir=${CASSANDRA_DIR}
ccm updateconf "enable_user_defined_functions: true"

version_from_build=$(ccm node1 versionfrombuild)
export pre_or_post_cdc=$(python -c """from distutils.version import LooseVersion
print \"postcdc\" if LooseVersion(\"${version_from_build}\") >= \"3.8\" else \"precdc\"
""")
case "${pre_or_post_cdc}" in
    postcdc)
        ccm updateconf "cdc_enabled: true"
        ;;
    precdc)
        :
        ;;
    *)
        echo "${pre_or_post_cdc}" is an invalid value.
        exit 1
        ;;
esac

ccm start --wait-for-binary-proto

cd ${SCRIPT_DIR}/cqlshlib/

set +e # disable immediate exit from this point
nosetests

ccm remove
mv nosetests.xml ${SCRIPT_DIR}/cqlshlib.xml

################################
#
# Clean
#
################################

# /virtualenv
deactivate

# Exit cleanly for usable "Unstable" status
exit 0
