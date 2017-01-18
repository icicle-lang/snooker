#!/bin/sh -eu

HADOOP=${HADOOP:-hadoop-2.2.0-cdh5.0.0-beta-2}
HADOOP_SHA1=${HADOOP_SHA1:-8bc92c14ceba3802e7acf59e9ec0f897a7609385}

export AWS_DEFAULT_REGION=ap-southeast-2
# For hadoop - needs JAVA_HOME set, but only for the 'java' command. :(
#export JAVA_HOME=$(dirname $(which java))/..

if [ ! -e "${DIR}/dist/${HADOOP}" ]; then
    mkdir -p ${DIR}/dist
    curl "http://archive-primary.cloudera.com/cdh5/cdh/5/${HADOOP}.tar.gz" > ${DIR}/dist/${HADOOP}.tar.gz
    echo "${HADOOP_SHA1}  ${DIR}/dist/${HADOOP}.tar.gz" | shasum -c -
    tar xf ${DIR}/dist/${HADOOP}.tar.gz -C ${DIR}/dist/
fi
