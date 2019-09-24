#!/bin/bash -l

set -exo pipefail
OLDPATH=${PATH}
echo "OLDPATH = ${OLDPATH}"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts

source "${GPDB_CONCOURSE_DIR}/common.bash"
function test() {
  cat > /home/gpadmin/test.sh <<-EOF
#!/bin/bash -l
set -exo pipefail
export GPRLANGUAGE=plr
pushd ${TOP_DIR}/GreenplumR_src
  source /usr/local/greenplum-db-devel/greenplum_path.sh
  source ${TOP_DIR}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
  gppkg -i ${TOP_DIR}/bin_plr/plr-*.gppkg
  sleep 1
  gpstop -arf
  createdb d_apply
  createdb d_tapply
  # start test
  sleep 3
  export PATH=${OLDPATH}
  export R_HOME=`R RHOME`
  unset LD_LIBRARY_PATH
  unset R_LIBS_USER
  R CMD check .
popd
EOF

  chown -R gpadmin:gpadmin $(pwd)
  chown gpadmin:gpadmin /home/gpadmin/test.sh
  chmod a+x /home/gpadmin/test.sh
  su gpadmin -c "/bin/bash /home/gpadmin/test.sh"
}

function determine_os() {
    if [ -f /etc/redhat-release ] ; then
      echo "centos"
      return
    fi
    if grep -q ID=ubuntu /etc/os-release ; then
      echo "ubuntu"
      return
    fi
    echo "Could not determine operating system type" >/dev/stderr
    exit 1
}

function setup_gpadmin_user() {
    ${GPDB_CONCOURSE_DIR}/setup_gpadmin_user.bash
}

function prepare_lib() {
    ${CWDIR}/install_r_package.R devtools
    ${CWDIR}/install_r_package.R testthat
    ${CWDIR}/install_r_package.R DBI
    ${CWDIR}/install_r_package.R RPostgreSQL
    ${CWDIR}/install_r_package.R shiny
    ${CWDIR}/install_r_package.R ini
}

function install_pkg() {
  case $TEST_OS in
  centos)
    yum install -y epel-release
    yum install -y R
    ;;
  ubuntu)
    apt update
    DEBIAN_FRONTEND=noninteractive apt install -y r-base pkg-config
    #texlive-latex-base texinfo texlive-fonts-extra
    ;;
  *)
    echo "unknown OSVER = $TEST_OS"
    exit 1
    ;;
  esac
}

function install_plr() {
    source /usr/local/greenplum-db-devel/greenplum_path.sh
    source ${TOP_DIR}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
    gppkg -i ${TOP_DIR}/bin_plr/plr-*.gppkg
    sleep 1
    gpstop -arf
}

function _main() {
    TEST_OS=$(determine_os)
    time install_pkg
    time install_gpdb
    time setup_gpadmin_user

    time make_cluster
    
    time prepare_lib
    time test
}

_main "$@"