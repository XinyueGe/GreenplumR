#!/bin/bash -l

set -exo pipefail
OLDPATH=${PATH}
echo "OLDPATH = ${OLDPATH}"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts
MODE=${MODE:=light}

source "${GPDB_CONCOURSE_DIR}/common.bash"

#
function test_run() {
  cat > /home/gpadmin/test_prepare.sh <<-EOF
#!/bin/bash -l
# install plr and restart gpdb
# then prepare databases needed by test
set -exo pipefail
  source /usr/local/greenplum-db-devel/greenplum_path.sh
  source ${TOP_DIR}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
  gppkg -i ${TOP_DIR}/bin_plr/plr-*.gppkg
  sleep 1
  source /usr/local/greenplum-db-devel/greenplum_path.sh
  source ${TOP_DIR}/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
  echo "R_HOME=`R RHOME`"
  gpstop -arf
  createdb rtest
  createdb d_apply
  createdb d_tapply
EOF
  cat > /home/gpadmin/test_run.sh <<-EOF
#!/bin/bash -l
set -exo pipefail
export GPRLANGUAGE=plr
pushd ${TOP_DIR}/GreenplumR_src
  # clear environment introduced by gpdb that may affect R
  sleep 3
  export PATH=${OLDPATH}
  export R_HOME=`R RHOME`
  echo "R_HOME=`R RHOME`"
  unset LD_LIBRARY_PATH
  unset R_LIBS_USER
  if [ "$MODE" == "light" ] ; then
    echo "library(testthat)" > test_script.R
    echo "testthat::test_dir('tests', reporter = 'failreporter', stop_on_failure = TRUE)" >> test_script.R
    R --no-save < test_script.R
  else
    R CMD check .
  fi
popd
EOF

  chown -R gpadmin:gpadmin $(pwd)
  pushd /home/gpadmin
    chown gpadmin:gpadmin test_prepare.sh test_run.sh
    chmod a+x test_prepare.sh test_run.sh
  popd
  su gpadmin -c "/bin/bash /home/gpadmin/test_prepare.sh"
  su gpadmin -c "/bin/bash /home/gpadmin/test_run.sh"
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

function install_libraries_heavy() {
  # install system libraries
  case $TEST_OS in
  centos)
    yum install -y epel-release
    # postgresql-devel is needed by RPostgreSQL
    yum install -y R postgresql-devel
    ;;
  ubuntu)
    apt update
    DEBIAN_FRONTEND=noninteractive apt install -y r-base pkg-config \
        texlive-latex-base texlive-fonts-extra
    ;;
  *)
    echo "unknown TEST_OS = $TEST_OS"
    exit 1
    ;;
  esac

  # install r libraries
    ${CWDIR}/install_r_package.R devtools
    ${CWDIR}/install_r_package.R testthat
    ${CWDIR}/install_r_package.R DBI
    ${CWDIR}/install_r_package.R RPostgreSQL
    ${CWDIR}/install_r_package.R shiny
    ${CWDIR}/install_r_package.R ini
}

function install_libraries_light() {
  # install system libraries
  case $TEST_OS in
  centos)
    yum install -y epel-release
    yum install -y R
    ;;
  ubuntu)
    apt update
    DEBIAN_FRONTEND=noninteractive apt install -y r-base
    ;;
  *)
    echo "unknown TEST_OS = $TEST_OS"
    exit 1
    ;;
  esac
    # install r libraries
    ${CWDIR}/install_r_package.R testthat
    ${CWDIR}/install_r_package.R RPostgreSQL
    ${CWDIR}/install_r_package.R shiny
    ${CWDIR}/install_r_package.R ini
}

# install libraries (light/heavy)
# install gpdb
# setup gpadmin
# make cluster
# install plr/plcontainer
# restart gpdb
# clear environment introduced by gpdb
#
# run tests (light/heavy)
function _main() {
    TEST_OS=$(determine_os)
    time install_libraries_${MODE}
    time install_gpdb
    time setup_gpadmin_user
    time make_cluster

    time test_run
}

_main "$@"
