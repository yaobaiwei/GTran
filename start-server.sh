# $1 PRO_NUM   $2 MACHINE.CFG    $3 IB.CONF
export MKL_LIB_PATH=/data/cghuan/ps2017u7/compilers_and_libraries_2017.7.259/linux/mkl/lib/intel64_lin
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${MKL_LIB_PATH}
mpirun -ppn 1 -n $1 -f $2 ./release/server $3
