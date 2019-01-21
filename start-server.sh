source script/runtime_environment.sh
mpirun -ppn 1 -n 7 -f machine.cfg ./release/server ib_conf
