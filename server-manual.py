#this program is designed to dynamically run gquery on specific nodes

import os
import sys
import json
import argparse
import os
import subprocess
import io
import time

#tatiana w1~w10
#liujie  w11~w15
#zzxx    w21~w30

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '-nnodes', '--nnodes', help='node cnt', required=True)
    args = vars(parser.parse_args())

    nnodes = int(args['nnodes'])

    #read from host file
    f = open('gq-hfs.txt', 'r')
    #get the hosts
    lns = f.readlines()
    f.close()
    #host in gq-hfs is in number format

    hf = open('manual-nodes.cfg', 'w')
    cfgf = open('manual_conf', 'w')

    #generate mpi hf and gq hf
    for i in range(nnodes):
        num_str = lns[i][:-1]
        hf.write("w" + num_str + "\n")
        if(i == 0):
            #the master
            cfgf.write("w" + num_str + ":ib" + num_str + ":19935:-1\n")
        else:
            cfgf.write("w" + num_str + ":ib" + num_str + ":19935:19936\n")

    hf.close()
    cfgf.close()

    #execute the command

    os.system("export GQUERY_HOME=${PWD}; mpirun -n " + str(nnodes) + " -f manual-nodes.cfg ./release/server manual_conf")

