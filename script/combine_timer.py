import sys
import re
from datetime import datetime

#USAGE： To collect the profile info of each query step from all nodes to a new created file "timer_XXX.txt"

l = {}
d = {}
mind = {}
maxd = {}
sumd = {}
for num in range(0, 8) :
    fname = "./timer" + str(num) + ".txt"
    file = open(fname, "r")

    for line in file :
        line.rstrip("\n")
        tokens = line.split()

        if (len(tokens) == 3) :
            del tokens[1]
        elif (len(tokens) == 0) :
            continue

        #print(tokens[0], " --> ", tokens[1])
        if (l.get(str(tokens[0])) == None) :
            tokens[0].rstrip(":")
            l[tokens[0]] = "\t" + tokens[1] + "\n"
            time = float(tokens[1].rstrip("ms"))
            d[tokens[0]] = time
            mind[tokens[0]] = time
            maxd[tokens[0]] = time
            sumd[tokens[0]] = 1
        else :
            tokens[0].rstrip(":")
            l[tokens[0]] += "\t" + tokens[1] + "\n"
            time = float(tokens[1].rstrip("ms"))
            d[tokens[0]] += time
            sumd[tokens[0]] += 1
            if(mind[tokens[0]] > time):
                mind[tokens[0]] = time
            if(maxd[tokens[0]] < time):
                maxd[tokens[0]] = time

timestamp = datetime.now().strftime('%H:%M:%S')
ofname = "./timer"+ timestamp +".txt"
keylist = list(l.keys())
keylist.sort()
f = open(ofname, 'w')
for key in keylist:
    f.write(key + " --> \n" + l[key])
    f.write("\nave:\t" + str(d[key] / sumd[key]) + "ms\n")
    f.write("max:\t" + str(maxd[key]) + "ms\n")
    f.write("min:\t" + str(mind[key]) + "ms\n\n")

f.close()

