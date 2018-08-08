import sys

sum = 0
count = 0

if ( len(sys.argv) != 3 ) :
    print("2 arguments needed")
    sys.exit(2)

for num in range(int(sys.argv[1]), int(sys.argv[2])) :
    fname = "/data/aaron/gquery/output/output" + str(num)
    file = open(fname, "r")

    for line in file :
        tokens = line.rstrip('\n').split(" ")
        for token in tokens :
            if ( token == "[Timer]" ):
                count += 1
                if ( tokens[2] == "ms"):
                    sum = sum + int(tokens[1]) * 1000
                else:
                    sum = sum + int(tokens[1])
            elif (token == "[Error]" or token == "error" or token == "Error" or token == "[error]" or token == "Error:"):
                print(line)

print("Average Time : ", round(sum / count, 4), " us");
