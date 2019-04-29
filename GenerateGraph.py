import random

MIN_PER_RANK = 1
MAX_PER_RANK = 4
MIN_RANKS = 5
MAX_RANKS = 7
PERCENT = 30

def rand():
    return int(random.random() * 1000000)

def main():

    with open("./out.dot", 'w') as myFile:

        nodes = 1
        ranks = MIN_RANKS + (rand() % (MAX_RANKS - MIN_RANKS + 1))

        print("INSERT INTO task VALUES")
        print("(NULL, -1, MD5(random()::Text)),")
        myFile.write("digraph {\n")
        myFile.write(" NULL -> %d;\n" % -1)

        for i in range(0, ranks):
            new_nodes = MIN_PER_RANK + (rand() % (MAX_PER_RANK - MIN_PER_RANK + 1))

            if (i == 0):
                print("(%d, %d, MD5(random()::text))," % (-1, 0))
                myFile.write(" %d -> %d;\n" % (-1, 0))

                for k in range(0, new_nodes):
                    print("(%d, %d, MD5(random()::text))," % (-1, k+nodes))
                    myFile.write(" %d -> %d;\n" % (-1, k+nodes))

            for j in range(0, nodes):
                for k in range(0, new_nodes):
                    if ((rand() % 100) < PERCENT):
                        print("(%d, %d, MD5(random()::text))," % (j, k + nodes))
                        myFile.write(" %d -> %d;\n" % (j, k + nodes))
            nodes += new_nodes
        print()
        myFile.write("}\n")
    print("Done")

if __name__ == "__main__":
    main()