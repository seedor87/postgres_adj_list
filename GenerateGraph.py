import random, csv, string

MIN_PER_RANK = 0
MAX_PER_RANK = 5
MIN_RANKS = 20
MAX_RANKS = 20
PERCENT = 10
data = []

def rand():
    return int(random.random() * 1000000)

def random_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def main():

    with open("./out.dot", 'w') as dotFile, open('./data.csv', 'w', newline='') as csvFile:
        csvWriter = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        nodes = 1
        ranks = MIN_RANKS + (rand() % (MAX_RANKS - MIN_RANKS + 1))

        csvWriter.writerow(("parent", "child"))
        dotFile.write("digraph {\n")

        for i in range(0, ranks):
            new_nodes = MIN_PER_RANK + (rand() % (MAX_PER_RANK - MIN_PER_RANK + 1))

            if (i == 0):
                csvWriter.writerow((-1, 0, random_generator(10)))
                dotFile.write(" %d -> %d;\n" % (-1, 0))

                for k in range(0, new_nodes):
                    csvWriter.writerow((-1, k+nodes, random_generator(10)))
                    dotFile.write(" %d -> %d;\n" % (-1, k+nodes))

            for j in range(0, nodes):
                for k in range(0, new_nodes):
                    if ((rand() % 100) < PERCENT):
                        csvWriter.writerow((j, k+nodes, random_generator(10)))
                        dotFile.write(" %d -> %d;\n" % (j, k+nodes))
            nodes += new_nodes
        print()
        dotFile.write("}\n")

    print("Done")

if __name__ == "__main__":
    main()