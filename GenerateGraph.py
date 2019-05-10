import random, csv, string

MIN_PER_RANK = 3
MAX_PER_RANK = 5
MIN_RANKS = 4
MAX_RANKS = 5
PERCENT = 50
data = []

def rand():
    return int(random.random() * 1000000)

def random_number_of_nodes():
    return MIN_PER_RANK + (rand() % (MAX_PER_RANK - MIN_PER_RANK + 1))

def random_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def generate_random_tree(csvWriter, dotFile, idx=0, parent=0, depth=0, max_children=2, max_depth=2):
    n = random_number_of_nodes()

    new_idx = idx
    for i in range(1, n):
        csvWriter.writerow((parent, idx + i, random_generator(10)))
        dotFile.write(" %d -> %d;\n" % (parent, idx + i))

        new_idx+=1

        if depth < max_depth and depth >= 0:
            new_idx = generate_random_tree(csvWriter, dotFile, new_idx, idx + i, depth + 1, max_children, max_depth)

    return new_idx

def random_Tree(dotFile, csvFile):
    csvWriter = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    ranks = MIN_RANKS + (rand() % (MAX_RANKS - MIN_RANKS + 1))

    csvWriter.writerow(("parent", "child"))
    dotFile.write("digraph {\n")

    random.seed(0)
    generate_random_tree(csvWriter, dotFile, max_depth=ranks)

    print()
    dotFile.write("}\n")


def random_DAG(dotFile, csvFile):
    csvWriter = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    nodes = 1
    ranks = MIN_RANKS + (rand() % (MAX_RANKS - MIN_RANKS + 1))

    csvWriter.writerow(("parent", "child"))
    dotFile.write("digraph {\n")

    new_nodes = random_number_of_nodes()

    csvWriter.writerow((-1, 0, random_generator(10)))
    dotFile.write(" %d -> %d;\n" % (-1, 0))
    for k in range(0, new_nodes):
        csvWriter.writerow((-1, k + nodes, random_generator(10)))
        dotFile.write(" %d -> %d;\n" % (-1, k + nodes))

    for i in range(0, ranks):
        new_nodes = MIN_PER_RANK + (rand() % (MAX_PER_RANK - MIN_PER_RANK + 1))
        for j in range(0, nodes):
            for k in range(0, new_nodes):
                if ((rand() % 100) < PERCENT):
                    csvWriter.writerow((j, k + nodes, random_generator(10)))
                    dotFile.write(" %d -> %d;\n" % (j, k + nodes))
        nodes += new_nodes
    print()
    dotFile.write("}\n")

def main():

    with open("./out.dot", 'w') as dotFile, open('./data.csv', 'w', newline='') as csvFile:
        random_Tree(dotFile, csvFile)

    print("Done")

if __name__ == "__main__":
    main()