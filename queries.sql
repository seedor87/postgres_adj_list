SET SEARCH_PATH="atrt_common";

DROP TABLE IF EXISTS task;
CREATE TABLE task (
	parent 	INT,
	node  	INT,
	data   	TEXT
);

COPY task(parent, node, data)
FROM 'C:\Users\rseedorf\PycharmProjects\postgres_adj_list\data.csv' DELIMITER ',' CSV HEADER;

-- INSERT INTO task VALUES 		--
-- (NULL, 5, MD5(random()::text)),	--	  (5)
-- (5, 3, MD5(random()::text)),		--	   |
-- (3, 1, MD5(random()::text)),		--        (3)
-- (3, 8, MD5(random()::text)),		--       /   \
-- (1, 2, MD5(random()::text)),		--     (1)   (8)
-- (1, 4, MD5(random()::text)),		--     /  \ / | \
-- (8, 4, MD5(random()::text)),		--   (2)  (4) |  (6)
-- (8, 9, MD5(random()::Text)),		--	  / \ | /
-- (8, 6, MD5(random()::text)),		--	(7)  (9)
-- (4, 7, MD5(random()::text)),		--            |
-- (4, 9, MD5(random()::text)),		--	     (0)
-- (6, 9, MD5(random()::text)),		--
-- (9, 0, MD5(random()::text));		--

SELECT * FROM task

-------------------------------
-- result set of all children
DROP FUNCTION IF EXISTS get_all_children(input_id INT);
CREATE OR REPLACE FUNCTION get_all_children (input_id INT)
	 RETURNS TABLE (
	 child INT
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node) AS (
	SELECT task.node FROM task
	WHERE task.parent = input_id
    UNION ALL
	SELECT task.node FROM task
	INNER JOIN traverse
	ON task.parent = traverse.node
)
SELECT DISTINCT traverse.node FROM traverse;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM get_all_children(5);


-------------------------------
-- result set of all parents
DROP FUNCTION IF EXISTS get_all_parents(input_id INT);
CREATE OR REPLACE FUNCTION get_all_parents (input_id INT)
	 RETURNS TABLE (
	 parent INT
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(parent) AS (
	SELECT task.parent FROM task
	WHERE task.node = input_id
    UNION ALL
	SELECT task.parent FROM task
	INNER JOIN traverse
	ON task.node = traverse.parent
)
SELECT DISTINCT traverse.parent FROM traverse;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM get_all_parents(5);


-------------------------------
-- result set of children with their depth
DROP FUNCTION IF EXISTS get_all_children_depth(input_id INT);
CREATE OR REPLACE FUNCTION get_all_children_depth (input_id INT)
	 RETURNS TABLE (
	 child INT,
	 depth INT
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, depth) AS (
        SELECT task.node, 1 FROM task
        WHERE task.parent = input_id
    UNION ALL
        SELECT task.node, traverse.depth + 1 FROM task
        INNER JOIN traverse
        ON task.parent = traverse.node
)
SELECT DISTINCT traverse.node, traverse.depth FROM traverse
ORDER BY traverse.depth ASC;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM get_all_children_depth(-1);

-- get all children within 2 generations
SELECT * FROM get_all_children_depth(6)
WHERE depth <= 2;


-------------------------------
-- result set of all ancestors with their depth
DROP FUNCTION IF EXISTS get_all_parents_depth(input_id INT);
CREATE OR REPLACE FUNCTION get_all_parents_depth (input_id INT)
	 RETURNS TABLE (
	 parent INT,
	 depth INT
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(parent, depth) AS (
        SELECT task.parent, 1 FROM task
        WHERE task.node = input_id
    UNION ALL
        SELECT task.parent, traverse.depth + 1 FROM task
        INNER JOIN traverse
        ON task.node = traverse.parent
)
SELECT DISTINCT traverse.parent, traverse.depth FROM traverse
ORDER BY traverse.depth ASC;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM get_all_parents_depth(13);


-------------------------------
-- sorted result set that has children with lowest count of children first
DROP FUNCTION IF EXISTS sort_lowest_children_first();
CREATE OR REPLACE FUNCTION sort_lowest_children_first ()
	 RETURNS TABLE (
	 node INT,
	 count_children INT
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, count) AS (
        SELECT task.node, 0 FROM task
        LEFT OUTER JOIN task as t2
        ON task.node = t2.parent
        WHERE t2.parent IS NULL
    UNION ALL
        SELECT task.parent AS node, traverse.count + 1 FROM task
        INNER JOIN traverse
        ON task.node = traverse.node
        WHERE task.parent IS NOT NULL
)
SELECT traverse.node, traverse.count FROM traverse
GROUP BY traverse.node, traverse.count
ORDER BY MAX(traverse.count) ASC;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM sort_lowest_children_first();

-- How to get parents with most children
SELECT node, max(count_children) most_children FROM sort_lowest_children_first()
GROUP BY node
ORDER BY most_children DESC

-------------------------------
-- Parent First Full Graph Traversal
DROP FUNCTION IF EXISTS parent_first_full_graph_traversal();
CREATE OR REPLACE FUNCTION parent_first_full_graph_traversal ()
	 RETURNS TABLE (
	 parent INT,
	 path INT[]
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, cycle) AS (
        SELECT task.node, ARRAY[task.node], false FROM task
        LEFT OUTER JOIN task AS t2
        ON task.node = t2.parent
        WHERE t2.parent IS NULL
    UNION ALL
        SELECT DISTINCT task.parent,
               traverse.path || task.parent,
               task.parent = ANY(traverse.path)
        FROM traverse
        INNER JOIN task
        ON task.node = traverse.node
        WHERE NOT cycle
)
SELECT traverse.node, traverse.path FROM traverse
LEFT OUTER JOIN traverse AS any_cycles ON any_cycles.cycle = true
WHERE any_cycles.cycle IS NULL
GROUP BY traverse.node, traverse.path
ORDER BY MAX(array_length(traverse.path, 1));
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM parent_first_full_graph_traversal();

-- get all distinct progeny given a parent
SELECT DISTINCT progeny
FROM parent_first_full_graph_traversal(), LATERAL unnest(path) progeny
WHERE parent = 4


-------------------------------
-- Child First Full Graph Traversal
DROP FUNCTION IF EXISTS child_first_full_graph_traversal();
CREATE OR REPLACE FUNCTION child_first_full_graph_traversal ()
	 RETURNS TABLE (
	 child INT,
	 path INT[]
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, cycle) AS (
        SELECT task.node, ARRAY[task.node], false FROM task
        LEFT OUTER JOIN task AS t2
        ON task.parent = t2.node
        WHERE t2.node IS NULL
    UNION ALL
        SELECT DISTINCT task.node,
               traverse.path || task.node,
               task.node = ANY(traverse.path)
        FROM traverse
        INNER JOIN task
        ON task.parent = traverse.node
        WHERE NOT cycle
)
SELECT traverse.node, traverse.path FROM traverse
LEFT OUTER JOIN traverse AS any_cycles ON any_cycles.cycle = true
WHERE any_cycles.cycle IS NULL
GROUP BY traverse.node, traverse.path
ORDER BY MAX(array_length(traverse.path, 1));
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM child_first_full_graph_traversal();

-- get all distinct ancestors given descendent
SELECT DISTINCT ancestor
FROM child_first_full_graph_traversal(), LATERAL unnest(path) ancestor
WHERE child = 4

-------------------------------
-- Child to Parent Traversal
DROP FUNCTION IF EXISTS child_to_parent_traversal(INT, INT);
CREATE OR REPLACE FUNCTION child_to_parent_traversal (c INT, p INT)
	 RETURNS TABLE (
	 parent INT,
	 path INT[]
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, cycle) AS (
        SELECT task.node, ARRAY[task.node], false FROM task
        LEFT OUTER JOIN task AS t2
        ON task.node = t2.parent
        WHERE task.node = c -- start
    UNION ALL
        SELECT DISTINCT task.parent,
               traverse.path || task.parent,
               task.parent = ANY(traverse.path)
        FROM traverse
        INNER JOIN task
        ON task.node = traverse.node
        WHERE NOT cycle
)
SELECT traverse.node, traverse.path FROM traverse
LEFT OUTER JOIN traverse AS any_cycles ON any_cycles.cycle = true
WHERE any_cycles.cycle IS NULL
AND traverse.node = p -- end
GROUP BY traverse.node, traverse.path
ORDER BY MAX(array_length(traverse.path, 1));
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM child_to_parent_traversal(13, 1);

-- empty set confirms that there is no relation from child to parent
SELECT * FROM child_to_parent_traversal(5, 1); -- ie cant get from 5 to 1

-------------------------------
-- Parent to Child Traversal
DROP FUNCTION IF EXISTS parent_to_child_traversal(INT, INT);
CREATE OR REPLACE FUNCTION parent_to_child_traversal (p INT, c INT)
	 RETURNS TABLE (
	 child INT,
	 path INT[]
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, cycle) AS (
        SELECT task.node, ARRAY[task.node], false FROM task
        LEFT OUTER JOIN task AS t2
        ON task.parent = t2.node
        WHERE task.node = p -- start
    UNION ALL
        SELECT DISTINCT task.node,
               traverse.path || task.node,
               task.node = ANY(traverse.path)
        FROM traverse
        INNER JOIN task
        ON task.parent = traverse.node
        WHERE NOT cycle
)
SELECT traverse.node, traverse.path FROM traverse
LEFT OUTER JOIN traverse AS any_cycles ON any_cycles.cycle = true
WHERE any_cycles.cycle IS NULL
AND traverse.node = c -- end
GROUP BY traverse.node, traverse.path
ORDER BY MAX(array_length(traverse.path, 1));
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM parent_to_child_traversal(-1, 15);

-- test to make sure that the closure of plaths from A to B is the same as B to A
SELECT * FROM parent_to_child_traversal(3,14)
UNION ALL
SELECT * FROM child_to_parent_traversal(14,3);


-------------------------------
-- get all parents within n generations
DROP FUNCTION IF EXISTS all_parents_within_n(INT, INT);
CREATE OR REPLACE FUNCTION all_parents_within_n (c INT, n INT)
	 RETURNS TABLE (
	 parent INT,
	 path INT[],
	 dist INT
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, dist) AS (
        SELECT task.node, ARRAY[task.node], 0 FROM task
        LEFT OUTER JOIN task AS t2
        ON task.node = t2.parent
        WHERE task.node = c
    UNION ALL
        SELECT
	  task.parent,
	  traverse.path || task.parent,
	  traverse.dist + 1
        FROM traverse
        INNER JOIN task
        ON task.node = traverse.node
        WHERE traverse.dist < n
)
SELECT traverse.node, traverse.path, traverse.dist FROM traverse
GROUP BY traverse.node, traverse.path, traverse.dist;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM all_parents_within_n(6, 2);


-------------------------------
-- get all children within n generations
DROP FUNCTION IF EXISTS all_children_within_n(INT, INT);
CREATE OR REPLACE FUNCTION all_children_within_n(p INT, n INT)
	RETURNS TABLE (
	child INT,
	path INT[],
	dist INT
)
AS $$
BEGIN RETURN QUERY
WITH RECURSIVE traverse(node, path, dist) AS (
	SELECT task.node, ARRAY[task.node], 0 FROM task
	LEFT OUTER JOIN task AS t2
	ON task.parent = t2.node
	WHERE task.node = p
   UNION ALL
	SELECT
          task.node,
	  traverse.path || task.node,
	  traverse.dist + 1
        FROM traverse
        INNER JOIN task
        ON task.parent = traverse.node
        WHERE traverse.dist < n
)
SELECT traverse.node, traverse.path, traverse.dist FROM traverse
GROUP BY traverse.node, traverse.path, traverse.dist;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM all_children_within_n(6, 2);

-- test to make sure that the closure of paths children and parents is complete
SELECT * FROM all_children_within_n(6,2)
UNION ALL
SELECT * FROM all_parents_within_n(6,2);

-- get all children of 6 within 3 generations that are at least 1 gen away
SELECT * FROM all_children_within_n(6, 3)
EXCEPT
SELECT * FROM all_children_within_n(6, 1);