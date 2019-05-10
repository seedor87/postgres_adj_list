

DROP TABLE IF EXISTS task;
CREATE TABLE task (
	parent 	INT,
	node  	INT,
	data   	TEXT
);

TRUNCATE task;
COPY task(parent, node, data)
FROM 'C:\Users\rseedorf\PycharmProjects\postgres_adj_list\data.csv' DELIMITER ',' CSV HEADER;

--INSERT INTO task (parent, node, data)
--VALUES (305, -1, 'AAAAAAAAAA');

--DELETE FROM task
--WHERE parent = 305 AND node = -1;

SELECT * FROM task;

SELECT count(*) FROM get_all_children(0);

SELECT * FROM parent_first_full_graph_traversal();
SELECT * FROM child_first_full_graph_traversal();

-------------------------------
-- result set of all children
DROP FUNCTION IF EXISTS get_all_children(input_id INT);
CREATE OR REPLACE FUNCTION get_all_children (input_id INT)
	 RETURNS TABLE (
	 child INT
) AS $$
BEGIN RETURN QUERY
WITH RECURSIVE traverse(node) AS (
	SELECT task.node FROM task
	WHERE task.parent = input_id
    UNION ALL
	SELECT task.node
	FROM task
	INNER JOIN traverse
	ON task.parent = traverse.node
)
SELECT DISTINCT traverse.node FROM traverse;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM get_all_children(0);


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

SELECT * FROM get_all_parents(199);


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

SELECT * FROM get_all_children_depth(0);

-- get all children within 2 generations
SELECT * FROM get_all_children_depth(0)
WHERE depth <= 4;


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

SELECT * FROM get_all_parents_depth(48);


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
	 path LTREE
	 --path INT[]
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, cycle) AS (
        SELECT task.node, 
		text2ltree(task.node::TEXT),
		--ARRAY[task.node], 
		false 
	FROM task
        LEFT OUTER JOIN task AS t2
        ON task.node = t2.parent
        WHERE t2.parent IS NULL
    UNION ALL
        SELECT DISTINCT task.parent,
               traverse.path || task.parent::TEXT,
               text2ltree(task.parent::TEXT) @> traverse.path
               --task.parent = ANY(traverse.path)
        FROM traverse
        INNER JOIN task
        ON task.node = traverse.node
        WHERE NOT cycle
)
SELECT traverse.node, traverse.path FROM traverse
LEFT OUTER JOIN traverse AS any_cycles ON any_cycles.cycle = true
WHERE any_cycles.cycle IS NULL
GROUP BY traverse.node, traverse.path
ORDER BY MAX(nlevel(traverse.path));
--ORDER BY MAX(array_length(traverse.path, 1));
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM parent_first_full_graph_traversal();

-- get all distinct progeny given a parent
SELECT DISTINCT progeny
FROM parent_first_full_graph_traversal(), LATERAL unnest(path) progeny
WHERE parent = 4


-- test creating a view to work with more quickly
DROP VIEW IF EXISTS parent_first_full_graph_traversal;
CREATE MATERIALIZED VIEW parent_first_full_graph_traversal AS
SELECT 
	ltree2text(subltree(path, nlevel(path)-1, nlevel(path)))::INT AS parent,
	path,
	ltree2text(subltree(path, 0, 1))::INT AS child
FROM parent_first_full_graph_traversal()
WITH NO DATA;

REFRESH MATERIALIZED VIEW parent_first_full_graph_traversal;

SELECT child, path, parent
FROM parent_first_full_graph_traversal;

-------------------------------
-- Child First Full Graph Traversal
DROP FUNCTION IF EXISTS child_first_full_graph_traversal() CASCADE;
CREATE OR REPLACE FUNCTION child_first_full_graph_traversal ()
	 RETURNS TABLE (
	 child INT,
	 path LTREE
	 --path INT[]
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, cycle) AS (
        SELECT 
		task.parent, 
		text2ltree(task.parent::TEXT),
		--ARRAY[task.parent], 
		false 
	FROM task
        LEFT OUTER JOIN task AS t2
        ON task.parent = t2.node
        WHERE t2.node IS NULL
    UNION ALL
        SELECT DISTINCT task.node,
               traverse.path || task.node::TEXT,
               text2ltree(task.node::TEXT) @> traverse.path
               --task.node = ANY(traverse.path)
        FROM traverse
        INNER JOIN task
        ON task.parent = traverse.node
        WHERE NOT cycle
)
SELECT traverse.node, traverse.path 
FROM traverse
LEFT OUTER JOIN traverse AS any_cycles 
ON any_cycles.cycle = true
WHERE any_cycles.cycle IS NULL
GROUP BY traverse.node, traverse.path
ORDER BY MAX(nlevel(traverse.path));
--ORDER BY MAX(array_length(traverse.path, 1));
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM child_first_full_graph_traversal();

-- get all ancestors with ancestry given descendent
SELECT DISTINCT child, ancestor, path AS ancestry, dist
FROM child_first_full_graph_traversal() as t
LEFT JOIN LATERAL 
unnest(string_to_array(path::TEXT, '.')) -- for ltree
--unnest(path) -- For pg array
WITH ORDINALITY AS a(ancestor, dist) ON TRUE
WHERE child = 48 
AND child <> ancestor::INT
ORDER BY dist;


-- test creating a view to work with more quickly
DROP MATERIALIZED VIEW IF EXISTS child_first_full_graph_traversal;
CREATE MATERIALIZED VIEW child_first_full_graph_traversal AS
SELECT 
	getLast(path)::INT AS child,
	path,
	ltree2text(subltree(path, 0, 1))::INT AS parent
FROM child_first_full_graph_traversal()
WITH NO DATA;

REFRESH MATERIALIZED VIEW child_first_full_graph_traversal;

SELECT DISTINCT child, subltree(path, index(path, '370'::LTREE), nlevel(path))
FROM child_first_full_graph_traversal
WHERE child = 374 
--AND contains(path, 370)
AND path <@ toPath(0,2,3,370)

-------------------------------
-- Child to Parent Traversal
DROP FUNCTION IF EXISTS child_to_parent_traversal(INT, INT);
CREATE OR REPLACE FUNCTION child_to_parent_traversal (c INT, p INT)
	 RETURNS TABLE (
	 parent INT,
	 path LTREE
	 --path INT[]
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, cycle) AS (
	SELECT task.node, 
		text2ltree(task.node::TEXT),
		--ARRAY[task.node], 
		false 
	FROM task
        LEFT OUTER JOIN task AS t2
        ON task.node = t2.parent
        WHERE task.node = c -- start
    UNION ALL
        SELECT DISTINCT task.parent,
               traverse.path || task.parent::TEXT,
	       text2ltree(task.parent::TEXT) @> traverse.path
               --task.parent = ANY(traverse.path)
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
ORDER BY MAX(nlevel(traverse.path));
--ORDER BY MAX(array_length(traverse.path, 1));
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM child_to_parent_traversal(467, 0);

-- empty set confirms that there is no relation from child to parent
SELECT * FROM child_to_parent_traversal(28, 7); -- ie cant get from 5 to 1

-------------------------------
-- Parent to Child Traversal
DROP FUNCTION IF EXISTS parent_to_child_traversal(INT, INT);
CREATE OR REPLACE FUNCTION parent_to_child_traversal (p INT, c INT)
	 RETURNS TABLE (
	 child INT,
	 path LTREE
	 --path INT[]
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, cycle) AS (
        SELECT task.parent, 
		text2ltree(task.parent::TEXT),
		--ARRAY[task.parent], 
		false 
	FROM task
        LEFT OUTER JOIN task AS t2
        ON task.node = t2.parent
        WHERE task.parent = p -- start
    UNION ALL
        SELECT DISTINCT task.node,
               traverse.path || task.node::TEXT,
               text2ltree(task.node::TEXT) @> traverse.path
               --task.node = ANY(traverse.path)
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
ORDER BY MAX(nlevel(traverse.path));
--ORDER BY MAX(array_length(traverse.path, 1));
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM parent_to_child_traversal(0,36);

-- test to make sure that the closure of plaths from A to B is the same as B to A
SELECT * FROM parent_to_child_traversal(0,36)
UNION ALL
SELECT * FROM child_to_parent_traversal(36,0);


-------------------------------
-- get all parents within n generations
DROP FUNCTION IF EXISTS all_parents_within_n(INT, INT);
CREATE OR REPLACE FUNCTION all_parents_within_n (c INT, n INT)
	 RETURNS TABLE (
	 parent INT,
	 path LTREE,
	 --path INT[],
	 dist INT
)
AS $$
BEGIN
RETURN QUERY
WITH RECURSIVE traverse(node, path, dist) AS (
        SELECT task.node, 
		text2ltree(task.node::TEXT),
		--ARRAY[task.node], 
		0 
	FROM task
        LEFT OUTER JOIN task AS t2
        ON task.node = t2.parent
        WHERE task.node = c
    UNION ALL
        SELECT
	  task.parent,
	  traverse.path || task.parent::TEXT,
	  traverse.dist + 1
        FROM traverse
        INNER JOIN task
        ON task.node = traverse.node
        WHERE traverse.dist < n
)
SELECT traverse.node, traverse.path, traverse.dist FROM traverse
GROUP BY traverse.node, traverse.path, traverse.dist
ORDER BY traverse.dist;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM all_parents_within_n(106, 3);


-------------------------------
-- get all children within n generations
DROP FUNCTION IF EXISTS all_children_within_n(INT, INT);
CREATE OR REPLACE FUNCTION all_children_within_n(p INT, n INT)
	RETURNS TABLE (
	child INT,
	path LTREE,
	 --path INT[],
	dist INT
)
AS $$
BEGIN RETURN QUERY
WITH RECURSIVE traverse(node, path, dist) AS (
	SELECT task.node, 
		text2ltree(task.node::TEXT),
		--ARRAY[task.node], 		
		0 
	FROM task
	LEFT OUTER JOIN task AS t2
	ON task.parent = t2.node
	WHERE task.node = p
   UNION ALL
	SELECT
          task.node,
	  traverse.path || task.node::TEXT,
	  traverse.dist + 1
        FROM traverse
        INNER JOIN task
        ON task.parent = traverse.node
        WHERE traverse.dist < n
)
SELECT traverse.node, traverse.path, traverse.dist FROM traverse
GROUP BY traverse.node, traverse.path, traverse.dist
ORDER BY traverse.dist;
END; $$
LANGUAGE 'plpgsql';

SELECT * FROM all_children_within_n(6, 2);

-- test to make sure that the closure of paths children and parents is complete
SELECT * FROM all_children_within_n(6,2)
UNION ALL
SELECT * FROM all_parents_within_n(6,2);

-- get all children of 6 within 3 generations that are at least 1 gen away
SELECT * FROM all_children_within_n(3, 3)
EXCEPT
SELECT * FROM all_children_within_n(3, 1);














---
