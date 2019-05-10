

-- easy to path for random array of nodes
DROP FUNCTION IF EXISTS toPath(VARIADIC TEXT[]);
CREATE FUNCTION toPath(VARIADIC TEXT[])
  RETURNS LTREE AS
$func$
   SELECT ARRAY_TO_STRING($1,'.')::LTREE
$func$ LANGUAGE sql;

DROP FUNCTION IF EXISTS toPath(VARIADIC INT[]);
CREATE FUNCTION toPath(VARIADIC INT[])
  RETURNS LTREE AS
$func$
   SELECT ARRAY_TO_STRING($1,'.')::LTREE
$func$ LANGUAGE sql;

SELECT toPath('0', '2', '3', '370');
SELECT toPath(VARIADIC ARRAY[0,2,3,370]);


-- easy to array from an ltree input
DROP FUNCTION IF EXISTS toArray(LTREE);
CREATE FUNCTION toArray(LTREE)
  RETURNS TEXT[] AS
$func$
   SELECT STRING_TO_ARRAY($1::TEXT,'.')
$func$ LANGUAGE sql;

SELECT toArray('0.2.3.370');
SELECT toArray('');


-- easy contains for path search
DROP FUNCTION IF EXISTS contains(LTREE, TEXT);
CREATE FUNCTION contains(path LTREE, node TEXT)
	RETURNS BOOLEAN AS
$func$
	SELECT path ~ CONCAT('*.', node, '.*')::LQUERY
$func$ LANGUAGE sql;

DROP FUNCTION IF EXISTS contains(LTREE, INT);
CREATE FUNCTION contains(path LTREE, node INT)
	RETURNS BOOLEAN AS
$func$
	SELECT path ~ CONCAT('*.', node, '.*')::LQUERY
$func$ LANGUAGE sql;

SELECT contains('0.2.3.370', 370);
SELECT contains(toPath(0,2,3,370), '4');


-- easy way to get final node on path
DROP FUNCTION IF EXISTS getLast(LTREE);
CREATE FUNCTION getLast(LTREE)
	RETURNS TEXT AS
$func$
	SELECT ltree2text(subltree($1, nlevel($1)-1, nlevel($1)))
$func$	LANGUAGE sql;

SELECT getLast('0.2.3.370')
SELECT getLast('0')

