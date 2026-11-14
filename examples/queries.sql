-- These names are served by src/test/java/farm/query/vgicalcite/FixtureWorker.java.
SELECT id, label FROM vgi.data.numbers WHERE id >= 4 ORDER BY id DESC;
SELECT COUNT(*), SUM(id) FROM TABLE(vgi.main.sequence(17));
SELECT * FROM TABLE(vgi.main.echo(CAST(NULL AS BIGINT), 7));
SELECT COUNT(*) FROM TABLE(vgi.main.sequence("count" => 7, "batch_size" => 2));
SELECT COUNT(*) FROM vgi.data.numbers a JOIN vgi.data.numbers b ON a.id + 1 = b.id;
EXPLAIN PLAN FOR SELECT label FROM vgi.data.numbers WHERE id >= 4;
