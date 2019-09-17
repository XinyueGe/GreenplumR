-- prepare data table: test_apply
DROP TABLE IF EXISTS test_apply;
CREATE TABLE test_apply AS
SELECT id , round(random()::numeric, 2) AS length FROM generate_series(1,10) id;

-- create type
DROP TYPE IF EXISTS gptype_apply CASCADE;
CREATE TYPE gptype_apply AS (
 id int,
 length float
);

-- PLR
CREATE OR REPLACE FUNCTION gprfunc_apply (id int4, length float8) 
RETURNS SETOF gptype_apply AS $$
 # container: plc_r_shared
gplocalf <- function (x)
{
    # x is a row value
    # plcontainer only works when x[2]
    # plr both works when x[2], x[1,2]
    x$length <- x$length + 1
    return(x)
}
df <- data.frame(id=id, length=length)
return(do.call(gplocalf, list(df)))
$$ LANGUAGE 'plr';

-- Execute
DROP TABLE IF EXISTS result_apply;
CREATE TABLE result_apply AS 
-- WITH gpdbtmpa AS (
--     SELECT (gprfunc_apply1(id, length)) AS gpdbtmpb FROM (SELECT id, length FROM test_apply1) tmptbl
-- )
-- SELECT (gpdbtmpb::gptype_apply1).* FROM gpdbtmpa;
SELECT (gprfunc_apply(id, length)::gptype_apply).* FROM test_apply;
select * from result_apply;
