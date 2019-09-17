-- prepare data table: test_apply
DROP TABLE IF EXISTS test_apply;
CREATE TABLE test_apply AS
--SELECT id, length from abalone2 limit 10;
SELECT id , round(random()::numeric, 2) AS length FROM generate_series(1,100000000) id;

-- create type
DROP TYPE IF EXISTS gptype_apply CASCADE;
CREATE TYPE gptype_apply AS (
 length float
);

-- PLR
CREATE OR REPLACE FUNCTION gprfunc_apply (id int4, length float8) 
RETURNS SETOF gptype_apply AS $$
 # container: plc_r_shared
gplocalf <- function (x)
{
    # x is a data.frame
    # plcontainer only works when x[2]
    # plr both works when x[2], x[1,2]
    return(x[2]+nrow(x))
}
 df <- data.frame(id=id, length=length)
return(do.call(gplocalf, list(df)))
 $$ LANGUAGE 'plr';

-- Execute
DROP TABLE IF EXISTS result_apply;
CREATE TABLE result_apply AS 
WITH gpdbtmpa AS (
    SELECT (gprfunc_apply(id, length)) AS gpdbtmpb FROM (SELECT id, length FROM test_apply) tmptbl
)
SELECT (gpdbtmpb::gptype_apply).* FROM gpdbtmpa;
select * from result_apply;


----------------
-- prepare data table: test_apply
DROP TABLE IF EXISTS test_apply;
CREATE TABLE test_apply(id int, name text);
INSERT INTO test_apply VALUES
(1, 'A'), (2, 'B'), (3, 'C');

-- create type
DROP TYPE IF EXISTS gptype_apply CASCADE;
CREATE TYPE gptype_apply AS (
 result text
);

-- create function for PL/container
CREATE OR REPLACE FUNCTION gprfunc_apply (id int, name text) 
RETURNS SETOF gptype_apply AS $$
# container: plc_r_shared
gplocalf <- function (x)
{
#    if (is.data.frame(x)) {
#        return (paste("data.frame, length=", nrow(x)))
#    }
#    if (is.list(x)) {
#        return (paste("list, len=", length(x)))
#    }
#    return(paste("<", toString(x), ">"))
    deparse(x)
    x[2]
}
df <- data.frame(id=id, name=name)
return(do.call(gplocalf, list(df)))
$$ LANGUAGE 'plr';

-- PLR

-- Execute
DROP TABLE IF EXISTS result_apply;
CREATE TABLE result_apply AS 
WITH gpdbtmpa AS (
    SELECT (gprfunc_apply(id, name)) AS gpdbtmpb FROM (SELECT id, name FROM test_apply) tmptbl
)
SELECT (gpdbtmpb::gptype_apply).* FROM gpdbtmpa;
select * from result_apply;












n = c(2, 3, 5) 
s = c("aa", "bb", "cc") 
b = c(TRUE, FALSE, TRUE) 
df = data.frame(n, s, b) 


-- array_agg
CREATE OR REPLACE FUNCTION gprfunc_apply (id int4[], length float8[]) 
RETURNS SETOF gptype_apply AS $$
 # container: plc_r_shared
gplocalf <- function (x)
{
    # x is a data.frame
    # plcontainer only works when x[2]
    # plr both works when x[2], x[1,2]
    # return(x[2]+nrow(x))
    return (x[2]+nrow(x))
}
 df <- data.frame(id=id, length=length)
return(do.call(gplocalf, list(df)))
 $$ LANGUAGE 'plr';

-- Execute
DROP TABLE IF EXISTS result_apply;
CREATE TABLE result_apply AS
WITH tempa AS (
    SELECT id, length, row_number() over() FROM test_apply
) 
, gpdbtmpa AS (
    SELECT (gprfunc_apply(array_agg(id), array_agg(length))) AS gpdbtmpb FROM tempa GROUP BY row_number/1024
)
SELECT (gpdbtmpb::gptype_apply).* FROM gpdbtmpa;
select * from result_apply;


-- function return a scalar value, not a table

DROP TYPE IF EXISTS gptype_apply CASCADE;
CREATE TYPE gptype_apply AS (
 result float
);
-- PLR
CREATE OR REPLACE FUNCTION gprfunc_apply (id int4, length float8) 
RETURNS gptype_apply AS $$
 # container: plc_r_shared
gplocalf <- function (x)
{
    # x is a list, a row value
    # plcontainer only works when x[2]
    # plr both works when x[2], x[1,2]
    x$length <- x$length + 1
    return(x$length)
}
 df <- data.frame(id=c(id), length=c(length))
return(do.call(gplocalf, list(df)))
 $$ LANGUAGE 'plr';

-- Execute
DROP TABLE IF EXISTS result_apply;
CREATE TABLE result_apply AS 
WITH gpdbtmpa(func_apply) AS (
    SELECT gprfunc_apply(id, length) FROM  test_apply limit 4
)
SELECT (func_apply).* FROM gpdbtmpa;
select * from result_apply;

SELECT (gprfunc_apply(id, length)::gptype_apply).* FROM  test_apply limit 4