
library(GreenplumR)

# feature list to test:
#   one column / multiple columns
#   output.signature is NULL/list/function/ other values(invalid)
#   data type in output.signature is not supported by GPDB
#   output.name is NULL
#   output.name is NOT NULL, the table does not exist
#   output.name is NOT NULL, the table does exist
#   clear.existing is TRUE/FALSE/ other values
#   case.sensitive is TRUE/FALSE or other values
#   output.distributeOn is random/...
#   input data.frame is NULL or does not a db.data.frame
#   FUN is NULL
#   FUN is not NULL, but is not a function
#   FUN is anonymous function
#   FUN that references out-world environment
#   language must be 'plr' or 'plcontainer'


env <- new.env(parent = globalenv())

.dbname = "d_apply"
.port = 15432
## connection ID
cid <- db.connect(port = .port, dbname = .dbname, verbose = FALSE)
db.q("CREATE EXTENSION plr;")
dat <- db.data.frame('test_apply')

sqrtFUN <- function(x){
    return(x[2] + 1)
}

res = db.gpapply(dat, output.name = "temp_result_1", FUN = sqrtFUN, output.signature = list("length" = "float"),
            clear.existing = TRUE, case.sensitive = FALSE, output.distributeOn = NULL, language = "plr")

print(res)

# 

db.disconnect(cid)

WITH gpdbtmpa AS (
    SELECT (gprfunc_LRKJN5307C(\"height\")) AS gpdbtmpb 
    FROM (SELECT \"height\" FROM \"one_Col_Table\") tmptbl
) SELECT (gpdbtmpb::gptype_LRKJN5307C).* FROM gpdbtmpa ;

WITH gpdbtmpa AS (
    SELECT (gprfunc_LRKJN5307C(height)) AS gpdbtmpb 
    FROM (SELECT height FROM "one_Col_Table") tmptbl
) SELECT (gpdbtmpb::gptype_LRKJN5307C).* FROM gpdbtmpa;






#####TEMPP 


.dbname = "d_tapply"
.port = 15432
## connection ID
cid <- db.connect(port = .port, dbname = .dbname, verbose = FALSE)