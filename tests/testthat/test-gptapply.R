context("Test matrix of gptapply")

## ----------------------------------------------------------------------
## Test preparations

# Need valid 'pivotalr_port' and 'pivotalr_dbname' values
env <- new.env(parent = globalenv())
#.dbname = get('pivotalr_dbname', envir=env)
#.port = get('pivotalr_port', envir=env)
.verbose <- FALSE

.host <- '172.17.0.1'
.dbname <- "d_apply"
.port <- 15432
.language <- 'plr'
## connection ID
cid <- db.connect(host = .host, port = .port, dbname = .dbname, verbose = .verbose)
.nrow.test <- 10

dat <- abalone[c(1:.nrow.test), ]

fn <- function(X)
{
    return (X)
}
# -----------------------------------------------------------
#
# -----------------------------------------------------------
test_that("Test gpt", {
    .output.name <- 'resultGPTapply'
    test_that::skip_on_cran()
    db.gptapply(dat, INDEX = 'sex', FUN = output.name = .output.name)
})












db.disconnect(cid)