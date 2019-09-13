context("Test matrix of gptapply")

## ----------------------------------------------------------------------
## Test preparations

# Need valid 'pivotalr_port' and 'pivotalr_dbname' values
env <- new.env(parent = globalenv())
#.dbname = get('pivotalr_dbname', envir=env)
#.port = get('pivotalr_port', envir=env)
.verbose <- TRUE

.host <- '172.17.0.1'
.host <- 'localhost'
.dbname <- "d_tapply"
.port <- 15432
.language <- 'plr'
## connection ID
cid <- db.connect(host = .host, port = .port, dbname = .dbname, verbose = .verbose)
.nrow.test <- 10

dat <- abalone[c(1:.nrow.test), ]

tname.1.col <- 'one_Col_Table'
tname.mul.col <- 'mul_Col_Table'
db.q('DROP SCHEMA IF EXISTS test_Schema CASCADE;', verbose = .verbose)
db.q('DROP SCHEMA IF EXISTS "test_Schema" CASCADE;', verbose = .verbose)
db.q(paste('DROP TABLE IF EXISTS "', tname.1.col, '";', sep = ''), verbose = .verbose)
db.q(paste('DROP TABLE IF EXISTS "', tname.mul.col, '";', sep = ''), verbose = .verbose)

# prepare test table
.dat.1 <- as.data.frame(dat$rings)
names(.dat.1) <- c('Rings')
dat.1 <- as.db.data.frame(.dat.1, table.name = tname.1.col, verbose = .verbose)
dat.mul <- as.db.data.frame(dat, table.name = tname.mul.col, verbose = .verbose)


# ---------------------------------------------------------------
# prepare data
# ---------------------------------------------------------------
test_that("Test prepare", {
    testthat::skip_on_cran()
    expect_equal(is.db.data.frame(dat.1), TRUE)
    expect_equal(is.db.data.frame(dat.mul), TRUE)
    expect_equal(nrow(dat.1), .nrow.test)
    expect_equal(ncol(dat.1), 1)
    expect_equal(nrow(dat.mul), .nrow.test)
    expect_equal(ncol(dat.mul), ncol(dat))

    expect_equal(db.existsObject(tname.1.col, conn.id = cid), TRUE)
    expect_equal(db.existsObject(tname.mul.col, conn.id = cid), TRUE)

    res <- db.q("CREATE SCHEMA test_Schema", verbose = .verbose)
    expect_equal(res, NULL)
    res <- db.q("SELECT nspname FROM pg_namespace WHERE nspname = 'test_schema';",
                verbose = .verbose)
    expect_equal(is.data.frame(res), TRUE)
    expect_equal(nrow(res), 1)
})

# test table has only one column
dat.test <- dat.1
.signature <- list("Rings" = "int")
.index <- "Rings"
fn.inc <- function(x)
{
    return (x[1] + 1)
}
# -----------------------------------------------------------
# ONE COLUMN TABLE
# -----------------------------------------------------------
# test_that("Test gpt", {
#     .output.name <- 'resultGPTapply'
#     test_that::skip_on_cran()
#     res <- db.gptapply(dat, INDEX = 'sex', FUN = fn, output.name = .output.name,
# 		output.signature = .signature, clear.existing = TRUE,
# 		case.sensitive = TRUE, language = .language)
# })
test_that("Test output.name is NULL", {
    .output.name <- NULL

    # case sensitive
    res <- db.gptapply(dat.test, INDEX = .index, output.name = .output.name,
                    FUN = fn.inc, output.signature = .signature,
                    clear.existing = TRUE, case.sensitive = TRUE, language = .language)
    expect_equal(is.data.frame(res), TRUE)
    expect_equal(nrow(res), nrow(dat.test))
    expect_equal(ncol(res), ncol(dat.test))

    # # case non-sensitive
    # res <- db.gptapply(dat.test, INDEX = .index, output.name = .output.name,
    #                 FUN = fn.inc, output.signature = .signature,
    #                 clear.existing = TRUE, case.sensitive = FALSE, language = .language)
    # expect_equal(is.data.frame(res), TRUE)
    # expect_equal(nrow(res), nrow(dat.test))
    # expect_equal(ncol(res), ncol(dat.test))

    # # clear.existing can be FALSE, or any other values, since output.name is NULL
    # res <- db.gptapply(dat.test, INDEX = .index, output.name = .output.name,
    #                 FUN = fn.inc, output.signature = .signature,
    #                 clear.existing = FALSE, case.sensitive = TRUE, language = .language)
    # expect_equal(is.data.frame(res), TRUE)
    # expect_equal(nrow(res), nrow(dat.test))
    # expect_equal(ncol(res), ncol(dat.test))
})











db.disconnect(cid)
