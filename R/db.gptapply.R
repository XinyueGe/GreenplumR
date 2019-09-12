# GPTapply
# Must handle if the value of INDEX is case-sensitive
.index.translate <- function(INDEX, ar)
{
    if (is.null(INDEX))
        stop("INDEX value cannot be NULL")
    if (is.integer(INDEX) || (is.double(INDEX) && floor(INDEX) == INDEX))
        return (ar$.col.name[INDEX])
    if (!is.character(INDEX))
        stop("INDEX value cannot be determined with value of type ", typeof(INDEX))
    # Take care: the INDEX may be case-sensitive
    if (INDEX %in% ar$.col.name)
        return (INDEX)
    index <- tolower(INDEX)
    if (index %in% ar$.col.name)
        return (index)
    stop(paste("invalid INDEX value:", INDEX))
}

#.generate.gptapply.query <- function(output.name, funName, param.name.list, param.group.list, relation_name, INDEX, typeName, output.distributeOn, clear.existing){
#    if (is.null(output.name))
#    {
#        query <- sprintf("WITH gpdbtmpa AS (\nSELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s GROUP BY %s) tmptbl\n)\nSELECT (gpdbtmpb::%s).* FROM gpdbtmpa;",
#                funName, param.name.list, param.group.list, relation_name, INDEX, typeName)
#    }
#    else
#    {
#        #add distributeOn
#        query <- sprintf("CREATE TABLE %s AS\nWITH gpdbtmpa AS (\nSELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s GROUP BY %s) tmptbl\n)\nSELECT (gpdbtmpb::%s).* FROM gpdbtmpa %s;",
#                output.name, funName, param.name.list, param.group.list, relation_name, INDEX, typeName, .distribute.str(output.distributeOn))
#        clearStmt <- .clear.existing.table(output.name, clear.existing)
#        if (nchar(clearStmt) > 0)
#                    query <- paste(clearStmt, query)
#    }
#
#    return (query)
#}


db.gptapply <- function(X, INDEX, FUN = NULL, output.name = NULL, output.signature = NULL, clear.existing = FALSE, case.sensitive = FALSE,
        output.distributeOn = NULL, debugger.mode = FALSE, simplify = TRUE, runtime.id = "plc_r_shared", language = "plcontainer", ...)
{

    if (is.null(X) || !is.db.data.frame(X))
        stop("X must be a db.data.frame")
    if (!is.function(FUN))
        stop("FUN must be a function")
    .check.output.name(output.name)
    .check.language(language)

    basename <- getRandomNameList()

    #create type
    typeName <- .to.type.name(basename)
    if (is.null(output.signature)) {
        # signature is null
        stop("output.signature is null")
    } else {
        create_type_str <- .create.type.sql(typeName, output.signature,
                                        case.sensitive = case.sensitive)
        db.q(create_type_str)
    }

    # generate function parameter str
    ar <- attributes(X)
    param.type.list <- ""
    param.group.list <- ""
    relation_name <- ar$.content

    param.name.list <- paste(ar$.col.name, collapse = ", ")

    if (isTRUE(case.sensitive)) {
        if (!is.null(output.name))
            output.name <-  paste('"', unlist(strsplit(output.name, '\\.')),'"', sep='', collapse='.')
    } else {
        if (!is.null(output.name))
            output.name <- tolower(output.name)
    }
    field.names <- paste('"', ar$.col.name, '"', sep = '')

    INDEX <- .index.translate(INDEX, ar)
    for (i in 1:length(ar$.col.name)) {
        if (i > 1) {
            if (ar$.col.name[i] == INDEX) {
                param.group.list <- paste(param.group.list, ", ", field.names[i], sep = "")
                param.type.list <- paste(param.type.list, ", ", ar$.col.name[i], " ", ar$.col.udt_name[i], sep = "")
            }
            else {
                param.group.list <- paste(param.group.list, " , array_agg(", field.names[i], ") AS ", ar$.col.name[i], sep = "")
                param.type.list <- paste(param.type.list, ", ", ar$.col.name[i], " ", ar$.col.udt_name[i], "[]", sep = "")
            }
        }
        else {
            if (ar$.col.name[i] == INDEX) {
                param.group.list <- paste(param.group.list, field.names[i], sep = "")
                param.type.list <- paste(param.type.list, ar$.col.name[i], " ", ar$.col.udt_name[i], sep = "")
            }
            else {
                param.group.list <- paste(param.group.list, "array_agg(", field.names[i], ") AS ", ar$.col.name[i], sep = "")
                param.type.list <- paste(param.type.list, ar$.col.name[i], " ", ar$.col.udt_name[i], "[]", sep = "")
            }
        }
    }
    print(param.name.list)
    print(param.group.list)

    createStmt <- .create.r.wrapper(basename = basename, FUN = FUN, 
                                selected.type.list = param.type.list,
                                selected.equal.list = .selected.equal.list(ar),
                                args = list(...), runtime.id = runtime.id,
                                language = language)
    print(createStmt)
    # db.q(createStmt)

    # STEP: Create SQL
    funName <- .to.func.name(basename)
    index <- paste('"', INDEX, '"', sep = '')
    if (is.null(output.name))
    {
        query <- sprintf("WITH gpdbtmpa AS (\nSELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s GROUP BY %s) tmptbl\n)\nSELECT (gpdbtmpb::%s).* FROM gpdbtmpa;",
                funName, param.name.list, param.group.list, relation_name, index, typeName)
    }
    else
    {
        query <- sprintf("CREATE TABLE %s AS\nWITH gpdbtmpa AS (\nSELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s GROUP BY %s) tmptbl\n)\nSELECT (gpdbtmpb::%s).* FROM gpdbtmpa %s;",
                output.name, funName, param.name.list, param.group.list, relation_name, index, typeName)
        clearStmt <- .clear.existing.table(output.name, clear.existing)
        if (nchar(clearStmt) > 0)
                    query <- paste(clearStmt, query)
    }
    print(query)
    results <- db.q(query, nrows = NULL)

    # STEP: Do cleanup
    cleanString <- sprintf("DROP TYPE %s CASCADE;",typeName)
    db.q(cleanString)

    return (results)
}
