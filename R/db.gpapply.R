
# needs to test
.simplify.signature <- function(signature) {
    if (is.null(signature) || is.list(signature))
        return (signature)
    if (is.function(signature))
        return (.simplify.signature(signature()))
    if (is.db.data.frame(signature))
        stop("type of signature is db.data.frame, not supported now")

    stop(paste("invalid signature:", signature))
}
.distribute.str <- function(distributeOn) {
    if (is.null(distributeOn)) {
        ""
    } else if (is.character(distributeOn)) {
        dist <- toupper(distributeOn)
        if (dist == "RANDOMLY") "DISTRIBUTED RANDOMLY"
        else if (dist == "REPLICATED") "DISTRIBUTED REPLICATED"
        else stop("invalid distribute value")
    } else if (is.list(distributeOn)) {
        # DISTRIBUTED BY (column)
        paste("DISTRIBUTED BY (", paste(distributeOn, sep=","), ")")
    } else {
        stop("invalid distributed value")
    }
}
.extract.param.list <- function(param_list) {
    if (is.null(param_list) || length(param_list)==0)
        return ("")
    arg_str_array <- strsplit(deparse(param_list), ", .Names = ")[[1]]
    message(length(arg_str_array))
    if (length(arg_str_array) == 1)
    {
        listStr <- substr(arg_str_array[1], 6, nchar(arg_str_array[1]) - 1)
    }
    else if (length(arg_str_array) == 2)
    {
        listStr <- substr(arg_str_array[1], 16, nchar(arg_str_array[1]) - 1)
    }
    else
    {
        stop("The functon input argument must not inlcude '.Names'")
    }
}

db.gpapply <- function(X, MARGIN=NULL, FUN = NULL, output.name=NULL, output.signature=NULL,
        clear.existing=FALSE, case.sensitive=FALSE,output.distributeOn=NULL, language="plcontainer", ...)
{
    
    if (!is.function(FUN))
        stop("FUN must be a function")
    randomName <- getRandomNameList()[1]

    typeName <- sprintf("gptype_%s", randomName)
    if (!is.null(output.signature))
    {
    
        #CASE_SENSITIVE:  output.signature
        typelist_str <- sprintf("CREATE TYPE %s AS (\n", typeName)
        typelist <- .simplify.signature(output.signature)

        if (case.sensitive){
            names(typelist) <- paste("\"",names(typelist),"\"", sep='')
        }

        fieldStr <- paste(names(typelist), typelist, sep=" ", collapse=",\n")
        typelist_str <- paste(typelist_str, fieldStr, "\n);", collapse="")
        #typelist_str <- paste(typelist_str, fieldStr, "\n);", sep="")
        message(typelist_str)
        db.q(typelist_str)
    }
    #CASE_SENSITIVE: output.name
    # generate function parameter str
    ar <- attributes(X)
    param_list_str <- ""
    local_data_frame_str <- ""
    #CASE_SENSITIVE: relation_name, param_list_str
    relation_name <- ar$.content
    local_data_frame_str <- paste(ar$.col.name, ar$.col.name, sep="=", collapse=", ")
    
    if (case.sensitive){
        output.name <-  paste("\"",output.name,"\"", sep='')
        relation_name <- paste("\"",relation_name,"\"", sep='')
        ar$.col.name <- paste("\"", ar$.col.name, "\"", sep='')
    }

    param_list_str_no_type <- paste(ar$.col.name, collapse=", ")
    param_list_str_with_type <- paste(ar$.col.name, ar$.col.udt_name, collapse=", ")

    message(param_list_str)

    funName <- paste("gprfunc_", randomName, sep="")

    # extract parameter list from gpapply
    listStr <- .extract.param.list(list(...))
    if (nchar(listStr)>0)
        listStr <- paste(", ", listStr, sep="")
    message(paste('listStr=', listStr))
    
    #FUN_NAME <- paste("gprfunc_", as.character(quote(FUN), randomNamesep="")
    funBody <- paste("# container:  plc_r_shared\ngplocalf <- ", paste(deparse(FUN), collapse="\n"))
    localdf <- sprintf("df <- data.frame(%s)\n", local_data_frame_str)
    localcall <- sprintf("do.call(gplocalf, list(df %s))", listStr);

    createStmt <- sprintf("CREATE OR REPLACE FUNCTION %s (%s) RETURNS SETOF %s AS $$ %s\n %s\ return(%s)\n $$ LANGUAGE '%s';",
    funName, param_list_str_with_type, typeName, funBody, localdf, localcall, language);
    
    tryCatch(
        db.q(createStmt), 
        error = function(e){
            print("hhhh")
            print(createStmt)
            print(funName)
            #print(param_list_str)
            print(typeName)
            print(funBody)
            print(localdf)
            print(localcall)
            print(language)
            print(ar$.col.name)
            #And anything else you want to only happen when errors do
        }
    )
    #write(createStmt, stderr())
    write("test", stderr())
    db.q(createStmt)


    if (is.null(output.name))
    {
        query <- sprintf("WITH gpdbtmpa AS (SELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s) tmptbl) SELECT (gpdbtmpb::%s).* FROM gpdbtmpa;",
                funName, param_list_str_no_type, param_list_str_no_type, relation_name, typeName)
    }
    else
    {
        if (clear.existing){
            query_drop_table = sprintf("DROP TABLE IF EXISTS %s", output.name)
            db.q(query_drop_table)
        }

        query <- sprintf("CREATE TABLE %s AS WITH gpdbtmpa AS (SELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s) tmptbl) SELECT (gpdbtmpb::%s).* FROM gpdbtmpa %s;",
                output.name, funName, param_list_str_no_type, param_list_str_no_type, relation_name, typeName, .distribute.str(output.distributeOn))
    }
    #message(query)

    results <- db.q(query, nrows = NULL)

    cleanString <- sprintf("DROP TYPE %s CASCADE;",typeName)

    #message(cleanString)
    db.q(cleanString)

    return(results)

}
