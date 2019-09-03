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

.create.type <- function(typeName, output.signature, case.sensitive){
    if (!is.null(output.signature))
    {
        typelist_str <- sprintf("CREATE TYPE %s AS (\n", typeName)
        typelist <- .simplify.signature(output.signature)

        #CASE_SENSITIVE SUPPORT FOR output.signature
        if (case.sensitive){
            names(typelist) <- paste("\"",names(typelist),"\"", sep='')
        }

        fieldStr <- paste(names(typelist), typelist, sep=" ", collapse=",\n")
        typelist_str <- paste(typelist_str, fieldStr, "\n);", collapse="")
        #typelist_str <- paste(typelist_str, fieldStr, "\n);", sep=")
    }
    else{
        #return dataframe
    }
    return (typelist_str)
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

.generate.gpapply.query <- function(output.name, funName, param_list_str_no_type,
                        relation_name, typeName, output.distributeOn, clear.existing){
    if (is.null(output.name))
    {
        query <- sprintf("WITH gpdbtmpa AS (SELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s) tmptbl) SELECT (gpdbtmpb::%s).* FROM gpdbtmpa;",
                funName, param_list_str_no_type, param_list_str_no_type, relation_name, typeName)
    }
    else
    {
        query <- sprintf("CREATE TABLE %s AS WITH gpdbtmpa AS (SELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s) tmptbl) SELECT (gpdbtmpb::%s).* FROM gpdbtmpa %s;",
                output.name, funName, param_list_str_no_type, param_list_str_no_type, relation_name, typeName, .distribute.str(output.distributeOn))
        
        if (clear.existing){
            query_drop_table <- sprintf("DROP TABLE IF EXISTS %s;", output.name)
            query <- paste(query_drop_table, query, sep="\n")
        }
    }
    
    return (query)
}

db.gpapply <- function(X, MARGIN=NULL, FUN = NULL, output.name=NULL, output.signature=NULL,
        clear.existing=FALSE, case.sensitive=FALSE,output.distributeOn=NULL, language="plcontainer", ...)
{
    
    if (!is.function(FUN))
        stop("FUN must be a function")
    randomName <- getRandomNameList()[1]

    #create returned type
    typeName <- sprintf("gptype_%s", randomName)
    create_type_str <- .create.type(typeName, output.signature, case.sensitive)
    db.q(create_type_str)

    #create handling function 
    #CASE_SENSITIVE: output.name, relation_name, X col name 
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

    #param_list_str_no_type is equivalent to call_udf_params in gptapply
    param_list_str_no_type <- paste(ar$.col.name, collapse=", ")
    param_list_str_with_type <- paste(ar$.col.name, ar$.col.udt_name, collapse=", ")

    # extract parameter list from gpapply
    args <- list(...)
    listStr <- .extract.param.list(args)
    #TBD: need case sensitive support here? 
    if (nchar(listStr)>0)
        listStr <- paste(", ", listStr, sep="")

    #generate output function
    funName <- paste("gprfunc_", randomName, sep="")
    funBody <- paste("# container:  plc_r_shared\ngplocalf <- ", paste(deparse(FUN), collapse="\n"))
    localdf <- sprintf("df <- data.frame(%s)\n", local_data_frame_str)
    localcall <- sprintf("do.call(gplocalf, list(df %s))", listStr);

    #Question: CREATE OR REPLACE FUNCTION? 
    createStmt <- sprintf("CREATE FUNCTION %s (%s) RETURNS SETOF %s AS $$ %s\n %s\ return(%s)\n $$ LANGUAGE '%s';",
                          funName, param_list_str_with_type, typeName, funBody, localdf, localcall, language);

    db.q(createStmt)

    #run the generated query inside GPDB
    tryCatch({
    query <- .generate.gpapply.query(output.name, funName, param_list_str_no_type, 
                                relation_name, typeName, output.distributeOn, clear.existing)
    results <- db.q(query, nrows = NULL)
    }, error = function(e){
		print("ERROR when executing: \n %s", query)
		cleanString <- sprintf("DROP TYPE %s CASCADE;",typeName)
		db.q(cleanString)
	})
    
    #drop type 
    cleanString <- sprintf("DROP TYPE %s CASCADE;",typeName)
    db.q(cleanString)

    return(results)

}


