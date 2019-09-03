# GPTapply

getRandomNameList <- function(n = 1) {
  a <- do.call(paste0, replicate(5, sample(LETTERS, n, TRUE), FALSE))
  paste0(a, sprintf("%04d", sample(9999, n, TRUE)), sample(LETTERS, n, TRUE))
}

.index.translate <- function(INDEX, ar){
	if(is.null(INDEX))
	{
		stop("INDEX value cannot be NULL");
	}
	else if (is.integer(INDEX) || (is.double(INDEX) && floor(INDEX) == INDEX))
	{
		return(ar$.col.name[INDEX]);
	}
	else if(!is.character(INDEX))
	{
		stop("INDEX value cannot be determined with value of type ", typeof(INDEX))
	}
	#additional support: if (is.db.data.frame(INDEX))
}

.generate.gptapply.query <- function(output.name, funName, call_udf_params, call_udf_inner_params, relation_name, INDEX, typeName, output.distributeOn, clear.existing){
	if (is.null(output.name))
	{
		query <- sprintf("WITH gpdbtmpa AS (SELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s GROUP BY %s) tmptbl) SELECT (gpdbtmpb::%s).* FROM gpdbtmpa;",
				funName, call_udf_params, call_udf_inner_params, relation_name, INDEX, typeName)
	}
	else
	{
		#add distributeOn
		query <- sprintf("CREATE TABLE %s AS WITH gpdbtmpa AS (SELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s GROUP BY %s) tmptbl) SELECT (gpdbtmpb::%s).* FROM gpdbtmpa %s;",
				output.name, funName, call_udf_params, call_udf_inner_params, relation_name, INDEX, typeName, .distribute.str(output.distributeOn))
	}

	if (clear.existing){
            query_drop_table <- sprintf("DROP TABLE IF EXISTS %s;", output.name)
            query <- paste(query_drop_table, query, sep="\n")
    }
	
	return(query)
}


db.gptapply <- function(X, INDEX, FUN = NULL, output.name=NULL, output.signature=NULL,
		clear.existing=FALSE, case.sensitive=FALSE,output.distributeOn=NULL,debugger.mode = FALSE, simplify = TRUE, language="plr", ...)
{	
	args <- list(...)

	randomName <- getRandomNameList()[1]

	#create type
	typeName <- sprintf("gptype_%s", randomName)
	create_type_str <- .create.type(typeName, output.signature, case.sensitive)
    db.q(create_type_str)
	
	# generate function parameter str
	ar <- attributes(X)
	func_para_str <- ""
	local_data_frame_str <- ""
	call_udf_params <- ""
	call_udf_inner_params <- ""
	relation_name <- ar$.content

	local_data_frame_str <- paste(ar$.col.name, ar$.col.name, sep="=", collapse=", ")
	call_udf_params <- paste(ar$.col.name, collapse=", ")
	
	if (case.sensitive){
        output.name <-  paste("\"",output.name,"\"", sep='')
        relation_name <- paste("\"",relation_name,"\"", sep='')
        ar$.col.name <- paste("\"", ar$.col.name, "\"", sep='')
    }

	INDEX <- .index.translate(INDEX, ar)
	for (i in 1:length(ar$.col.name))
	{
		if (i > 1)
		{
			if(toupper(ar$.col.name[i]) == toupper(INDEX))
			{
				call_udf_inner_params <- paste(call_udf_inner_params, ", ", ar$.col.name[i], sep="")
				func_para_str <- paste(func_para_str, ", ", ar$.col.name[i], " ", ar$.col.udt_name[i], sep="")
			}
			else
			{
				call_udf_inner_params <- paste(call_udf_inner_params, " , array_agg(", ar$.col.name[i], ") AS ", ar$.col.name[i], sep="")
				func_para_str <- paste(func_para_str, ", ", ar$.col.name[i], " ", ar$.col.udt_name[i], "[]", sep="")
			}
		}
		else
		{
			if(toupper(ar$.col.name[i]) == toupper(INDEX))
			{
				call_udf_inner_params <- paste(call_udf_inner_params, ar$.col.name[i], sep="")
				func_para_str <- paste(func_para_str, ar$.col.name[i], " ", ar$.col.udt_name[i], sep="")
			}
			else
			{
				call_udf_inner_params <- paste(call_udf_inner_params, "array_agg(", ar$.col.name[i], ") AS ", ar$.col.name[i], sep="")
				func_para_str <- paste(func_para_str, ar$.col.name[i], " ", ar$.col.udt_name[i], "[]", sep="")
			}
		}
	}
	#print(call_udf_params)
	#print(call_udf_inner_params)
	
	listStr <- .extract.param.list(args)
	if (nchar(listStr)>0)
		listStr <- paste(", ", listStr, sep="")

	funName <- sprintf("gprfunc_%s", randomName)
	funBody <- paste("# container:  plc_r_shared\ngplocalf <- ",paste(deparse(FUN), collapse="\n"))
	localdf <- sprintf("df <- data.frame(%s)\n", local_data_frame_str)
	localcall <- sprintf("do.call(gplocalf, list(df %s))", listStr);

	createStmt <- sprintf("CREATE FUNCTION %s (%s) RETURNS SETOF %s AS $$ %s\n %s\ return(%s)\n $$ LANGUAGE '%s';",
	funName, func_para_str, typeName, funBody, localdf, localcall, language);

	#print(createStmt)
	db.q(createStmt)

	
	#print(query)

	tryCatch({
	query <- .generate.gptapply.query(output.name, funName, call_udf_params, call_udf_inner_params, relation_name, INDEX, typeName, output.distributeOn, clear.existing)
	results <- db.q(query, nrows = NULL)
	}, error = function(e){
		print("ERROR when executing: \n %s", query)
		cleanString <- sprintf("DROP TYPE %s CASCADE;",typeName)
		db.q(cleanString)
	})
	

	cleanString <- sprintf("DROP TYPE %s CASCADE;",typeName)
	db.q(cleanString)

	return(results)

}
