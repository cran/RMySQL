#include "RMySQL.h"

/**********************************************************************
 *
 * $Id$
 * 
 * S DataBase Interface to MySQL
 * This driver hooks S and MySQL and implements the proposed S-DBI
 * generic R/S-database interface 0.2.
 * For details see
 *  On S, Appendix A in "Programming with Data" by John M. Chambers. 
 *  On R, "The .Call and .External Interfaces"  in the R manual.
 *  On MySQL, The "MySQL Reference Manual (version 3.23.7 alpha,
 *     02 Dec 1999" and the O'Reilly book "MySQL & mSQL" by Yarger, 
 *     Reese, and King.
 * 
 * C Function library which can be used to run SQL queries from
 * inside of S4, Splus5.x, or R.
 *
 * TODO:
 *    1. make sure we don't leave any variables/structs that may make this
 *       code thread unsafe --- we may need to move the globals to
 *       a struct that we pass around to all the functions that need it.
 */

static S_MySQL_connectionTable *connectionTable;

s_object *
S_MySQL_init(s_object *config_params, s_object *reload)
{
  /* Currently we can specify the defaults for 2 parameters, max num of
   * connections, and max of records per fetch (this can be over-ridden
   * explicitly in the S call to fetch).
   */
  S_EVALUATOR

  int   i, max_con;
  Sint  *params, fetch_default_rec, force_reload;

  S_MySQL_connection **con;
  s_object *output;

  force_reload = LOGICAL_DATA(reload)[0];
  if(force_reload){
    s_object  *cls, *mgrId;
    MEM_PROTECT(mgrId = NEW_INTEGER((Sint) 1));
    INTEGER_DATA(mgrId)[0] = (Sint) getpid();
    cls = S_MySQL_close(mgrId);
    MEM_UNPROTECT(1);
  }
  /* make sure connectionTable hasn't been inititalized yet. 
   * Force reload does not mean re-setting alloc a new
   * connectionTable, it means allocating (possibly more) space
   * for connections.  WARNING: force.reload will close *all* open
   * connections (thus the "force" in force.reload!)
   */
  if(connectionTable){
    /* do we need to allocate space for new connections? */
    if(connectionTable->connections){
      output = NEW_INTEGER((Sint) 1);
      INTEGER_DATA(output)[0] = (Sint) connectionTable->processId;
      return output;
    }
  }
  else {
    connectionTable = (S_MySQL_connectionTable *)
      malloc(sizeof(S_MySQL_connectionTable));
  }
  params = INTEGER_DATA(config_params);
  max_con = (int) params[0];
  fetch_default_rec = params[1];
  if(max_con < 0 || max_con > S_MYSQL_MAX_CON)
    max_con = S_MYSQL_MAX_CON;
  con = (S_MySQL_connection **) 
          calloc(max_con, sizeof(S_MySQL_connection *));
  if(!con)
    S_DBI_errorMessage("could not allocate space for MySQL connections",
		       S_DBI_ERROR);
  connectionTable->connections = con;
  connectionTable->connectionIds = (int *) calloc(max_con, sizeof(int));
  if(!connectionTable->connectionIds)
    S_DBI_errorMessage("could not allocate space for MySQL connection Ids",
		       S_DBI_ERROR);
  for( i = 0; i < max_con; i++){
    con[i] = (S_MySQL_connection *) NULL;
    connectionTable->connectionIds[i] = -1;
  }
  connectionTable->fetch_default_rec = fetch_default_rec;
  connectionTable->processId = getpid();
  connectionTable->num_con = 0;
  connectionTable->counter = 0;
  connectionTable->length = max_con;

  /* return a manager id (pid) */
  output = NEW_INTEGER((Sint) 1);
  INTEGER_DATA(output)[0] = (Sint) getpid();
  return output;
}

s_object *
S_MySQL_close(s_object *mgr_id)
{
  /* remove connectionTable and possibly any remaining open
   * connections and resultSets.
   */

  S_EVALUATOR

  s_object *output;
  int i;
  S_MySQL_connection **connections;

  if(!is_validHandle(mgr_id, 1))
    S_DBI_errorMessage("invalid MySQL manager object", S_DBI_ERROR);

  MEM_PROTECT(output = NEW_LOGICAL((Sint) 1));
  connections = connectionTable->connections;
  for(i = 0; i < connectionTable->length; i++){
    if(connections[i])
      S_mysql_closeConnection(connections[i]);
  }
  if(connectionTable->connectionIds)
    free(connectionTable->connectionIds);
  free(connections);
  connectionTable->connections = (S_MySQL_connection **) NULL;
  connectionTable->connectionIds = (int *) NULL;
  LOGICAL_DATA(output)[0] = TRUE;
    
  /* On purpose we don't de-allocate the ptr to connectionTable in order
   * not to restart the connection counter (we´re making sure connection ids
   * are unique for a given process).  See S_MySQL_init for more details.
   */

  MEM_UNPROTECT(1);

  return output;
}

s_object *
S_MySQL_newConnection(s_object *mgr_id, s_object *con_params)
{
  /* return a 2-element vector with process id and connection id.
   * These will define a "connection" object in S.  All functionality
   * is provided by C methods that key off this "connection" S object.
   */
  S_EVALUATOR

  s_object  *conHandle;
  MYSQL     *my_connection;
  S_MySQL_connection  *con;
  unsigned int  port, client_flags;
  char  *user, *passwd, *host, *dbname, *unix_socket;

  if(!is_validHandle(mgr_id, 1))
    S_DBI_errorMessage("invalid MySQLManger", S_DBI_ERROR);

  user   = (strcmp(CHAR_DEREF(STRING_ELT(con_params, 0)), "") ?
	    CHAR_DEREF(STRING_ELT(con_params, 0)) : NULL);
  passwd = (strcmp(CHAR_DEREF(STRING_ELT(con_params, 1)), "") ?
	    CHAR_DEREF(STRING_ELT(con_params, 1)) : NULL);
  host   = (strcmp(CHAR_DEREF(STRING_ELT(con_params, 2)), "") ?
	    CHAR_DEREF(STRING_ELT(con_params, 2)) : NULL);
  dbname = (strcmp(CHAR_DEREF(STRING_ELT(con_params, 3)), "") ?
	    CHAR_DEREF(STRING_ELT(con_params, 3)) : NULL);
  unix_socket = (strcmp(CHAR_DEREF(STRING_ELT(con_params, 4)), "") ?
		 CHAR_DEREF(STRING_ELT(con_params, 4)) : NULL);
  port   = (unsigned int) atol(CHAR_DEREF(STRING_ELT(con_params, 5)));
  client_flags = (unsigned int) atol(CHAR_DEREF(STRING_ELT(con_params, 6)));

  my_connection = mysql_init(NULL);
  my_connection = 
    mysql_real_connect(my_connection, host, user, passwd, dbname, 
		       port, unix_socket, client_flags);
  if(!my_connection)
    S_DBI_errorMessage("could not open MySQL", S_DBI_ERROR);
  con = (S_MySQL_connection *) malloc(sizeof(S_MySQL_connection));
  if(!con){
    mysql_close(my_connection);
    S_DBI_errorMessage("could not alloc space for connection object",
		       S_DBI_ERROR);
  }
  con->my_connection = my_connection;
  con->processId = getpid();
  con->connectionId = S_DBI_assignConnection(con);
  if(!user) user = "";
  con->user = S_DBI_copyString(user);
  if(!host) host = "";
  con->host = S_DBI_copyString(host);
  if(!dbname) dbname = "";
  con->dbname = S_DBI_copyString(dbname);
  con->counter = 0;               /* num of queries on the connection*/
  con->resultSet = (S_MySQL_resultSet *) NULL;
  conHandle = NEW_INTEGER((Sint) 2);
  INTEGER_DATA(conHandle)[0] = (Sint) con->processId;
  INTEGER_DATA(conHandle)[1] = (Sint) con->connectionId;

  return conHandle;
}

s_object *
S_MySQL_closeConnection(s_object *conHandle)
{
  S_EVALUATOR

  S_MySQL_connection  *con;
  s_object  *output;

  con = S_DBI_resolveConHandle(conHandle);
  S_mysql_closeConnection(con);

  /* TODO: make sure we did in fact close the connection */
  output = NEW_LOGICAL((Sint) 1);
  LOGICAL_DATA(output)[0] = TRUE;
  return output;
}


/* Execute (currently) one sql statement (INSERT, DELETE, SELECT, etc.),
 * set coercion type mappings between the server internal data types and 
 * S classes.  
 * Returns rsHandle, an S handle to a resultSet object.
 */

s_object *
S_MySQL_exec(s_object *conHandle, s_object *statement)
{
  S_EVALUATOR

  s_object  *rsHandle;
  S_MySQL_resultSet  *result;
  S_MySQL_connection *con;
  MYSQL  *my_connection;
  unsigned int   num_fields;
  int    state, is_select;
  char   *dyn_statement;

  con = S_DBI_resolveConHandle(conHandle);
  dyn_statement = S_DBI_copyString(CHAR_DEREF(STRING_ELT(statement, 0)));

  my_connection = con->my_connection;

  /* Do we have a pending resultSet in the current connection?   */
  if(con->resultSet != (S_MySQL_resultSet *) NULL){
    if(con->resultSet->completed == 0)
      S_DBI_errorMessage( 
       "connection with pending rows, close resultSet before continuing",
       S_DBI_ERROR);
    else{
      S_mysql_closeResultSet(con->resultSet);
      con->resultSet = (S_MySQL_resultSet *) NULL;
    }
  }

  state = mysql_query(my_connection, dyn_statement);
  if(state){
    char buf[512];
    sprintf(buf, "could not run statement: %s",
	    mysql_error(con->my_connection));
    S_DBI_errorMessage(buf, S_DBI_ERROR);
  }
  /* Do we need output column/field descriptors?  Only for SELECT-like
   * statements. The MySQL reference manual suggests invoking
   * mysql_use_result() and if it succeed the statement is SELECT-like
   * that can use a resultSet.  Otherwise call mysql_field_count()
   * and if it returns zero, the sql was not a SELECT-like statement.
   * Finally a non-zero means a failed SELECT-like statement.
   */

  num_fields = mysql_field_count(con->my_connection);
  result = S_mysql_createResultSet(con, dyn_statement);
  if(!result)
    S_DBI_errorMessage("could not allocate space for resultSet object",
		       S_DBI_ERROR);
  if(result->my_resultSet != (MYSQL_RES *) NULL)
    is_select = TRUE;
  else {
    if(num_fields == 0)
      is_select = FALSE;
    else  /* a failed select-like statement */
      S_DBI_errorMessage("error in mysql_query", S_DBI_ERROR);
  }
  result->rowCount = (Sint) 0;
  if(!is_select){
    result->rowsAffected = (Sint) mysql_affected_rows(con->my_connection);
    result->completed = 1;
   }
  else {
    result->rowsAffected = -(Sint) 1;
    result->completed = 0;
    /* We have a SELECT-like statement --- get the details on each column
     * being extracted (name, data types, ....)
     */
    result->fieldDescription = S_mysql_createDataMappings(result);
  }

  rsHandle = NEW_INTEGER((Sint) 3);
  INTEGER_DATA(rsHandle)[0] = (Sint) result->processId;
  INTEGER_DATA(rsHandle)[1] = (Sint) result->connectionId;
  INTEGER_DATA(rsHandle)[2] = (Sint) result->resultSetId;
  return rsHandle;
}

/* Currently we allow only one resultSet per connection */
S_MySQL_resultSet * 
S_mysql_createResultSet(S_MySQL_connection *con, char *statement)
{
  S_MySQL_resultSet *result;

  result = (S_MySQL_resultSet *) malloc(sizeof(S_MySQL_resultSet));
  if(!result)
    S_DBI_errorMessage("could not allocate resultSet object",
		       S_DBI_ERROR);
  result->my_resultSet = mysql_use_result(con->my_connection);
  if(!(result->my_resultSet) )
    S_DBI_errorMessage(mysql_error(con->my_connection), S_DBI_ERROR);
 
  result->statement = statement;
  result->processId = con->processId;
  result->connectionId = con->connectionId;
  result->rowsAffected = -(Sint) 1;
  result->rowCount = -(Sint) 1;
  result->completed = -1;
  result->fieldDescription = (S_DBI_fieldDescription *) NULL;
  
  con->counter += 1;
  result->resultSetId = con->counter;
  con->resultSet = result;  /* currently we only allow 1 rs/con */

  return result; 
}

S_DBI_fieldDescription *
S_mysql_createDataMappings(S_MySQL_resultSet *result)
{
  MYSQL_FIELD   *select_dp;
  S_MySQL_connection      *con;
  S_DBI_fieldDescription  *flds;
  int   j, num_fields, null_ok, precision, scale, internal_type;

  select_dp = mysql_fetch_fields(result->my_resultSet);
  con = S_DBI_getConnection(result->connectionId);
  num_fields = (int) mysql_field_count(con->my_connection);

  flds = (S_DBI_fieldDescription *) malloc(sizeof(S_DBI_fieldDescription));
  if(!flds)
    S_DBI_errorMessage("could not alloc field description object",
		       S_DBI_ERROR);
  flds->num_fields = num_fields;
  flds->name =     (char **)calloc(num_fields, sizeof(char *));
  flds->type =     (Sint *) calloc(num_fields, sizeof(Sint));
  flds->length =   (Sint *) calloc(num_fields, sizeof(Sint));
  flds->precision= (Sint *) calloc(num_fields, sizeof(Sint));
  flds->scale =    (Sint *) calloc(num_fields, sizeof(Sint));
  flds->nullOk =   (Sint *) calloc(num_fields, sizeof(Sint));
  flds->Sclass =   (Sint *) calloc(num_fields, sizeof(Sint));

  for (j = 0; j < num_fields; j++){

    /* First, save the name, MySQL internal field name, type, length, etc. */
    
    flds->name[j] = S_DBI_copyString(select_dp[j].name);
    flds->type[j] = select_dp[j].type;  /* recall that these are enum*/
    flds->length[j] = select_dp[j].length;
    flds->precision[j] = select_dp[j].length; 
    flds->scale[j] = select_dp[j].decimals; 
    flds->nullOk[j] = (!IS_NOT_NULL(select_dp[j].flags));

    internal_type = select_dp[j].type;
    switch(internal_type) {
    case FIELD_TYPE_VAR_STRING:
    case FIELD_TYPE_STRING:
      flds->Sclass[j] = CHARACTER_TYPE;
      break;
    case FIELD_TYPE_TINY:
    case FIELD_TYPE_SHORT:
    case FIELD_TYPE_LONG:
    case FIELD_TYPE_LONGLONG:
    case FIELD_TYPE_INT24:
      flds->Sclass[j] = INTEGER_TYPE;
      break;
    case FIELD_TYPE_DECIMAL:
      if(scale > 0)
	flds->Sclass[j] = NUMERIC_TYPE;
      else 
	flds->Sclass[j] = INTEGER_TYPE;
      break;
    case FIELD_TYPE_FLOAT:
      flds->Sclass[j] = NUMERIC_TYPE;
      break;
    case FIELD_TYPE_DOUBLE:
      flds->Sclass[j] = NUMERIC_TYPE;
      break;
    case FIELD_TYPE_BLOB:         /* TODO: how should we bring large ones*/
    case FIELD_TYPE_TINY_BLOB:
    case FIELD_TYPE_MEDIUM_BLOB:
    case FIELD_TYPE_LONG_BLOB:
      flds->Sclass[j] = LIST_TYPE;
      break;
    case FIELD_TYPE_DATE:
    case FIELD_TYPE_TIME:
    case FIELD_TYPE_DATETIME:
    case FIELD_TYPE_YEAR:
    case FIELD_TYPE_NEWDATE:
      flds->Sclass[j] = CHARACTER_TYPE;
      break;
    case FIELD_TYPE_ENUM:
      flds->Sclass[j] = CHARACTER_TYPE;   /* see the MySQL ref. manual */
      break;
    case FIELD_TYPE_SET:
      flds->Sclass[j] = CHARACTER_TYPE;
      break;
    default:
      flds->Sclass[j] = CHARACTER_TYPE;
    }
  }  
  return flds;
}

/* TODO: monitor memory/object size consumption agains S limits */
/* TODO: invoke user-specified generators */
void
S_DBI_allocOutput(s_object *output, S_DBI_fieldDescription *flds,
		  Sint num_rec, int  expand)
{
  S_EVALUATOR

  Sint   num_fields;
  Stype  *fld_Sclass;
  int    j;

  num_fields = (Sint) flds->num_fields;
  fld_Sclass = flds->Sclass;

  if(expand){
    for(j = 0; j < num_fields; j++) {
      s_object *elt = VECTOR_ELT(output, j);
      SET_LENGTH(elt, num_rec);
      SET_VECTOR_ELT(output, j, elt);
    }
    return;
  }

  for(j = 0; j < num_fields; j++){

    switch((int)fld_Sclass[j]){
    case LOGICAL_TYPE:    
      SET_VECTOR_ELT(output, (Sint) j, MEM_PROTECT(NEW_LOGICAL(num_rec)));
      break;
    case CHARACTER_TYPE:
      SET_VECTOR_ELT(output, (Sint) j, MEM_PROTECT(NEW_CHARACTER(num_rec)));
      break;
    case INTEGER_TYPE:
      SET_VECTOR_ELT(output, (Sint) j, MEM_PROTECT(NEW_INTEGER(num_rec)));
      break;
    case NUMERIC_TYPE:
      SET_VECTOR_ELT(output, (Sint) j, MEM_PROTECT(NEW_NUMERIC(num_rec)));
      break;
    case LIST_TYPE:
      SET_VECTOR_ELT(output, (Sint) j, MEM_PROTECT(NEW_LIST(num_rec)));
      break;
#ifndef USING_R
    case RAW:
      SET_VECTOR_ELT(output, (Sint) j, MEM_PROTECT(NEW_RAW(num_rec)));
      break;
#endif
    default:
      S_DBI_errorMessage("unsupported data type", S_DBI_ERROR);
    }
    MEM_UNPROTECT(1);
  }
  return;
}
s_object *
S_MySQL_fetch(s_object *rsHandle, s_object *max_rec)
{
  S_EVALUATOR

  s_object  *S_output_list, *S_output_names;
  s_object  *raw_obj, *raw_list;

  S_DBI_fieldDescription  *flds;
  S_MySQL_resultSet  *result;
  unsigned long  *lens;

  MYSQL_RES   *my_resultSet;
  MYSQL_ROW   row;

  int    i, j, null_item, expand;
  Sint   *fld_nullOk, completed;
  Stype  *fld_Sclass;
  char   **fld_names, *raw_data;
  charPtr ptr_c;
  Sint   *ptr_i, rowCount, num_rec, num_fields;
  double *ptr_d;   

  result = S_DBI_resolveRSHandle(rsHandle);
  if(!result)
    S_DBI_errorMessage("could not resolve resultSet handle",
		       S_DBI_ERROR);
  flds = result->fieldDescription;
  if(!flds)
    S_DBI_errorMessage("corrupt resultSet, missing fieldDescription",
		       S_DBI_ERROR);
  num_rec = INTEGER_DATA(max_rec)[0];
  if(num_rec < 1){     /* just get all the records */
    expand = 1; 
    num_rec = connectionTable->fetch_default_rec;  /* alloc in chunks  */
  }
  else
    expand = 0;
  num_fields = flds->num_fields;
  MEM_PROTECT(S_output_list = NEW_LIST((Sint) num_fields));
  S_DBI_allocOutput(S_output_list, flds, num_rec, 0);

  fld_names = flds->name;
  fld_Sclass = flds->Sclass;
  fld_nullOk = flds->nullOk;
  
  /* actual fetching....*/
  my_resultSet = result->my_resultSet;
  completed = (Sint) 0;
  for(i = 0; ; i++){ 

    if(i==num_rec){
      if(expand){
	num_rec = 2 * num_rec;
	S_DBI_allocOutput(S_output_list, flds, num_rec, expand);
      }
      else
	break;
    }

    row = mysql_fetch_row(my_resultSet);
    if(row==NULL){    /* either we finish or we encounter an error */
      unsigned int  err_no;
      S_MySQL_connection   *con;
      con = S_DBI_getConnection(result->connectionId);
      err_no = mysql_errno(con->my_connection);
      if(err_no)
	completed = -(Sint) 1;
      else
	completed = (Sint) 1;
      break;
    }
    lens = mysql_fetch_lengths(my_resultSet); 
    for(j = 0; j < num_fields; j++){

      null_item = (row[j] == NULL);
      switch((int)fld_Sclass[j]){
      case INTEGER_TYPE:
	ptr_i = INTEGER_DATA(VECTOR_ELT(S_output_list, j));
	if(null_item)
	  ptr_i[i] = NA_INTEGER;
	else
	  ptr_i[i] = atol(row[j]);
	break;
      case CHARACTER_TYPE:
      {
	s_object *elt = VECTOR_ELT(S_output_list, j);
	if(null_item)
	  SET_STRING_ELT(elt, i, NA_STRING);
	else
	  SET_STRING_ELT(elt, i, C_S_CPY(row[j]));
	break;
      }
      case NUMERIC_TYPE:
	ptr_d = NUMERIC_DATA(VECTOR_ELT(S_output_list, j));
	if(null_item)
	  ptr_d[i] = NA_REAL;
	else
	  ptr_d[i] = (double) atof(row[j]);
	break;
#ifndef USING_R
      case LIST_TYPE:  /* these are blob's */
  	raw_list = els[j];
  	/* we need to inset a raw object */
	raw_obj = NEW_RAW((Sint) lens[j]);
	raw_data = RAW_DATA(raw_obj);
	memcpy(raw_data, row[j], lens[j]);
	SET_ELEMENT(raw_list, (Sint) i, raw_obj);
  	break;
#endif
      }
    }
  }
  
  /* actual number of records fetched */
  if(i < num_rec){
    num_rec = i;
    /* adjust the length of each of the members in the output_list */
    for(j = 0; j<num_fields; j++) {
      s_object *elt = VECTOR_ELT(S_output_list, j);
      SET_LENGTH(elt, num_rec);
      SET_VECTOR_ELT(S_output_list, j, elt);
    }
  }
  if(completed < 0)
    S_DBI_errorMessage("error while fetching rows", S_DBI_WARNING);

  result->rowCount += num_rec;
  result->completed = (int) completed;

  MEM_PROTECT(S_output_names = NEW_CHARACTER(num_fields));
  for(j = 0; j < num_fields; j++)
    SET_STRING_ELT(S_output_names, j, C_S_CPY(fld_names[j]));
  SET_NAMES(S_output_list, S_output_names);

  MEM_UNPROTECT(2);
  return(S_output_list);
}

s_object *
S_MySQL_closeResultSet(s_object *rsHandle)
{
  S_EVALUATOR

  s_object  *output;
  S_MySQL_resultSet  *result;
  S_MySQL_connection *con;

  result = S_DBI_resolveRSHandle(rsHandle);
  con = S_DBI_getConnection(result->connectionId);

  /* TODO: make sure we indeed closed the resultSet */

  S_mysql_closeResultSet(result);
  con->resultSet = (S_MySQL_resultSet *) NULL;
  output = NEW_LOGICAL((Sint) 1);
  LOGICAL_DATA(output)[0] = TRUE;
  return output;
}

s_object *
S_MySQL_getException(s_object *conHandle)
{
  S_EVALUATOR

  s_object  *exception, *s_errno, *s_errmsg;
  S_MySQL_connection   *con;
  unsigned int  err_no;
  char  *err_msg;

  con = S_DBI_resolveConHandle(conHandle);

  MEM_PROTECT(exception = NEW_LIST((Sint) 2));
  MEM_PROTECT(s_errno = NEW_INTEGER((Sint) 1));
  MEM_PROTECT(s_errmsg = NEW_CHARACTER((Sint) 1));
  err_no = mysql_errno(con->my_connection);
  err_msg = mysql_error(con->my_connection); 
  SET_STRING_ELT(s_errmsg, 0, C_S_CPY(err_msg));
  INTEGER_DATA(s_errno)[0] = (Sint) err_no;

  SET_VECTOR_ELT(exception, (Sint) 0, s_errno);
  SET_VECTOR_ELT(exception, (Sint) 1, s_errmsg);

  MEM_UNPROTECT(3);

  return exception;
}

void
S_mysql_closeConnection(S_MySQL_connection *con)
{
  unsigned int  err_no;
  int  con_id;
  
  if(!con->my_connection)
    S_DBI_errorMessage("could not close connection", S_DBI_ERROR);
  if(con->resultSet)
    S_mysql_closeResultSet(con->resultSet);
  con_id = con->connectionId;
  mysql_close(con->my_connection);
  if(con->user) free(con->user);
  if(con->host) free(con->host);
  if(con->dbname) free(con->dbname);
  free(con);
  S_DBI_removeConnectionId(con_id);
  return;
}

void
S_mysql_closeResultSet(S_MySQL_resultSet *result)
{
  /* we need to free the various components of the resultSet (oh what fun!)*/
  if(result->my_resultSet){
    /* we need to flush any possibly remaining rows (see Ch 20 p.358) */
    MYSQL_ROW row;
    while((row = mysql_fetch_row(result->my_resultSet)))
      ;
    mysql_free_result(result->my_resultSet);
  }
  if(result->statement) free(result->statement);
  if(result->fieldDescription){
    int  j;
    S_DBI_fieldDescription *flds;

    flds = result->fieldDescription;

    for( j = 0; j < flds->num_fields; j++){
      if(flds->name[j]) free(flds->name[j]);
    }
    /*  free(flds->name); */
    free(flds->type);
    free(flds->length);
    free(flds->precision);
    free(flds->scale);
    free(flds->nullOk);
    free(flds->Sclass);
    free(flds);
  }
  free(result);
  return;
}
    
void 
S_DBI_errorMessage(char *msg, int exception_type)
{
  switch(exception_type) {
  case S_DBI_MESSAGE:
    PROBLEM "MySQL driver message: (%s)", msg WARN; /* was PRINT_IT */
    break;
  case S_DBI_WARNING:
    PROBLEM "MySQL driver warning: (%s)", msg WARN;
    break;
  case S_DBI_ERROR:
    PROBLEM  "MySQL driver: (%s)", msg ERROR;
    break;
  case S_DBI_TERMINATE:
    PROBLEM "MySQL driver fatal: (%s)", msg ERROR; /* was TERMINATE */
    break;
  }
  return;
}

S_MySQL_resultSet *
S_DBI_resolveRSHandle(s_object *rsHandle)
{
  S_EVALUATOR

  S_MySQL_connection *con;
  pid_t pid;
  int   con_id, res_id;
  Sint  n;
  
  if(!is_validHandle(rsHandle, 3))
    S_DBI_errorMessage("invalid resultSet object", S_DBI_ERROR);
  pid  = (pid_t) INTEGER_DATA(rsHandle)[0];
  con_id = (int) INTEGER_DATA(rsHandle)[1];
  res_id = (int) INTEGER_DATA(rsHandle)[2];
  con = S_DBI_getConnection(con_id);
  return(con->resultSet);
}

S_MySQL_connection *
S_DBI_resolveConHandle(s_object *conHandle)
{
  S_EVALUATOR

  S_MySQL_connection *con;
  int  con_id;

  if(!is_validHandle(conHandle, 2))
    S_DBI_errorMessage("invalid connection object", S_DBI_ERROR);
  con_id = (int) INTEGER_DATA(conHandle)[1];
  con = S_DBI_getConnection(con_id);
  return(con);
}

char *
S_DBI_copyString(const char *str)
{
  char *buffer;

  buffer = (char *) malloc((size_t) strlen(str)+1);
  if(!buffer)
    S_DBI_errorMessage("could not alloc string space", S_DBI_ERROR);
  strcpy(buffer, str);
  return(buffer);
}

/* Here we implement the methods for the connectionManager:
 *   assignConnection to connectionTable
 *   removeConnection from connectionTable
 *   lookupConnectionId in connectionTable
 */

S_MySQL_connection *
S_DBI_getConnection(int con_id)
{
  /* NOTE: The con_id should be validated prior to getting here! */
  int indx;

  indx = S_DBI_lookupConnectionId(con_id);
  if(indx < 0)
    S_DBI_errorMessage("internal error: corrupted connection table", 
		       S_DBI_ERROR);
  if(!connectionTable->connections[indx])
    S_DBI_errorMessage("internal error: corrupted connection table", 
		       S_DBI_ERROR);
  return(connectionTable->connections[indx]);
}

int
S_DBI_lookupConnectionId(int con_id)
{
  int   i, *con_ids, indx;

  if(!connectionTable->connectionIds)
    return -1;
  con_ids = connectionTable->connectionIds;
  indx = -1;
  for(i = 0; i < connectionTable->length; i++)
    if(con_ids[i] == con_id){
      indx = i;
      break;
    }
  if(indx >= 0){
    if(!connectionTable->connections[indx])
      S_DBI_errorMessage("internal error: corrupted connectionTable",
	S_DBI_ERROR);
  }
  return(indx);
}

/* Store (assign)  connection into connectionTable. 
 * Return the connection id.
 */
int
S_DBI_assignConnection(S_MySQL_connection *con)
{
  int    i, indx, con_id;

  if(connectionTable->length == connectionTable->num_con)
    S_DBI_errorMessage("cannot allocate new connection in connection table",
		       S_DBI_ERROR);
  indx = -1;
  for(i = 0; i < connectionTable->length; i++)  
    if(!connectionTable->connections[i]){
      indx = i;                          /* free connection slot */
      break;
    }
  if(indx < 0)
    S_DBI_errorMessage("corrupted connection table", S_DBI_ERROR);
  connectionTable->num_con += 1;
  connectionTable->counter += 1;
  con_id = connectionTable->counter;
  connectionTable->connections[indx] = con;
  connectionTable->connectionIds[indx] = con_id;
  return con_id;
}

/* update connectionTable given that connection "con_id" 
 * has been closed. The calling function should free the space 
 * allocated by the connection itself.
 */
void
S_DBI_removeConnectionId(int con_id)
{
  int  indx;

  indx = S_DBI_lookupConnectionId(con_id);
  if(indx < 0)
    S_DBI_errorMessage("corrupted connection table", S_DBI_ERROR);
  connectionTable->connections[indx] = (S_MySQL_connection *) NULL;
  connectionTable->connectionIds[indx] = -1;
  connectionTable->num_con -= 1;
  return;
}

s_object *
S_MySQL_validHandle(s_object *handle)
{ 
   S_EVALUATOR
   s_object  *valid;

   MEM_PROTECT(valid = NEW_LOGICAL((Sint) 1));
   LOGICAL_DATA(valid)[0] = (Sint) 
                   is_validHandle(handle, (int) GET_LENGTH(handle));

   MEM_UNPROTECT(1);
   return valid;
}
    
int 
is_validHandle(s_object *handle, int handleType)
{
   /* types == 1, 2, or 3 for manager, connection, and resultSet handles */
   Sint   len;
   int    con_id, res_id, id, indx;
   pid_t  mgr_id;

   if(IS_INTEGER(handle))
     handle = AS_INTEGER(handle);
   else
     return 0;       /* non handle object */

   len = GET_LENGTH(handle);
   if(len!=handleType || handleType<1 || handleType>3) 
     return 0;
   mgr_id = (pid_t) INTEGER_DATA(handle)[0];
   if(getpid() != mgr_id)
     return 0;
   if(!connectionTable || !connectionTable->connections) /* expired manager*/
     return 0;
   if(handleType == 1) return 1;                 /* valid manager id */

   con_id = (int) INTEGER_DATA(handle)[1];
   indx = S_DBI_lookupConnectionId(con_id);
   if(indx<0) 
     return 0;
   if(handleType==2) return 1;                  /* valid connection id */

   if(connectionTable->connections[indx])
     if(connectionTable->connections[indx]->resultSet)
       id = connectionTable->connections[indx]->resultSet->resultSetId;
   else 
     return 0;
   res_id = INTEGER_DATA(handle)[2];
   if(res_id!=id) 
     return 0;
   return 1;
}


#define EQUAL(s1, s2)  !strcmp(s1, s2)

/* Copy resultState field "what" back to S */
s_object *
S_MySQL_resultSetInfo(s_object *rsHandle, s_object *what)
{
  S_EVALUATOR

  S_MySQL_resultSet  *result;
  s_object  *info;
  char  *fld_name;

  result = S_DBI_resolveRSHandle(rsHandle);
  fld_name = CHARACTER_VALUE(what);
  
  if(EQUAL(fld_name, "rowCount")){
    info = NEW_INTEGER((Sint) 1);
    INTEGER_DATA(info)[0] = (Sint) result->rowCount;
    return info;
  }
  if(EQUAL(fld_name, "rowsAffected")){
    info = NEW_INTEGER((Sint) 1);
    INTEGER_DATA(info)[0] = (Sint) result->rowsAffected;
    return info;
  }
  if(EQUAL(fld_name, "completed")){
    info = NEW_INTEGER((Sint) 1);
    INTEGER_DATA(info)[0] = (Sint) result->completed;
    return info;
  }
  if(EQUAL(fld_name, "statement")){
    MEM_PROTECT(info = NEW_CHARACTER((Sint) 1));
    SET_STRING_ELT(info, 0, C_S_CPY(result->statement));
    MEM_UNPROTECT(1);
    return info;
  }
  if(EQUAL(fld_name, "fieldDescription")){
    S_DBI_fieldDescription  *flds;
    flds = result->fieldDescription;
    info = S_DBI_copyFieldDescription(flds);
    return(info);
  }
  if(EQUAL(fld_name, "connection")){
    char   *buf;
    S_MySQL_connection   *con;

    con = S_DBI_getConnection(result->connectionId);
    buf = mysql_get_host_info(con->my_connection);
    MEM_PROTECT(info = NEW_CHARACTER((Sint) 1));
    SET_STRING_ELT(info, 0, C_S_CPY(buf));
    MEM_UNPROTECT(1);
    return info;
  }
}

s_object *
S_MySQL_managerInfo(void)
{
  S_EVALUATOR
  s_object *mgrInfo, *conIds;
  int  i, num_con;

  char *mgrDesc[] = {"connectionIds", "fetch_default_rec",
		     "managerId", "length", "num_con", "counter"};
  Stype mgrType[] = {INTEGER_TYPE, INTEGER_TYPE, INTEGER_TYPE,
		     INTEGER_TYPE, INTEGER_TYPE, INTEGER_TYPE};
  Sint mgrLen[]  = {1, 1, 1, 1, 1, 1};
  
  if(!connectionTable)
    S_DBI_errorMessage("driver not loaded yet", S_DBI_ERROR);
  num_con = (Sint) connectionTable->num_con;
  mgrLen[0] = num_con;

  mgrInfo = S_DBI_createNamedList(mgrDesc, mgrType, mgrLen, (Sint) 6);
  if(IS_LIST(mgrInfo))
    mgrInfo = AS_LIST(mgrInfo);
  conIds = VECTOR_ELT(mgrInfo, 0);
  for(i = 0; i < num_con; i++)
    INTEGER_DATA(conIds)[i] = connectionTable->connectionIds[i];

  INTEGER_DATA(VECTOR_ELT(mgrInfo, 1))[0] =
      connectionTable->fetch_default_rec;
  INTEGER_DATA(VECTOR_ELT(mgrInfo, 2))[0] =
      connectionTable->processId;
  INTEGER_DATA(VECTOR_ELT(mgrInfo, 3))[0] =
      connectionTable->length;
  INTEGER_DATA(VECTOR_ELT(mgrInfo, 4))[0] =
      connectionTable->num_con;
  INTEGER_DATA(VECTOR_ELT(mgrInfo, 5))[0] =
      connectionTable->counter;

  return mgrInfo;
}

s_object *
S_MySQL_connectionInfo(s_object *conHandle)
{
  S_EVALUATOR
  
  s_object  *info, *rsId, *output;
  S_MySQL_connection  *con;
  S_MySQL_resultSet   *res;
  char   *buf;

  con = S_DBI_resolveConHandle(conHandle);
  buf = mysql_get_host_info(con->my_connection);

  MEM_PROTECT(info = NEW_CHARACTER((Sint) 4));
  SET_VECTOR_ELT(info, 0, C_S_CPY(con->user));
  SET_VECTOR_ELT(info, 1, C_S_CPY(con->host));
  SET_VECTOR_ELT(info, 2, C_S_CPY(con->dbname));
  SET_VECTOR_ELT(info, 3, C_S_CPY(buf));
  
  MEM_PROTECT(rsId = NEW_INTEGER((Sint) 1));
  if(con->resultSet!= (S_MySQL_resultSet *) NULL)
    INTEGER_DATA(rsId)[0] = (Sint) con->resultSet->resultSetId;
  else
    INTEGER_DATA(rsId)[0] = (Sint) -1;

  output = NEW_LIST((Sint) 2);
  SET_VECTOR_ELT(output, (Sint) 0, info);
  SET_VECTOR_ELT(output, (Sint) 1, rsId);

  MEM_UNPROTECT(2);
  return output;
}

s_object *
S_DBI_copyFieldDescription(S_DBI_fieldDescription *flds)
{
  S_EVALUATOR

  s_object *field_descriptors;
  char  *desc[]={"name", "Sclass", "type", "len", "precision",
		"scale","nullOK"};
  Stype types[] = {CHARACTER_TYPE, INTEGER_TYPE, INTEGER_TYPE,
		   INTEGER_TYPE, INTEGER_TYPE, INTEGER_TYPE,
		   LOGICAL_TYPE};
  Sint  lengths[7];
  int   i, j, num_fields;

  num_fields = flds->num_fields;
  for(j = 0; j < 7; j++) 
    lengths[j] = num_fields;
  field_descriptors = S_DBI_createNamedList(desc, types, lengths,
					    (Sint) 7);
  field_descriptors = AS_LIST(field_descriptors);

  /* copy contentes in flds to S list */
  for(i = 0; i < num_fields; i++){
    SET_STRING_ELT(VECTOR_ELT(field_descriptors, 0), i,
		   C_S_CPY(flds->name[i]));
    INTEGER_DATA(VECTOR_ELT(field_descriptors, 1))[i] =
	(Sint) flds->Sclass[i];
    INTEGER_DATA(VECTOR_ELT(field_descriptors, 2))[i] =
	(Sint) flds->type[i];
    INTEGER_DATA(VECTOR_ELT(field_descriptors, 3))[i] =
	(Sint) flds->length[i];
    INTEGER_DATA(VECTOR_ELT(field_descriptors, 4))[i] =
	(Sint) flds->precision[i];
    INTEGER_DATA(VECTOR_ELT(field_descriptors, 5))[i] =
		 (Sint) flds->scale[i];
    INTEGER_DATA(VECTOR_ELT(field_descriptors, 6))[i] =
		 (Sint) flds->nullOk[i];
  }

  return(field_descriptors);
} 

s_object *
S_DBI_createNamedList(char **names, Stype *types, Sint *lengths, Sint  n)
{
  S_EVALUATOR
  s_object *output, *output_names, *obj;
  charPtr  nms;
  Sint  num_elem;
  int   j;

  MEM_PROTECT(output = NEW_LIST(n));
  MEM_PROTECT(output_names = NEW_CHARACTER(n));
  for(j = 0; j < n; j++){
    num_elem = lengths[j];
    switch((int)types[j]){
    case LOGICAL_TYPE: 
      obj = NEW_LOGICAL(num_elem);
      break;
    case INTEGER_TYPE:
      obj = NEW_INTEGER(num_elem);
      break;
    case NUMERIC_TYPE:
      obj = NEW_NUMERIC(num_elem);
      break;
    case CHARACTER_TYPE:
      obj = NEW_CHARACTER(num_elem);
      break;
    case LIST_TYPE:
      obj = NEW_LIST(num_elem);
      break;
#ifndef USING_R
    case RAW:
      obj = NEW_RAW(num_elem);
      break;
#endif
    default:
      S_DBI_errorMessage("unsupported data type", S_DBI_ERROR);
    }
    SET_VECTOR_ELT(output, (Sint)j, obj);
    SET_STRING_ELT(output_names, j, C_S_CPY(names[j]));
  }
  SET_NAMES(output, output_names);
  MEM_UNPROTECT(2);
  return(output);
}

#ifdef USING_R
static struct {
    char *str;
    Sint type;
}
/* the following table is from R/src/main/util.c */
DataTypeTable[] = {
    { "NULL",		NILSXP	   },  /* real types */
    { "symbol",		SYMSXP	   },
    { "pairlist",	LISTSXP	   },
    { "closure",	CLOSXP	   },
    { "environment",	ENVSXP	   },
    { "promise",	PROMSXP	   },
    { "language",	LANGSXP	   },
    { "special",	SPECIALSXP },
    { "builtin",	BUILTINSXP },
    { "char",		CHARSXP	   },
    { "logical",	LGLSXP	   },
    { "integer",	INTSXP	   },
    { "double",		REALSXP	   }, /*-  "real", for R <= 0.61.x */
    { "complex",	CPLXSXP	   },
    { "character",	STRSXP	   },
    { "...",		DOTSXP	   },
    { "any",		ANYSXP	   },
    { "expression",	EXPRSXP	   },
    { "list",		VECSXP	   },
    /* aliases : */
    { "numeric",	REALSXP	   },
    { "name",		SYMSXP	   },

    { (char *)0,	-1	   }
};

#else
static struct {
    char *str;
    Sint type;
}
/* the following S type names are from "S.h" */
DataTypeTable[] = {
    { "logical",	LGL	  },
    { "integer",	INT	  },
    { "single",		REAL	  },
    { "numeric",	DOUBLE	  },
    { "character",	CHAR	  },
    { "list",		LIST	  },
    { "complex",	COMPLEX	  },
    { "raw",		RAW	  },
    { "any",		ANY	  },
    { "structure",	STRUCTURE },
    { (char *)0,	-1	  }
};
#endif

char *
S_DBI_dataType2names(Sint t)
{
  int i;
  char buf[512];

  for (i = 0; DataTypeTable[i].str; i++) {
    if (DataTypeTable[i].type == t)
      return DataTypeTable[i].str;
  }
  sprintf(buf, "unknown data type: %d", t);
  S_DBI_errorMessage(buf, S_DBI_ERROR);
  return (char *) 0; /* for -Wall */
}

s_object *
S_DBI_SclassNames(s_object *type)
{
  s_object *typeNames;
  Sint *typeCodes;
  Sint n;
  int i;
  
  n = LENGTH(type);
  typeCodes = INTEGER_DATA(type);
  MEM_PROTECT(typeNames = NEW_CHARACTER(n));
  for(i = 0; i < n; i++) {
    SET_STRING_ELT(typeNames, i,
		   C_S_CPY(S_DBI_dataType2names(typeCodes[i])));
  }
  MEM_UNPROTECT(1);
  return typeNames;
}

static struct {
    char *str;
    Sint type;
}
/* the following type names are from "mysql_com.h" */
FieldTypeTable[] = {
    { "FIELD_TYPE_DECIMAL",	FIELD_TYPE_DECIMAL	},
    { "FIELD_TYPE_TINY",	FIELD_TYPE_TINY		},
    { "FIELD_TYPE_SHORT",	FIELD_TYPE_SHORT	},
    { "FIELD_TYPE_LONG",	FIELD_TYPE_LONG		},
    { "FIELD_TYPE_FLOAT",	FIELD_TYPE_FLOAT	},
    { "FIELD_TYPE_DOUBLE",	FIELD_TYPE_DOUBLE	},
    { "FIELD_TYPE_NULL",	FIELD_TYPE_NULL		},
    { "FIELD_TYPE_TIMESTAMP",   FIELD_TYPE_TIMESTAMP	},
    { "FIELD_TYPE_LONGLONG",	FIELD_TYPE_LONGLONG	},
    { "FIELD_TYPE_INT24",	FIELD_TYPE_INT24	},
    { "FIELD_TYPE_DATE",	FIELD_TYPE_DATE		},
    { "FIELD_TYPE_TIME",	FIELD_TYPE_TIME		},
    { "FIELD_TYPE_DATETIME",	FIELD_TYPE_DATETIME	},
    { "FIELD_TYPE_YEAR",	FIELD_TYPE_YEAR		},
    { "FIELD_TYPE_NEWDATE",	FIELD_TYPE_NEWDATE	},
    { "FIELD_TYPE_ENUM",	FIELD_TYPE_ENUM		},
    { "FIELD_TYPE_SET",		FIELD_TYPE_SET		},
    { "FIELD_TYPE_TINY_BLOB",	FIELD_TYPE_TINY_BLOB	},
    { "FIELD_TYPE_MEDIUM_BLOB",	FIELD_TYPE_MEDIUM_BLOB	},
    { "FIELD_TYPE_LONG_BLOB",	FIELD_TYPE_LONG_BLOB	},
    { "FIELD_TYPE_BLOB",	FIELD_TYPE_BLOB		},
    { "FIELD_TYPE_VAR_STRING",	FIELD_TYPE_VAR_STRING	},
    { "FIELD_TYPE_STRING",	FIELD_TYPE_STRING	},
    { (char *) 0,			-1		}
};

char *
S_MySQL_fieldType2names(Sint t)
{
  int i;
  char buf[512];

  for (i = 0; FieldTypeTable[i].str; i++) {
    if (FieldTypeTable[i].type == t)
      return FieldTypeTable[i].str;
  }
  sprintf(buf, "unknown data type: %d", t);
  S_DBI_errorMessage(buf, S_DBI_ERROR);
  return (char *) 0; /* for -Wall */
}

s_object *
S_MySQL_fieldTypeNames(s_object *type)
{
  s_object *typeNames;
  Sint n;
  Sint *typeCodes;
  int i;
  
  n = LENGTH(type);
  typeCodes = INTEGER_DATA(type);
  MEM_PROTECT(typeNames = NEW_CHARACTER(n));
  for(i = 0; i < n; i++) {
    SET_STRING_ELT(typeNames, i,
		  C_S_CPY(S_MySQL_fieldType2names(typeCodes[i])));
  }
  MEM_UNPROTECT(1);
  return typeNames;
}
