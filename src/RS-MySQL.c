/* $Id$
 *
 *
 * Copyright (C) 1999 The Omega Project for Statistical Computing.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include "RS-MySQL.h"

/* R and S DataBase Interface to MySQL
 * 
 *
 * C Function library which can be used to run SQL queries from
 * inside of S4, Splus5.x, or R.
 * This driver hooks R/S and MySQL and implements the proposed S-DBI
 * generic R/S-database interface 0.2.
 * 
 * For details see
 *  On S, Appendix A in "Programming with Data" by John M. Chambers. 
 *  On R, "The .Call and .External Interfaces"  in the R manual.
 *  On MySQL, 
       The "MySQL Reference Manual (version 3.23.7 alpha,
 *         02 Dec 1999" and the O'Reilly book "MySQL & mSQL" by Yarger, 
 *         Reese, and King.
 *     Also, "MySQL" by Paul Dubois (2000) New Riders Publishing.
 *
 * TODO:
 *    1. Make sure the code is thread-save, in particular,
 *       we need to remove the PROBLEM ... ERROR macros
 *       in RS_DBI_errorMessage() because it's definetely not 
 *       thread-safe.  But see RS_DBI_setException().
 *    2. Apparently, MySQL treats TEXT as BLOB's (actually, it seems
 *       that MySQL makes no clear distinction between BLOB's and
 *       CLOB's).  Need to resolve this once and for all.
 */


Mgr_Handle *
RS_MySQL_init(s_object *config_params, s_object *reload)
{
  /* Currently we can specify the defaults for 2 parameters, max num of
   * connections, and max of records per fetch (this can be over-ridden
   * explicitly in the S call to fetch).
   */
  S_EVALUATOR

  Mgr_Handle *mgrHandle;
  Sint  fetch_default_rec, force_reload, max_con;
  const char *drvName = "MySQL";

  max_con = INT_EL(config_params,0); 
  fetch_default_rec = INT_EL(config_params,1);
  force_reload = LGL_EL(reload,0);

  mgrHandle = RS_DBI_allocManager(drvName, max_con, fetch_default_rec, 
			     force_reload);
  return mgrHandle;
} 

s_object *
RS_MySQL_closeManager(Mgr_Handle *mgrHandle)
{
  S_EVALUATOR
  RS_DBI_manager *mgr;
  s_object *status;

  mgr = RS_DBI_getManager(mgrHandle);
  if(mgr->num_con)
    RS_DBI_errorMessage("there are opened connections -- close them first",
			RS_DBI_ERROR);

  RS_DBI_freeManager(mgrHandle);

  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status,0) = TRUE;
  MEM_UNPROTECT(1);
  return status;
}

/* open a connection with the same parameters used for in conHandle */
Con_Handle *
RS_MySQL_cloneConnection(Con_Handle *conHandle)
{
  S_EVALUATOR
  Mgr_Handle  *mgrHandle;
  RS_DBI_connection  *con;
  RS_MySQL_conParams *conParams;
  s_object    *con_params, *MySQLgroups;
  char   buf1[256], buf2[256];

  /* get connection params used to open existing connection */
  con = RS_DBI_getConnection(conHandle);
  conParams = con->conParams;

  /* will not used the "group" MySQL config file info (use a dummy one) */
  MEM_PROTECT(MySQLgroups = NEW_CHARACTER((Sint) 1)) ;
  SET_CHR_EL(MySQLgroups,0,C_S_CPY(""));

  mgrHandle = RS_DBI_asMgrHandle(MGR_ID(conHandle));
  
  /* Connection parameters need to be put into a 7-element character
   * vector to be passed to the RS_MySQL_newConnection() function.
   */
  MEM_PROTECT(con_params = NEW_CHARACTER((Sint) 7));
  SET_CHR_EL(con_params,0,C_S_CPY(conParams->user));
  SET_CHR_EL(con_params,1,C_S_CPY(conParams->passwd));
  SET_CHR_EL(con_params,2,C_S_CPY(conParams->host));
  SET_CHR_EL(con_params,3,C_S_CPY(conParams->dbname));
  SET_CHR_EL(con_params,4,C_S_CPY(conParams->unix_socket));
  sprintf(buf1, "%d", conParams->port);
  sprintf(buf2, "%d", conParams->client_flags);
  SET_CHR_EL(con_params,5,C_S_CPY(buf1));
  SET_CHR_EL(con_params,6,C_S_CPY(buf2));
  
  MEM_UNPROTECT(2); 

  return RS_MySQL_newConnection(mgrHandle, con_params, MySQLgroups);
}

RS_MySQL_conParams *
RS_mysql_allocConParams(void)
{
  RS_MySQL_conParams *conParams;

  conParams = (RS_MySQL_conParams *)
     malloc(sizeof(RS_MySQL_conParams));
  if(!conParams){
    RS_DBI_errorMessage("could not malloc space for connection params",
                       RS_DBI_ERROR);
  }
  return conParams;
}

void
RS_MySQL_freeConParams(RS_MySQL_conParams *conParams)
{
  if(conParams->host) free(conParams->host);
  if(conParams->dbname) free(conParams->dbname);
  if(conParams->user) free(conParams->user);
  if(conParams->passwd) free(conParams->passwd);
  if(conParams->unix_socket) free(conParams->unix_socket);
  /* port and client_flags are unsigned ints */
  free(conParams);
  return;
}

Con_Handle *
RS_MySQL_newConnection(Mgr_Handle *mgrHandle, s_object *con_params, 
		       s_object *MySQLgroups)
{
  S_EVALUATOR
  RS_DBI_connection  *con;
  RS_MySQL_conParams *conParams;
  Con_Handle  *conHandle;
  MYSQL     *my_connection;
  unsigned int  p, port, client_flags;
  char      *user = NULL, *passwd = NULL, *host = NULL, *dbname = NULL;
  char      *unix_socket = NULL;
  int       i, argc, option_index;
  Sint      ngroups;
  char      **groups;
  char      **argv;

  if(!is_validHandle(mgrHandle, MGR_HANDLE_TYPE))
    RS_DBI_errorMessage("invalid MySQLManager", RS_DBI_ERROR);

  my_connection = mysql_init(NULL);

  /* Load MySQL default connection values from the [client] and
   * [rs-dbi] sections of the MySQL configuration files.  We
   * recognize options in the [client] and [rs-dbi] sections, plus any
   * other passed from S in the MySQLgroups character vector.  
   * Note that we're faking the argc and argv -- it just simpler (and the
   * recommended way from MySQL perspective).  Re-initialize the
   * getopt_long buffers by setting optind = 0 (defined in getopt.h),
   * this is needed to avoid getopt_long "remembering" options from
   * one invocation to the next.  
   *
   */
  ngroups = GET_LENGTH(MySQLgroups);
  groups = (char **) S_alloc((long) ngroups+3, (int) sizeof(char **));
  groups[0] = RS_DBI_copyString("client");  
  groups[1] = RS_DBI_copyString("rs-dbi");
  groups[ngroups+2] = NULL;       /* required sentinel */
  for(i=0; i<ngroups; i++)
    groups[i+2] = RS_DBI_copyString(CHR_EL(MySQLgroups,i));
  argc = 1;
  argv = (char **) S_alloc((long) 1, (int) sizeof(char **));
  argv[0] = (char *) RS_DBI_copyString("dummy"); 

  load_defaults("my",(const char **) groups, &argc, &argv);
  option_index = optind = 0;
  while(1){
    char c;
    struct option long_options[] = {
      {"host",     required_argument, NULL, 'h'},
      {"user",     required_argument, NULL, 'u'},
      {"password", required_argument, NULL, 'p'},
      {"port",     required_argument, NULL, 'P'},
      {"socket",   required_argument, NULL, 's'},
      {"database", required_argument, NULL, 'd'},
      {0, 0, 0, 0}
    };
    c = getopt_long(argc, argv, "h:u:p:d:P:s:", long_options, &option_index);
    if(c == -1)
      break;
    switch(c){
    case 'h': host = optarg;   break;
    case 'u': user = optarg;   break;
    case 'p': passwd = optarg; break;
    case 'd': dbname = optarg; break;
    case 'P': port = (unsigned int) atoi(optarg); break;
    case 's': unix_socket = optarg;  break;
    }
  }

#define IS_EMPTY(s1)   !strcmp((s1), "")

  /* R/S arguments take precedence over configuration file defaults. */
  if(!IS_EMPTY(CHR_EL(con_params,0)))
    user =  CHR_EL(con_params,0);
  if(!IS_EMPTY(CHR_EL(con_params,1)))
    passwd= CHR_EL(con_params,1);
  if(!IS_EMPTY(CHR_EL(con_params,2)))
    host = CHR_EL(con_params,2);
  if(!IS_EMPTY(CHR_EL(con_params,3)))
    dbname = CHR_EL(con_params,3);
  if(!IS_EMPTY(CHR_EL(con_params,4)))
    unix_socket = CHR_EL(con_params,4);
  p = (unsigned int) atol(CHR_EL(con_params,5));
  port = (p > 0) ? p : 0;
  client_flags = (unsigned int) atol(CHR_EL(con_params,6));

  my_connection = 
    mysql_real_connect(my_connection, host, user, passwd, dbname, 
		       port, unix_socket, client_flags);
  if(!my_connection){
    char buf[512];
    sprintf(buf, "could not connect %s@%s on dbname \"%s\"\n",
	    user, host, dbname);
    RS_DBI_errorMessage(buf, RS_DBI_ERROR);
  }

  conParams = RS_mysql_allocConParams();
  /* connection parameters */
  if(!user) 
     user = ""; 
  conParams->user = RS_DBI_copyString(user);
  if(!passwd) 
     passwd = "";
  conParams->passwd = RS_DBI_copyString(passwd);
  if(!host) 
     host = "";
  conParams->host = RS_DBI_copyString(host);
  if(!dbname) 
     dbname = "";
  conParams->dbname = RS_DBI_copyString(dbname);
  if(!unix_socket) 
     unix_socket = "";
  conParams->unix_socket = RS_DBI_copyString(unix_socket);
  conParams->port = port;
  conParams->client_flags = client_flags;

  /* MySQL connections can only have 1 result set open at a time */  
  conHandle = RS_DBI_allocConnection(mgrHandle, (Sint) 1); 
  con = RS_DBI_getConnection(conHandle);
  if(!con){
    mysql_close(my_connection);
    RS_MySQL_freeConParams(conParams);
    conParams = (RS_MySQL_conParams *) NULL;
    RS_DBI_errorMessage("could not alloc space for connection object",
                       RS_DBI_ERROR);
  }
  con->drvConnection = (void *) my_connection;
  con->conParams = (void *) conParams;

  return conHandle;
}

s_object *
RS_MySQL_closeConnection(Con_Handle *conHandle)
{
  S_EVALUATOR
  RS_DBI_connection *con;
  MYSQL *my_connection;
  s_object *status;

  con = RS_DBI_getConnection(conHandle);
  if(con->num_res>0){
    RS_DBI_errorMessage(
     "close the pending result sets before closing this connection",
     RS_DBI_ERROR);
  }
  /* make sure we first free the conParams and mysql connection from
   * the RS-RBI connection object.
   */
  if(con->conParams){
     RS_MySQL_freeConParams(con->conParams);
     con->conParams = (RS_MySQL_conParams *) NULL;
  }
  my_connection = (MYSQL *) con->drvConnection;
  mysql_close(my_connection);
  con->drvConnection = (void *) NULL;

  RS_DBI_freeConnection(conHandle);
  
  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status, 0) = TRUE;
  MEM_UNPROTECT(1);

  return status;
}  
  
/* Execute (currently) one sql statement (INSERT, DELETE, SELECT, etc.),
 * set coercion type mappings between the server internal data types and 
 * S classes.   Returns  an S handle to a resultSet object.
 */
Res_Handle *
RS_MySQL_exec(Con_Handle *conHandle, s_object *statement)
{
  S_EVALUATOR

  RS_DBI_connection *con;
  Res_Handle        *rsHandle;
  RS_DBI_resultSet  *result;
  MYSQL             *my_connection;
  MYSQL_RES         *my_result;
  unsigned int   num_fields;
  int      state;
  Sint     res_id, is_select;
  char     *dyn_statement;

  con = RS_DBI_getConnection(conHandle);
  my_connection = (MYSQL *) con->drvConnection;
  dyn_statement = RS_DBI_copyString(CHR_EL(statement,0));

  /* Do we have a pending resultSet in the current connection?  
   * MySQL only allows  one resultSet per connection.
   */
  if(con->num_res>0){
    res_id = (Sint) con->resultSetIds[0]; /* recall, MySQL has only 1 res */
    rsHandle = RS_DBI_asResHandle(MGR_ID(conHandle), 
	                          CON_ID(conHandle),
				  res_id);
    result = RS_DBI_getResultSet(rsHandle);
    if(result->completed == 0){
      free(dyn_statement);
      RS_DBI_errorMessage( 
       "connection with pending rows, close resultSet before continuing",
       RS_DBI_ERROR);
    }
    else 
      RS_MySQL_closeResultSet(rsHandle);
  }

  /* Here is where we actually run the query */
  state = mysql_query(my_connection, dyn_statement);
  if(state) { 
    char errMsg[256];
    free(dyn_statement);
    (void) sprintf(errMsg, "could not run statement: %s",
		   mysql_error(my_connection));
    RS_DBI_errorMessage(errMsg, RS_DBI_ERROR);
  }
  /* Do we need output column/field descriptors?  Only for SELECT-like
   * statements. The MySQL reference manual suggests invoking
   * mysql_use_result() and if it succeed the statement is SELECT-like
   * that can use a resultSet.  Otherwise call mysql_field_count()
   * and if it returns zero, the sql was not a SELECT-like statement.
   * Finally a non-zero means a failed SELECT-like statement.
   */
  my_result = mysql_use_result(my_connection);
  if(!my_result)
    my_result = (MYSQL_RES *) NULL;
    
  num_fields = mysql_field_count(my_connection);
  is_select = (Sint) TRUE;
  if(!my_result){
    if(num_fields>0){
      free(dyn_statement);
      RS_DBI_errorMessage("error in select/select-like", RS_DBI_ERROR);
    }
    else 
      is_select = FALSE;
  }

  /* we now create the wrapper and copy values */
  rsHandle = RS_DBI_allocResultSet(conHandle);
  result = RS_DBI_getResultSet(rsHandle);
  result->statement = RS_DBI_copyString(dyn_statement);
  result->drvResultSet = (void *) my_result;
  result->rowCount = (Sint) 0;
  result->isSelect = is_select;
  if(!is_select){
    result->rowsAffected = (Sint) mysql_affected_rows(my_connection);
    result->completed = 1;
   }
  else {
    result->rowsAffected = (Sint) -1;
    result->completed = 0;
  }
  
  if(is_select)
    result->fields = RS_MySQL_createDataMappings(rsHandle);

  free(dyn_statement);
  return rsHandle;
}

RS_DBI_fields *
RS_MySQL_createDataMappings(Res_Handle *rsHandle)
{
  MYSQL_RES     *my_result;
  MYSQL_FIELD   *select_dp;
  RS_DBI_connection  *con;
  RS_DBI_resultSet   *result; 
  RS_DBI_fields      *flds;
  int     j, num_fields, internal_type;
  char    errMsg[128];

  result = RS_DBI_getResultSet(rsHandle);
  my_result = (MYSQL_RES *) result->drvResultSet;

  /* here we fetch from the MySQL server the field descriptions */
  select_dp = mysql_fetch_fields(my_result);

  con = RS_DBI_getConnection(rsHandle);
  num_fields = (int) mysql_field_count((MYSQL *) con->drvConnection);

  flds = RS_DBI_allocFields(num_fields);

  /* WARNING: TEXT fields are represented as BLOBS (sic),
   * not VARCHAR or some kind of string type. More troublesome is the
   * fact that a TEXT fields can be BINARY to indicate case-sensitivity.
   * The bottom line is that MySQL has a serious deficiency re: text
   * types (IMHO).  A binary object (in SQL92, X/SQL at least) can
   * store all kinds of non-ASCII, non-printable stuff that can
   * potentially screw up S and R CHARACTER_TYPE.  We are on thin ice.
   * 
   * I'm aware that I'm introducing a potential bug here by following
   * the MySQL convention of treating BLOB's as TEXT (I'm symplifying
   * in order to properly handle commonly-found TEXT fields, at the 
   * risk of core dumping when bona fide Binary objects are being
   * retrieved.
   * 
   * Possible workaround: if strlen() of the field equals the
   *    MYSQL_FIELD->length for all rows, then we are probably(?) safe
   * in considering TEXT a character type (non-binary).
   */
  for (j = 0; j < num_fields; j++){

    /* First, save the name, MySQL internal field name, type, length, etc. */
    
    flds->name[j] = RS_DBI_copyString(select_dp[j].name);
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
      flds->isVarLength[j] = (Sint) 1;
      break;
    case FIELD_TYPE_TINY:
    case FIELD_TYPE_SHORT:
      flds->Sclass[j] = INTEGER_TYPE;
      break;
    case FIELD_TYPE_LONG:            /* are we sure we can fit these */
    case FIELD_TYPE_LONGLONG:        /* in S ints? */
    case FIELD_TYPE_INT24:           
      if(flds->length[j] <= sizeof(Sint))
	flds->Sclass[j] = INTEGER_TYPE;
      else 
	flds->Sclass[j] = NUMERIC_TYPE;
    case FIELD_TYPE_DECIMAL:
      if(flds->scale[j] > 0 || flds->precision[j] > 10)
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
      flds->Sclass[j] = CHARACTER_TYPE;   /* Grr! Hate this! */
      flds->isVarLength[j] = (Sint) 1;
      break;
    case FIELD_TYPE_DATE:
    case FIELD_TYPE_TIME:
    case FIELD_TYPE_DATETIME:
    case FIELD_TYPE_YEAR:
    case FIELD_TYPE_NEWDATE:
      flds->Sclass[j] = CHARACTER_TYPE;
      flds->isVarLength[j] = (Sint) 1;
      break;
    case FIELD_TYPE_ENUM:
      flds->Sclass[j] = CHARACTER_TYPE;   /* see the MySQL ref. manual */
      flds->isVarLength[j] = (Sint) 1;
      break;
    case FIELD_TYPE_SET:
      flds->Sclass[j] = CHARACTER_TYPE;
      flds->isVarLength[j] = (Sint) 0;
      break;
    default:
      flds->Sclass[j] = CHARACTER_TYPE;
      flds->isVarLength[j] = (Sint) 1;
      (void) sprintf(errMsg, 
		     "unrecognized MySQL field type %d in column %d", 
		     internal_type, j);
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
      break;
    }
  }  
  return flds;
}

s_object *    /* output is a named list */
RS_MySQL_fetch(s_object *rsHandle, s_object *max_rec)
{
  S_EVALUATOR
  RS_DBI_manager   *mgr;
  RS_DBI_resultSet *result;
  RS_DBI_fields    *flds;
  MYSQL_RES *my_result;
  MYSQL_ROW  row;
  s_object  *output;
  s_object  *raw_obj;
  s_object  *raw_container;
  unsigned long  *lens;
  int    i, j, null_item, expand;
  Sint   *fld_nullOk, completed;
  Stype  *fld_Sclass;
  Sint   num_rec, num_fields;

  result = RS_DBI_getResultSet(rsHandle);
  flds = result->fields;
  if(!flds)
    RS_DBI_errorMessage("corrupt resultSet, missing fieldDescription",
		       RS_DBI_ERROR);
  num_rec = INT_EL(max_rec,0);
  expand = (num_rec < 0);   /* dyn expand output to accommodate all rows*/
  if(expand || num_rec == 0){
    mgr = RS_DBI_getManager(rsHandle);
    num_rec = mgr->fetch_default_rec;
  }
  num_fields = flds->num_fields;
  MEM_PROTECT(output = NEW_LIST((Sint) num_fields));
  RS_DBI_allocOutput(output, flds, num_rec, 0);
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc output list",
			RS_DBI_ERROR);
  fld_Sclass = flds->Sclass;
  fld_nullOk = flds->nullOk;
  
  /* actual fetching....*/
  my_result = (MYSQL_RES *) result->drvResultSet;
  completed = (Sint) 0;
  for(i = 0; ; i++){ 
    if(i==num_rec){  /* exhausted the allocated space */
      if(expand){    /* do we extend or return the records fetched so far*/
	num_rec = 2 * num_rec;
	RS_DBI_allocOutput(output, flds, num_rec, expand);
	if(IS_LIST(output))
	  output = AS_LIST(output);
	else
	  RS_DBI_errorMessage("internal error: could not alloc output list",
			      RS_DBI_ERROR);
      }
      else
	break;       /* okay, no more fetching for now */
    }
    row = mysql_fetch_row(my_result);
    if(row==NULL){    /* either we finish or we encounter an error */
      unsigned int  err_no;
      RS_DBI_connection   *con;
      con = RS_DBI_getConnection(rsHandle);
      err_no = mysql_errno((MYSQL *) con->drvConnection);
      completed = (Sint) (err_no ? -1 : 1);
      break;
    }
    lens = mysql_fetch_lengths(my_result); 
    for(j = 0; j < num_fields; j++){

      null_item = (row[j] == NULL);
      switch((int)fld_Sclass[j]){
      case INTEGER_TYPE:
	if(null_item)
	  NA_SET(&(LST_INT_EL(output,j,i)), INTEGER_TYPE);
	else
	  LST_INT_EL(output,j,i) = atol(row[j]);
	break;
      case CHARACTER_TYPE:
	/* BUG: I need to verify that a TEXT field (which is stored as
	 * a BLOB by MySQL!) is indeed char and not a true
	 * Binary obj (MySQL does not truly distinguish them). This
	 * test is very grosse.
	 */
	if(null_item)
#ifdef USING_R
	  SET_LST_CHR_EL(output,j,i,NA_STRING);
#else
	  NA_CHR_SET(LST_CHR_EL(output,j,i));
#endif
	else {
	  if((size_t) lens[j] != strlen(row[j])){
	    char warn[128];
	    (void) sprintf(warn, 
			   "internal error: row %ld field %ld truncated",
			   (long) i, (long) j);
	    RS_DBI_errorMessage(warn, RS_DBI_WARNING);
	  }
	  SET_LST_CHR_EL(output,j,i,C_S_CPY(row[j]));
	}
	break;
      case NUMERIC_TYPE:
	if(null_item)
	  NA_SET(&(LST_NUM_EL(output,j,i)), SINGLE_TYPE);
	else
	  LST_NUM_EL(output,j,i) = (double) atof(row[j]);
	break;
#ifndef USING_R
      case SINGLE_TYPE:
	if(null_item)
	  NA_SET(&(LST_FLT_EL(output,j,i)), SINGLE_TYPE);
	else
	  LST_FLT_EL(output,j,i) = (float) atof(row[j]);
	break;
      case RAW_TYPE:           /* these are blob's */
	raw_obj = NEW_RAW((Sint) lens[j]);
	memcpy(RAW_DATA(raw_obj), row[j], lens[j]);
	raw_container = LST_EL(output,j);    /* get list of raw objects*/
	SET_ELEMENT(raw_container, (Sint) i, raw_obj); 
	SET_ELEMENT(output, (Sint) j, raw_container);
  	break;
#endif
      default:  /* error, but we'll try the field as character (!)*/
	if(null_item)
#ifdef USING_R
	  SET_LST_CHR_EL(output,j,i, NA_STRING);
#else
	  NA_CHR_SET(LST_CHR_EL(output,j,i));
#endif
	else {
	    char warn[64];
	    (void) sprintf(warn, 
			   "unrecognized field type %d in column %d",
			   (int) fld_Sclass[j], (int) j);
	    RS_DBI_errorMessage(warn, RS_DBI_WARNING);
	    SET_LST_CHR_EL(output,j,i,C_S_CPY(row[j]));
	  }
	  break;
      } 
    } 
  }
  
  /* actual number of records fetched */
  if(i < num_rec){
    num_rec = i;
    /* adjust the length of each of the members in the output_list */
    for(j = 0; j<num_fields; j++)
      SET_LENGTH(LST_EL(output,j), num_rec);
  }
  if(completed < 0)
    RS_DBI_errorMessage("error while fetching rows", RS_DBI_WARNING);

  result->rowCount += num_rec;
  result->completed = (int) completed;

  MEM_UNPROTECT(1);
  return output;
}

/* return a 2-elem list with the last exception number and
 * exception message on a given connection.
 */
s_object *
RS_MySQL_getException(s_object *conHandle)
{
  S_EVALUATOR

  MYSQL *my_connection;
  s_object  *output;
  RS_DBI_connection   *con;
  Sint  n = 2;
  char *exDesc[] = {"errorNum", "errorMsg"};
  Stype exType[] = {INTEGER_TYPE, CHARACTER_TYPE};
  Sint  exLen[]  = {1, 1};

  con = RS_DBI_getConnection(conHandle);
  if(!con->drvConnection)
    RS_DBI_errorMessage("internal error: corrupt connection handle",
			RS_DBI_ERROR);
  output = RS_DBI_createNamedList(exDesc, exType, exLen, n);
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not allocate named list",
			RS_DBI_ERROR);
  my_connection = (MYSQL *) con->drvConnection;
  LST_INT_EL(output,0,0) = (Sint) mysql_errno(my_connection);
  SET_LST_CHR_EL(output,1,0,C_S_CPY(mysql_error(my_connection)));

  return output;
}

s_object *
RS_MySQL_closeResultSet(s_object *resHandle)
{
  S_EVALUATOR 
  RS_DBI_resultSet *result;
  MYSQL_RES        *my_result;
  s_object *status;

  result = RS_DBI_getResultSet(resHandle);

  my_result = (MYSQL_RES *) result->drvResultSet;
  if(my_result){
    /* we need to flush any possibly remaining rows (see Manual Ch 20 p358) */
    MYSQL_ROW row;
    while((row = mysql_fetch_row(result->drvResultSet)))
      ;
  }
  mysql_free_result(my_result);

  /* need to NULL drvResultSet, otherwise can't free the rsHandle */
  result->drvResultSet = (void *) NULL;
  RS_DBI_freeResultSet(resHandle);

  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status, 0) = TRUE;
  MEM_UNPROTECT(1);

  return status;
}

s_object *
RS_MySQL_managerInfo(Mgr_Handle *mgrHandle)
{
  S_EVALUATOR
  RS_DBI_manager *mgr;
  s_object *output;
  Sint i, num_con, max_con, *cons, ncon;
  Sint j, n = 8;
  char *mgrDesc[] = {"drvName",   "connectionIds", "fetch_default_rec",
                     "managerId", "length",        "num_con", 
                     "counter",   "clientVersion"};
  Stype mgrType[] = {CHARACTER_TYPE, INTEGER_TYPE, INTEGER_TYPE, 
                     INTEGER_TYPE,   INTEGER_TYPE, INTEGER_TYPE, 
                     INTEGER_TYPE,   CHARACTER_TYPE};
  Sint  mgrLen[]  = {1, 1, 1, 1, 1, 1, 1, 1};
  
  mgr = RS_DBI_getManager(mgrHandle);
  if(!mgr)
    RS_DBI_errorMessage("driver not loaded yet", RS_DBI_ERROR);
  num_con = (Sint) mgr->num_con;
  max_con = (Sint) mgr->length;
  mgrLen[1] = num_con;

  output = RS_DBI_createNamedList(mgrDesc, mgrType, mgrLen, n);
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list", 
			RS_DBI_ERROR);
  j = (Sint) 0;
  if(mgr->drvName)
    SET_LST_CHR_EL(output,j++,0,C_S_CPY(mgr->drvName));
  else 
    SET_LST_CHR_EL(output,j++,0,C_S_CPY(""));

  cons = (Sint *) S_alloc((long)max_con, (int)sizeof(Sint));
  ncon = RS_DBI_listEntries(mgr->connectionIds, mgr->length, cons);
  if(ncon != num_con){
    RS_DBI_errorMessage(
	  "internal error: corrupt RS_DBI connection table",
	  RS_DBI_ERROR);
  }
  for(i = 0; i < num_con; i++)
    LST_INT_EL(output, j, i) = cons[i];
  j++;
  LST_INT_EL(output,j++,0) = mgr->fetch_default_rec;
  LST_INT_EL(output,j++,0) = mgr->managerId;
  LST_INT_EL(output,j++,0) = mgr->length;
  LST_INT_EL(output,j++,0) = mgr->num_con;
  LST_INT_EL(output,j++,0) = mgr->counter;
  SET_LST_CHR_EL(output,j++,0,C_S_CPY(mysql_get_client_info()));

  return output;
}

s_object *
RS_MySQL_connectionInfo(Con_Handle *conHandle)
{
  S_EVALUATOR
  
  MYSQL   *my_con;
  RS_MySQL_conParams *conParams;
  RS_DBI_connection  *con;
  s_object   *output;
  Sint       i, n = 8, *res, nres;
  char *conDesc[] = {"host", "user", "dbname", "conType",
		     "serverVersion", "protocolVersion",
		     "threadId", "rsId"};
  Stype conType[] = {CHARACTER_TYPE, CHARACTER_TYPE, CHARACTER_TYPE,
		      CHARACTER_TYPE, CHARACTER_TYPE, INTEGER_TYPE,
		      INTEGER_TYPE, INTEGER_TYPE};
  Sint  conLen[]  = {1, 1, 1, 1, 1, 1, 1, 1};

  con = RS_DBI_getConnection(conHandle);
  conLen[7] = con->num_res;         /* num of open resultSets */
  my_con = (MYSQL *) con->drvConnection;
  output = RS_DBI_createNamedList(conDesc, conType, conLen, n);
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list",
			RS_DBI_ERROR);
  conParams = (RS_MySQL_conParams *) con->conParams;
  SET_LST_CHR_EL(output,0,0,C_S_CPY(conParams->host));
  SET_LST_CHR_EL(output,1,0,C_S_CPY(conParams->user));
  SET_LST_CHR_EL(output,2,0,C_S_CPY(conParams->dbname));
  SET_LST_CHR_EL(output,3,0,C_S_CPY(mysql_get_host_info(my_con)));
  SET_LST_CHR_EL(output,4,0,C_S_CPY(mysql_get_server_info(my_con)));

  LST_INT_EL(output,5,0) = (Sint) mysql_get_proto_info(my_con);
  LST_INT_EL(output,6,0) = (Sint) mysql_thread_id(my_con);

  res = (Sint *) S_alloc( (long) con->length, (int) sizeof(Sint));
  nres = RS_DBI_listEntries(con->resultSetIds, con->length, res);
  if(nres != con->num_res){
    RS_DBI_errorMessage(
	  "internal error: corrupt RS_DBI resultSet table",
	  RS_DBI_ERROR);
  }
  for( i = 0; i < con->num_res; i++){
    LST_INT_EL(output,7,i) = (Sint) res[i];
  }

  return output;

}
s_object *
RS_MySQL_resultSetInfo(Res_Handle *rsHandle)
{
  S_EVALUATOR

  RS_DBI_resultSet   *result;
  s_object  *output, *flds;
  Sint  n = 6;
  char  *rsDesc[] = {"statement", "isSelect", "rowsAffected",
		     "rowCount", "completed", "fieldDescription"};
  Stype rsType[]  = {CHARACTER_TYPE, INTEGER_TYPE, INTEGER_TYPE,
		     INTEGER_TYPE,   INTEGER_TYPE, LIST_TYPE};
  Sint  rsLen[]   = {1, 1, 1, 1, 1, 1};

  result = RS_DBI_getResultSet(rsHandle);
  if(result->fields)
    flds = RS_DBI_getFieldDescriptions(result->fields);
  else
    flds = S_NULL_ENTRY;

  output = RS_DBI_createNamedList(rsDesc, rsType, rsLen, n);
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list",
			RS_DBI_ERROR);
  SET_LST_CHR_EL(output,0,0,C_S_CPY(result->statement));
  LST_INT_EL(output,1,0) = result->isSelect;
  LST_INT_EL(output,2,0) = result->rowsAffected;
  LST_INT_EL(output,3,0) = result->rowCount;
  LST_INT_EL(output,4,0) = result->completed;
  if(flds != S_NULL_ENTRY)
     SET_ELEMENT(LST_EL(output, 5), (Sint) 0, flds);

  return output;
}


char *
RS_MySQL_getFieldTypeName(Sint t)
{
  int i;
  char buf[512];

  for (i = 0; RS_MySQL_fieldTypes[i].typeName; i++) {
    if (RS_MySQL_fieldTypes[i].typeId == t)
      return RS_MySQL_fieldTypes[i].typeName;
  }
#ifdef USING_R
  sprintf(buf, "unknown data type: %d", t);
#else
  sprintf(buf, "unknown data type: %ld", t);
#endif
  RS_DBI_errorMessage(buf, RS_DBI_ERROR);
  return (char *) 0; /* for -Wall */
}

s_object *
RS_MySQL_getFieldTypeNames(s_object *type)
{
  s_object *typeNames;
  Sint n;
  Sint *typeCodes;
  int i;
  
  n = LENGTH(type);
  typeCodes = INTEGER_DATA(type);
  MEM_PROTECT(typeNames = NEW_CHARACTER(n));
  for(i = 0; i < n; i++) {
    SET_CHR_EL(typeNames,i,C_S_CPY(RS_MySQL_getFieldTypeName(typeCodes[i])));
  }
  MEM_UNPROTECT(1);
  return typeNames;
}
