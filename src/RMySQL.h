/*
 *  $Id$
*/

#include "S4R.h"

#ifdef USING_R
#undef INTEGER_DATA
#define INTEGER_DATA(x) (INTEGER(x))
#endif

#define S_NULL_ENTRY  NULL_ENTRY

#define S_DBI_MESSAGE     1
#define S_DBI_WARNING     2
#define S_DBI_ERROR       3
#define S_DBI_TERMINATE   4
#define S_DBI_STATEMENT_LEN 10000  /* dynamic statement length */
#define S_DBI_MAX_ITEMS       100  /* max items in select SQL92 */
#define S_DBI_MAX_VNAME_LEN    30  /* name len (ANSI SQL92 max is 18) */
#define S_DBI_MAX_NUM_CURSORS 100  /* SQL92 max is 10 */
#define S_DBI_ERR_MSG_LEN    1024  /* for Oracle's sake */

#include <mysql.h>
#include <mysql_com.h>

#define S_MYSQL_MAX_CON       100 

/* The following fully describes the fields output by a select
 * (or select-like) statement, and the mappings from the internal
 * database types to S classes.  This structure contains the info we need
 * to build the S list (or data.frame) that will receive the SQL
 * output.  It still needs some work to handle arbitrty BLOB's (namely
 * methods to map BLOBs into user-defined S objects).
 */
typedef struct st_sdbi_fieldDescription {
  int num_fields;
  char  **name;   
  Sint  *type;   Sint  *length;   Sint  *precision;
  Sint  *scale;  Sint  *nullOk;   Stype *Sclass;
} S_DBI_fieldDescription;

/* The S-MySQL resultSet consists of the actual MySQL resultSet 
 * plus the fields defined by the S-DBI implementation.
 */

typedef struct st_sdbi_mysql_resultSet {
  MYSQL_RES  *my_resultSet; 
  pid_t processId;                /* the 3 *Id's are used for   */
  int   connectionId;             /* validating stuff coming from S */
  int   resultSetId;  
  char  *statement;
  Sint  rowsAffected;             /* used by non-SELECT statements */
  Sint  rowCount;                 /* rows fetched so far (SELECT-types)*/
  int   completed;                /* have we fetched all rows? */
  S_DBI_fieldDescription *fieldDescription;
} S_MySQL_resultSet;

/* An S-MySQL connection consists of an actual MySQL connection plus
 * a resultSet and other goodies used by the S-DBI implementation.
 */

typedef struct st_sdbi_connection {
  MYSQL  *my_connection;             /* actual MySQL connection */
  pid_t  processId;
  int    connectionId;
  char   *host;
  char   *dbname;
  char   *user;
  /* Currently we allow only one resultSet per connection, but
   * eventually we may allow an array of resultSets. [Does MySQL
   * allows multiple open cursors per connection?].
   */
  int    counter;                    /* total number of queries */
  S_MySQL_resultSet  *resultSet; 
} S_MySQL_connection;

/* This connection table provides the storage for the connection
 * manager. We may replace it with a hash table, if the need arises.
 */
typedef struct  st_sdbi_connTable {
  S_MySQL_connection **connections;
  int *connectionIds; 
  Sint fetch_default_rec;        /* default num of records per fetch */
  pid_t processId;
  int length;
  int num_con;                   /* number of opened connections */
  int counter;                   /* total number of connections handled */
} S_MySQL_connectionTable;


/* The following functions are the S/R entry into the C implementation;
 * we use the prefix "S_MySQL" in functions to denote this,
 * (but note that defined types also have the S_MySQL prefix).
 * They are mostly wrappers to the underlying C (lower-case) S_mysql API.
 */
s_object *S_MySQL_init();           
s_object *S_MySQL_close();          
s_object *S_MySQL_newConnection();  
s_object *S_MySQL_closeConnection();
s_object *S_MySQL_exec();           
s_object *S_MySQL_fetch();          
s_object *S_MySQL_closeResultSet();  
s_object *S_MySQL_getException();   
s_object *S_MySQL_validHandle();    
s_object *S_MySQL_resultSetInfo();      
s_object *S_MySQL_connectionInfo();
s_object *S_MySQL_managerInfo();
s_object *S_DBI_copyFieldDescription(); 

/* All the remaining functions are internal to the C implementation;
 * (we use the prefix "S_mysql" to denote this).
 */
void                S_mysql_closeResultSet(); 
void                S_mysql_closeConnection();  
S_MySQL_resultSet  *S_mysql_createResultSet();  
S_MySQL_resultSet  *S_DBI_resolveRSHandle(); 
S_MySQL_connection *S_DBI_resolveConHandle();
S_DBI_fieldDescription  *S_mysql_createDataMappings();


/* The following roughly implement a connection Manager, namely,
 * these functions track connections in a connectionTable (adding,
 * removing, looking up, validating, etc.)  The table is assumed to be
 * smallish (certainly no more than 100 entries, but more likely only
 * a handful) thus, this simple implementation.  We may re-implement
 * this using a hash table at some point.
 */
S_MySQL_connection *S_DBI_getConnection();   
int       S_DBI_assignConnection();   
void      S_DBI_removeConnectionId(); 
int       S_DBI_lookupConnectionId(); 

/* The following are helper functions, possibly useful when
 * implementing S/R interfaces to other databases (already used in
 * the S-Oracle driver).
 */
void      S_DBI_errorMessage();
s_object *S_DBI_createNamedList();
char     *S_DBI_copyString();
