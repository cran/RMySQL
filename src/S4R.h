#ifndef S4R_H
#define S4R_H

#include "S.h"

/*
 * Some of these come from MASS, some from packages developed under
 * the Omega project.
 */

#if (defined(SPLUS_VERSION) && SPLUS_VERSION >= 5000 )
#  define USING_SP5
#endif

#if (defined(S_VERSION) && S_VERSION == 4-M)
#  define USING_S4
#endif

#if ( defined(USING_S4) || defined(USING_SP5) )
#  define RANDIN  seedin((long *)NULL, S_evaluator)
#  define RANDOUT seedout((long *)NULL, S_evaluator)
#  define UNIF unif_rand(S_evaluator)
#else
#  define RANDIN  seed_in((long *)NULL)
#  define RANDOUT seed_out((long *)NULL)
#  define UNIF unif_rand()
#endif

#ifdef USING_R
#  include "Rdefines.h"
#define singl double
#define Sint int
#define charPtr SEXP *
#  define CHAR_DEREF(x) CHAR(x)
#  define C_S_CPY(p)    COPY_TO_USER_STRING(p)  /* cpy C string to R */
/* #define RAW_DATA(p)   (RAW_POINTER(p)) */ /* R do not support a raw type */
#  define MEM_PROTECT(x) PROTECT(x)
#  define MEM_UNPROTECT(n) UNPROTECT(n)
#  define MEM_UNPROTECT_PTR(x) UNPROTECT_PTR(x)
#elif (defined(USING_S4) || defined(USING_SP5))
#define singl float
#define  Sint long
#define  charPtr char **
#  define CHAR_DEREF(x) x
#  define C_S_CPY(p)    c_s_cpy((p), S_evaluator)  /* cpy C string to S */
#  define RAW_DATA(p)   (RAW_POINTER(p)) /* this is missing in S4 S.h */
#  define MEM_PROTECT(x) /**/
#  define MEM_UNPROTECT(n) /**/
#  define MEM_UNPROTECT_PTR(x) /**/
#endif

#if (defined(USING_S4) && !defined(USING_SP5))
     /* S4-M do not define these */
#    define F77_CALL(x)    F77_SUB(x)
#    define F77_NAME(x)    F77_SUB(x)
#    define F77_COM(x)     F77_SUB(x)
#    define F77_COMDECL(x) F77_COM(x)
#endif

#if ( defined(USING_R) || defined(USING_S4))
#  define LIST_DATA(p)  (LIST_POINTER(p))  /* this is missing in S4 S.h */
#else /* using S3 */
#  define S_EVALUATOR
#endif

/* types common to R and S4 */
#if defined(USING_R)
#  define Stype SEXPTYPE
#  define	LOGICAL_TYPE	LGLSXP
#  define	INTEGER_TYPE	INTSXP
#  define	NUMERIC_TYPE	REALSXP
#  define       CHARACTER_TYPE	STRSXP
#  define	COMPLEX_TYPE	CPLXSXP
#  define       LIST_TYPE	VECSXP
/* #  define	RAW_TYPE	RAWSXP */ /* no raw type in R now */
#elif ( defined(USING_S4) || defined(USING_SP5) )
#  define Stype int
#  define	LOGICAL_TYPE	LGL
#  define	INTEGER_TYPE	INT
#  define	NUMERIC_TYPE	DOUBLE
#  define       CHARACTER_TYPE	CHAR
#  define	COMPLEX_TYPE	COMPLEX
#  define       LIST_TYPE	LIST
#  define	RAW_TYPE	RAW
#endif

#endif /* S4R_H */
