## 
## $Id: zzz.R 205 2006-10-25 19:59:20Z sethf $
##

".conflicts.OK" <- TRUE

".First.lib" <- 
function(libname, pkgname) 
{
   library.dynam("RMySQL", pkgname, libname)
}
