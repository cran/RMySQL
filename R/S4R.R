new <- function(classname, ...)
{
  class(classname) <- classname
  UseMethod("new")
}

new.default <- function(classname, ...)
  structure(list(...), class = unclass(classname))

as <- function(object, classname)
  get(paste("as", as.character(classname), sep = "."))(object)
