# R CMD check's static analyzer can't see through mirai's expression
# substitution — `run_flush`, `path`, and `payload` in the body of the
# `mirai::mirai(...)` call in async.R are bound via named arguments, not
# as lexical variables. Declare them as known globals to silence the note.
utils::globalVariables(c("run_flush", "path", "payload"))
