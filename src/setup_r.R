args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 3L)
library_path <- normalizePath(args[[1L]], mustWork = TRUE)
.libPaths(c(library_path, .libPaths()))
options(repos = c(CRAN = "https://cloud.r-project.org"), Ncpus = 1L)
dependencies <- c("jsonlite", "pbapply", "Rcpp", "BH", "RcppArmadillo", "testthat")
install.packages(dependencies, lib = library_path)
status <- system2(file.path(R.home("bin"), "R"),
  c("CMD", "INSTALL", "--preclean", "--no-multiarch", paste0("--library=", shQuote(library_path)),
    shQuote(normalizePath(args[[2L]], mustWork = TRUE))))
if (status != 0L) stop("acousticTS installation failed")
packages <- as.data.frame(installed.packages(lib.loc = library_path)[,
  c("Package", "Version", "Built"), drop = FALSE], stringsAsFactors = FALSE)
compiler_keys <- c("CC", "CFLAGS", "CXX17", "CXX17STD", "CXX17FLAGS", "FC", "FFLAGS", "FLIBS")
compiler_config <- setNames(lapply(compiler_keys, function(key) {
  paste(system2(file.path(R.home("bin"), "R"), c("CMD", "config", key),
                stdout = TRUE, stderr = TRUE), collapse = "\n")
}), compiler_keys)
# Exercise the public precision request once during installation. A successful
# double-only build must not silently stand in for the requested quad protocol.
quad_probe <- tryCatch({
  shape <- acousticTS::prolate_spheroid(semimajor_length = 0.001,
                                       semiminor_length = 0.0005)
  object <- acousticTS::fls_generate(shape = shape, g_body = 1, h_body = 1)
  result <- acousticTS::target_strength(object, frequency = 1000, model = "psms",
    boundary = "fixed_rigid", precision = "quad", adaptive = TRUE,
    density_sw = 1026.8, sound_speed_sw = 1477.3)
  if (!is.finite(acousticTS::extract(result, "model")$PSMS$TS[[1L]]))
    stop("Native quad probe returned nonfinite target strength")
  list(available = TRUE, message = "Public quad-precision calculation succeeded")
}, error = function(error) {
  list(available = FALSE, message = conditionMessage(error))
})
jsonlite::write_json(list(R = R.version.string, packages = packages,
                    compiler_config = compiler_config,
                    compiler_environment = as.list(Sys.getenv(c("CC", "CFLAGS", "CXX17",
                      "CXX17FLAGS", "FC", "FFLAGS", "FCFLAGS", "R_MAKEVARS_USER"))),
                    capabilities = list(quad = quad_probe)), args[[3L]],
                    auto_unbox = TRUE, pretty = TRUE)
