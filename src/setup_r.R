args <- commandArgs(trailingOnly = TRUE)
stopifnot(length(args) == 3L)
library_path <- normalizePath(args[[1L]], mustWork = TRUE)
toolchain_file <- Sys.getenv("BENCHMARK_TOOLCHAIN_FILE", unset = "")
if (nzchar(toolchain_file)) {
  makevars <- Sys.getenv("R_MAKEVARS_USER", unset = "")
  if (!nzchar(makevars) || !file.exists(makevars)) stop("Pinned R compiler configuration is missing")
  if (!identical(Sys.getenv("BENCHMARK_GNU_VERSION"), "14.3.0")) stop("GNU14.3.0 is required")
}
.libPaths(c(library_path, .libPaths()))
repository <- Sys.getenv("RSPM", unset = "")
if (!nzchar(repository)) repository <- "https://cloud.r-project.org"
options(repos = c(CRAN = repository), Ncpus = 1L,
        HTTPUserAgent = sprintf("R/%s R (%s) on GitHub Actions", getRversion(),
          paste(getRversion(), R.version$platform, R.version$arch, R.version$os)))
dependencies <- c("microbenchmark", "jsonlite", "pbapply", "Rcpp", "BH", "RcppArmadillo", "testthat")
missing <- dependencies[!vapply(dependencies, function(package) {
  requireNamespace(package, lib.loc = library_path, quietly = TRUE)
}, logical(1))]
if (length(missing)) {
  message("Installing missing R dependencies: ", paste(missing, collapse = ", "))
  ordinary <- setdiff(missing, c("Rcpp", "RcppArmadillo"))
  compiled <- intersect(missing, c("Rcpp", "RcppArmadillo"))
  if (length(ordinary)) install.packages(ordinary, lib = library_path)
  if (length(compiled)) install.packages(compiled, lib = library_path, type = "source")
} else {
  message("Reusing all cached R dependencies")
}
unavailable <- dependencies[!vapply(dependencies, function(package) {
  requireNamespace(package, lib.loc = library_path, quietly = TRUE)
}, logical(1))]
if (length(unavailable)) stop("R dependencies unavailable after installation: ", paste(unavailable, collapse = ", "))
reuse_compiled <- identical(Sys.getenv("BENCHMARK_R_LIBRARY_CACHE_HIT", unset = ""), "true") &&
  requireNamespace("acousticTS", lib.loc = library_path, quietly = TRUE)
if (reuse_compiled) {
  message("Reusing compiled acousticTS from the exact R library cache")
} else {
  status <- system2(file.path(R.home("bin"), "R"),
    c("CMD", "INSTALL", "--preclean", "--no-multiarch", paste0("--library=", shQuote(library_path)),
      shQuote(normalizePath(args[[2L]], mustWork = TRUE))))
  if (status != 0L) stop("acousticTS installation failed")
}
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
