# One fresh public PSMS calculation. Runtime startup and a distinct warmup case
# are outside elapsed_seconds. The supervisor owns process-tree timeouts.

psms_controls <- function(request) {
  controls <- modifyList(list(precision = "quad", adaptive = TRUE,
                              simplify_Amn = FALSE), request$controls)
  allowed <- c("precision", "adaptive", "simplify_Amn", "n_integration")
  unknown <- setdiff(names(controls), allowed)
  if (length(unknown)) stop("Unknown acousticTS controls: ", paste(unknown, collapse = ", "))
  controls
}

psms_calculation <- function(request, controls) {
  boundary <- switch(request$boundary, rigid = "fixed_rigid",
                     pressure_release = "pressure_release", liquid = "liquid_filled",
                     gas = "gas_filled", stop("Unsupported boundary"))
  shape <- acousticTS::prolate_spheroid(
    semimajor_length = request$semimajor_m,
    semiminor_length = request$semiminor_m, length_units = "m")
  penetrable <- request$boundary %in% c("liquid", "gas")
  target <- acousticTS::fls_generate(
    shape = shape,
    density_body = if (penetrable) request$target_density_kg_m3 else request$medium_density_kg_m3,
    sound_speed_body = if (penetrable) request$target_sound_speed_m_s else request$medium_sound_speed_m_s,
    theta_body = request$incidence_deg * pi / 180, theta_units = "radians")
  prediction <- do.call(acousticTS::target_strength, c(list(
    object = target, frequency = request$frequency_hz, model = "psms",
    boundary = boundary, phi_body = pi,
    density_sw = request$medium_density_kg_m3,
    sound_speed_sw = request$medium_sound_speed_m_s), controls))
  result <- acousticTS::extract(prediction, "model")$PSMS
  if (nrow(result) != 1L) stop("Expected exactly one frequency result")
  k <- 2 * pi * request$frequency_hz / request$medium_sound_speed_m_s
  # Public f_bs stores the dimensionless modal sum. Convert to metres using
  # the same normalization used by acousticTS to calculate TS.
  amplitude <- -2i / k * result$f_bs[[1L]]
  list(ts_db = result$TS[[1L]], amplitude = amplitude,
       parameters = acousticTS::extract(prediction, "model_parameters")$PSMS$parameters)
}

psms_worker <- function(request) {
  answer <- list(status = "setup_error", elapsed_seconds = NULL, ts_db = NULL,
                 amplitude_real_m = NULL, amplitude_imag_m = NULL,
                 warmup_seconds = 0, diagnostics = list(), message = "",
                 runtime_version = R.version.string)
  phase <- "setup"
  warnings <- character()
  started <- NULL
  tryCatch(withCallingHandlers({
    if (!requireNamespace("acousticTS", quietly = TRUE)) stop("acousticTS is not installed")
    controls <- psms_controls(request)
    answer$diagnostics <- list(
      controls = controls, package_version = as.character(utils::packageVersion("acousticTS")),
      package_path = find.package("acousticTS"), timer = "proc.time elapsed",
      timing_scope = "fresh geometry, scatterer, public target_strength and result extraction",
      amplitude_convention = "-2i/k times the public PSMS f_bs modal sum, metres",
      convergence_assessed = isTRUE(controls$adaptive), reference_qualified = FALSE)
    if (isTRUE(request$warmup)) {
      phase <- "warmup"
      warm <- request
      warm$semimajor_m <- 0.001
      warm$semiminor_m <- 0.0005
      warm$frequency_hz <- 1000
      warm_started <- proc.time()[["elapsed"]]
      psms_calculation(warm, controls)
      answer$warmup_seconds <- proc.time()[["elapsed"]] - warm_started
    }
    gc(verbose = FALSE)
    phase <- "solve"
    if (!is.null(request$timing_started_path)) {
      jsonlite::write_json(list(started_epoch = as.numeric(Sys.time()), phase = phase),
                           request$timing_started_path, auto_unbox = TRUE)
    }
    started <- proc.time()[["elapsed"]]
    value <- psms_calculation(request, controls)
    answer$elapsed_seconds <- proc.time()[["elapsed"]] - started
    phase <- "postprocess"
    finite <- is.finite(value$ts_db) && is.finite(value$amplitude)
    answer$status <- if (finite) "ok" else "numerical_error"
    answer$ts_db <- if (is.finite(value$ts_db)) value$ts_db else NULL
    answer$amplitude_real_m <- if (is.finite(Re(value$amplitude))) Re(value$amplitude) else NULL
    answer$amplitude_imag_m <- if (is.finite(Im(value$amplitude))) Im(value$amplitude) else NULL
    answer$diagnostics$parameters <- value$parameters
    if (!finite) answer$message <- "The public calculation returned nonfinite TS or amplitude"
  }, warning = function(warning) {
    warnings <<- c(warnings, conditionMessage(warning))
    invokeRestart("muffleWarning")
  }), error = function(error) {
    if (!is.null(started)) answer$elapsed_seconds <<- proc.time()[["elapsed"]] - started
    detail <- conditionMessage(error)
    answer$message <<- paste0(phase, ": ", detail)
    answer$status <<- if (phase %in% c("setup", "warmup")) "setup_error" else
      if (grepl("converg|modal support|boundary residual", detail, ignore.case = TRUE))
        "nonconverged" else "numerical_error"
  })
  answer$diagnostics$warnings <- as.list(warnings)
  answer$diagnostics$phase <- phase
  if (answer$status == "ok" && any(grepl("failed.*converg|did not converge|not converged",
                                         warnings, ignore.case = TRUE))) {
    answer$status <- "nonconverged"
    answer$message <- paste(warnings, collapse = "\n")
  }
  answer
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 2L) stop("Usage: Rscript worker_acousticts.R REQUEST_JSON OUTPUT_JSON")
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("jsonlite is required for the worker protocol")
  request <- jsonlite::fromJSON(args[[1L]], simplifyVector = FALSE)
  answer <- psms_worker(request)
  jsonlite::write_json(answer, args[[2L]], auto_unbox = TRUE, null = "null",
                       na = "null", digits = NA, pretty = TRUE)
}
