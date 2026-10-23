# Run explicitly after installing the campaign's acousticTS source and jsonlite.
library(testthat)
source(file.path("src", "worker_acousticts.R"))

test_that("memory boundaries signal the actual worker and fail independently", {
  skip_if_not_installed("jsonlite")
  directory <- tempfile("memory-protocol-")
  dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE))
  request <- setNames(lapply(c("ready", "ready_ack", "done", "done_ack", "failed"),
    function(name) file.path(directory, name)),
    paste0("memory_", c("ready", "ready_ack", "done", "done_ack", "failed"), "_path"))
  file.create(request$memory_ready_ack_path, request$memory_done_ack_path)
  memory_boundary(request, "ready")
  expect_equal(jsonlite::read_json(request$memory_ready_path)$pid, Sys.getpid())
  expect_false(file.exists(request$memory_done_path))
  memory_boundary(request, "done")
  expect_true(file.exists(request$memory_done_path))
  expect_false(file.exists(request$memory_failed_path))
  expect_silent(memory_boundary(list(), "ready"))
  request$memory_ready_path <- directory
  suppressWarnings(memory_boundary(request, "ready"))
  expect_true(file.exists(request$memory_failed_path))
})

test_that("public PSMS results retain metre normalization for every boundary", {
  skip_if_not_installed("acousticTS")
  request <- list(software = "acousticts", frequency_hz = 1000,
    incidence_deg = 37, semimajor_m = 0.001, semiminor_m = 0.0005,
    medium_density_kg_m3 = 1026.8, medium_sound_speed_m_s = 1477.3,
    controls = list(precision = "quad", adaptive = TRUE), warmup = TRUE)
  for (boundary in c("rigid", "pressure_release", "liquid", "gas")) {
    request$boundary <- boundary
    request$target_density_kg_m3 <- if (boundary == "gas") 1.24 else 1028.9
    request$target_sound_speed_m_s <- if (boundary == "gas") 345 else 1480.3
    answer <- psms_worker(request)
    expect_identical(answer$status, "ok", info = answer$message)
    expect_true(is.finite(answer$ts_db))
    amplitude <- complex(real = answer$amplitude_real_m,
                         imaginary = answer$amplitude_imag_m)
    expect_equal(answer$ts_db, 20 * log10(Mod(amplitude)), tolerance = 1e-10)
    expect_true(answer$elapsed_seconds >= 0)
    expect_false(answer$diagnostics$reference_qualified)
  }
})

test_that("unsupported controls fail before any calculation", {
  expect_error(psms_controls(list(controls = list(m_max = 4))), "Unknown acousticTS controls")
})
