# Persistent reference calculations. Import and compilation occur once per batch.
include(joinpath(@__DIR__, "worker_julia.jl"))

function reference_save(path, value)
    mkpath(dirname(path))
    temporary = path * ".tmp"
    open(temporary, "w") do io
        JSON3.write(io, json_safe(value))
        flush(io)
    end
    Base.Filesystem.rename(temporary, path)
end

function reference_calculation(request; calculate=scattering_calculation, progress=phase -> nothing)
    controls = Dict(Symbol(k) => v for (k,v) in request["controls"])
    controls[:adaptive] === true || error("Reference spatial refinement must be enabled")
    controls[:target_tol] <= request["policy"]["spatial_tolerance_db"] || error("Spatial tolerance exceeds policy")
    progress("solving")
    baseline = Base.invokelatest(calculate, request, controls)
    expanded = nothing
    if baseline.status == "ok" && request["incidence_deg"] != 0
        modes = baseline.diagnostics["solver_report"].solver_options.m_max
        checked = merge(controls, Dict(:m_max => modes + request["policy"]["mode_increment"]))
        progress("checking_fourier")
        expanded = Base.invokelatest(calculate, request, checked)
    end
    # Python independently derives qualification from these retained results.
    Dict("status" => baseline.status, "baseline" => baseline, "expanded" => expanded)
end

function reference_batch(batch)
    threads = batch["threads"]
    Threads.nthreads() == threads || error("Reference Julia thread count does not match the request")
    BLAS.set_num_threads(1)
    cases = batch["cases"]
    isempty(cases) && return
    active_path = batch["active_path"]
    work = batch["work_directory"]
    attempts = get(batch, "attempts_directory", joinpath(work, "reference-attempts"))
    started = get(batch, "started_epoch", time())
    active = Dict("reference_id" => first(cases)["reference_id"], "started_epoch" => started,
                  "phase" => "importing")
    reference_save(active_path, active)
    @eval using AcousticScattering
    metadata = scattering_package_metadata()
    warmed = Set{String}()
    for (index, request) in enumerate(cases)
        # The first reference already spent time importing. Its existing wall
        # deadline includes that work, so do not reserve a second full timeout.
        case_started = index == 1 ? started : time()
        case_started + batch["case_timeout_seconds"] > batch["deadline_epoch"] && break
        identity = request["reference_id"]
        started = case_started
        active = Dict("reference_id" => identity, "started_epoch" => started, "phase" => "starting")
        function progress(phase)
            active["phase"] = phase
            reference_save(active_path, active)
            println("REFERENCE ", identity, " ", phase, " f=", request["frequency_hz"],
                    " Hz angle=", request["incidence_deg"])
            flush(stdout)
        end
        warmup_seconds = 0.0
        evidence = try
            boundary = request["boundary"]
            if !(boundary in warmed)
                progress("warming_up")
                warm = merge(request, Dict("semimajor_m" => .001, "semiminor_m" => .0005,
                                          "frequency_hz" => 1000))
                controls = Dict(Symbol(k) => v for (k,v) in request["controls"])
                tick = time_ns()
                Base.invokelatest(scattering_calculation, warm, controls)
                warmup_seconds = (time_ns() - tick) / 1e9
                push!(warmed, boundary)
            end
            GC.gc()
            reference_calculation(request; progress)
        catch exception
            exception isa InterruptException && rethrow()
            Dict("status" => "numerical_error", "message" => sprint(showerror, exception, catch_backtrace()),
                 "phase" => active["phase"])
        end
        evidence["elapsed_seconds"] = time() - started
        evidence["warmup_seconds"] = warmup_seconds
        evidence["runtime_version"] = string(VERSION)
        evidence["package_metadata"] = metadata
        evidence["julia_threads"] = Threads.nthreads()
        evidence["blas_threads"] = BLAS.get_num_threads()
        reference_save(joinpath(attempts, identity * ".json"), evidence)
        progress("recorded")
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    length(ARGS) == 1 || error("Usage: julia --threads=4 worker_reference.jl BATCH_JSON")
    reference_batch(JSON3.read(read(ARGS[1], String), Dict{String,Any}))
end
