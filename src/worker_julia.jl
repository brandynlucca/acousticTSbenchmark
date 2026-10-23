# Single-case worker. All native output goes to the supervisor's process log.
using JSON3, LinearAlgebra, Printf, DelimitedFiles, SHA

function memory_boundary(request, phase)
    signal = get(request, "memory_$(phase)_path", nothing)
    signal === nothing && return
    try
        if phase == "ready"
            open(signal, "w") do io
                JSON3.write(io, Dict("pid" => getpid()))
            end
        else
            touch(signal)
        end
        acknowledgement = request["memory_$(phase)_ack_path"]
        deadline = time_ns() + 5_000_000_000
        while !isfile(acknowledgement)
            time_ns() >= deadline && error("Memory sampler acknowledgement timed out")
            sleep(.01)
        end
    catch
        try
            touch(request["memory_failed_path"])
        catch
        end
    end
    nothing
end

function scattering_package_metadata()
    # Julia 1.12 also ages newly imported global bindings. Read the module
    # after the dynamic import in the current world, outside the solve timer.
    Base.invokelatest() do
        Dict("package_version" => string(Base.pkgversion(AcousticScattering)),
             "package_path" => pathof(AcousticScattering))
    end
end

function json_safe(value)
    value === nothing && return nothing
    ismissing(value) && return nothing
    if value isa AbstractFloat
        converted = Float64(value)
        return isfinite(converted) ? converted : nothing
    end
    value isa Union{Bool,Integer,AbstractString} && return value
    value isa Symbol && return string(value)
    value isa Complex && return Dict("real" => json_safe(real(value)), "imag" => json_safe(imag(value)))
    value isa Union{NamedTuple,AbstractDict} && return Dict(string(k) => json_safe(v) for (k,v) in pairs(value))
    value isa Tuple && return json_safe.(collect(value))
    if value isa AbstractArray
        ndims(value) == 1 && return json_safe.(value)
        return Dict("dimensions" => collect(size(value)), "storage_order" => "column-major",
                    "values" => json_safe.(vec(value)))
    end
    string(value)
end

function scattering_controls(request)
    boundary = request["boundary"]
    controls = Dict{Symbol,Any}(:adaptive => true, :target_tol => .005,
        :max_refinements => 8, :mode_threads => 1, :rtol => 1e-6,
        :quadrature_extra => 0, :chief_points => 8,
        :boundary_order => boundary == "gas" ? 5 : 7)
    boundary in ("liquid", "gas") && (controls[:order] = boundary == "liquid" ? 8 : 6)
    requested = get(request, "controls", Dict())
    for (key, value) in requested
        key in ("rigid", "pressure_release", "liquid", "gas") && continue
        controls[Symbol(key)] = value
    end
    for (key, value) in get(requested, boundary, Dict())
        controls[Symbol(key)] = value
    end
    controls[:mode_threads] == 1 || error("Serial benchmarks require mode_threads=1")
    controls
end

function scattering_calculation(request, controls)
    body = AcousticScattering.Spheroid(request["semimajor_m"], request["semiminor_m"])
    label = request["boundary"]
    boundary = label == "rigid" ? AcousticScattering.Rigid() :
        label == "pressure_release" ? AcousticScattering.PressureRelease() :
        label in ("liquid", "gas") ? AcousticScattering.FluidFilled(
            request["target_density_kg_m3"] / request["medium_density_kg_m3"],
            request["target_sound_speed_m_s"] / request["medium_sound_speed_m_s"]) :
        error("Unsupported boundary: $label")
    k = 2pi * request["frequency_hz"] / request["medium_sound_speed_m_s"]
    angle = deg2rad(request["incidence_deg"])
    solution = if label in ("liquid", "gas")
        AcousticScattering.coupled(body, boundary, k;
            coupling = AcousticScattering.Coupling(interior = :fem, exterior = :bem),
            incidence_angle = angle, controls...)
    else
        AcousticScattering.bem(body, boundary, k; incidence_angle = angle, controls...)
    end
    amplitude = AcousticScattering.scattering_amplitude(solution)
    strength = AcousticScattering.target_strength(solution)
    report = AcousticScattering.diagnostics(solution)
    finite = isfinite(amplitude) && isfinite(strength)
    refinement = get(report, :refinement, nothing)
    spatial = refinement !== nothing && get(refinement, :converged, false) &&
        isfinite(get(refinement, :change_db, Inf)) &&
        refinement.change_db <= controls[:target_tol]
    algebraic = get(report, :converged, true) !== false
    status = !finite ? "numerical_error" : spatial && algebraic ? "ok" : "nonconverged"
    (status = status, ts_db = strength, amplitude = amplitude,
     diagnostics = Dict("solver_report" => report, "spatial_converged" => spatial,
                        "reference_qualified" => false,
                        "qualification" => "spatial-change check only, independent reference qualification required"))
end

# Preserve the upstream public coefficient and pattern equations. This bridge
# replaces obsolete Julia syntax and shell launching only. Stock executables
# retain their original intermediate files and 256-bit arithmetic settings.
module LiquidSpheroidBridge
using LinearAlgebra, Printf, DelimitedFiles
const Complex128 = ComplexF64
const executable = Ref("")
const native_seconds = Ref(0.0)
const native_calls = Ref(0)
linspace(a, b, n) = range(a, b; length = n)
cell(dimensions...) = Array{Any}(undef, dimensions...)

function native_run(command::Cmd)
    words = collect(command)
    script = basename(words[1])
    startswith(script, "pro_sphwv_") || error("Unexpected native command: $script")
    args = words[2:end]
    c, m, n = args[1:3]
    common = [executable[], "-max_memory", "4000", "-precision", "256", "-verbose", "n",
              "-c", c, "-m", m, "-n", n]
    function invoke(task, extra = String[], output = nothing)
        command = Cmd(vcat(common, ["-w", task], extra))
        library_path = get(ENV, "BENCHMARK_LIQUID_LIBRARY_PATH", "")
        if Sys.islinux() && !isempty(library_path)
            existing = get(ENV, "LD_LIBRARY_PATH", "")
            command = addenv(command, "LD_LIBRARY_PATH" =>
                (isempty(existing) ? library_path : library_path * ":" * existing))
        end
        # Some distributed binaries write S1 to Salida_S.dat, others stdout.
        # Never reuse an earlier S1 output after a native failure.
        task == "S1" && isfile("Salida_S.dat") && rm("Salida_S.dat")
        started = time_ns()
        if output === nothing
            Base.run(pipeline(command; stdout = devnull))
        else
            open(output, "w") do io
                Base.run(pipeline(command; stdout = io))
            end
            task == "S1" && isfile("Salida_S.dat") && cp("Salida_S.dat", output; force = true)
            filesize(output) > 0 || error("Native $task returned an empty output")
        end
        native_seconds[] += (time_ns() - started) / 1e9
        native_calls[] += 1
    end
    invoke("lambda")
    invoke("dr", ["-n_dr", "20", "-dr_min", "1.0e-100"])
    if occursin("dr.N", script)
        cp(joinpath("data", "pro_$(args[4])_dr.txt"), "Out_dr.dat"; force = true)
    end
    invoke("dr_neg", ["-n_dr_neg", "20", "-dr_neg_min", "1.0e-100"])
    invoke("N")
    if occursin("dr.N", script)
        cp(joinpath("data", "pro_$(args[4])_N.txt"), "Out_S_N.dat"; force = true)
        return nothing
    end
    for task in ("F", "k1", "k2")
        invoke(task)
    end
    invoke("c2k", ["-n_c2k", "20", "-c2k_min", "1.0e-100"])
    eta = args[4]
    single = ["-a", eta, "-b", eta, "-d", "0.01", "-arg_type", "eta", "-p", "34"]
    if occursin("Sgrid", script)
        invoke("S1", single, "Out_Sinc.dat")
        invoke("S1", ["-a", "-1.0", "-b", "1.0000001", "-d", args[5],
                      "-arg_type", "eta", "-p", "34"], "Out_S.dat")
    else
        invoke("S1", single, "Out_S.dat")
        invoke("R", ["-a", args[5], "-b", args[5], "-d", "0.1", "-arg_type", "xi",
                     "-which", "R1_1,R1_2,R2_1,R2_2", "-p", "34"], "Out_R.dat")
    end
    nothing
end

function load_source(root, native)
    executable[] = native
    for filename in ("JUL.auxiliar.jl", "JUL.routines.jl")
        path = joinpath(root, "linux", filename)
        source = read(path, String)
        source = replace(source, r"\bconst\s+" => "", "ASCIIString" => "String",
            "@ sprintf" => "@sprintf", r"\brun\s*\(" => "native_run(",
            "abs( Pat )" => "abs.( Pat )")
        include_string(@__MODULE__, source, path)
    end
    nothing
end
end

function liquid_controls(request)
    controls = get(request, "controls", Dict())
    unknown = setdiff(collect(keys(controls)), ["M", "N", "precision_bits", "method"])
    isempty(unknown) || error("Unknown liquid_spheroid controls: $(join(unknown, ", "))")
    haskey(controls, "M") || error("liquid_spheroid requires an explicit modal ceiling M=N")
    M = controls["M"]
    M isa Integer && M >= 0 || error("M must be a nonnegative integer")
    get(controls, "N", M) == M || error("The public liquid_spheroid routines use a shared M=N ceiling")
    get(controls, "precision_bits", 256) == 256 || error("The stock native configuration uses 256-bit precision")
    method = get(controls, "method", 2)
    method in (1, 2) || error("method must be 1 or 2")
    Dict("M" => M, "N" => M, "precision_bits" => 256, "method" => method)
end

function liquid_receiver(native_eta, pattern, receiver)
    j = argmin(abs.(vec(native_eta) .- receiver))
    coordinate_error = abs(native_eta[j] - receiver)
    coordinate_error <= big"1e-30" || error("Native pattern grid misses the monostatic receiving coordinate")
    j > 1 || error("Public second-half pattern omits the receiving endpoint")
    index = length(native_eta) + j - 1
    magnitude = pattern[index, 2]
    (magnitude = magnitude, index = index, coordinate_error = coordinate_error,
     receiver_theta_radian = acos(Float64(native_eta[j])),
     receiver_phi_radian = Float64(pi), reported_pattern_angle_radian = pattern[index, 1])
end

function liquid_calculation(request, controls)
    0 <= request["incidence_deg"] <= 90 || error("Incidence must lie between 0 and 90 degrees")
    # Reversing the unoriented major axis makes the original eta grid contain
    # the exact monostatic receiver even for oblique incidence.
    eta = request["incidence_deg"] == 90 ? BigFloat(0) : -BigFloat(cosd(request["incidence_deg"]))
    deta = 1 - eta
    a, b = request["semimajor_m"], request["semiminor_m"]
    k0 = 2pi * request["frequency_hz"] / request["medium_sound_speed_m_s"]
    M, method = controls["M"], controls["method"]
    native_start, calls_start = LiquidSpheroidBridge.native_seconds[], LiquidSpheroidBridge.native_calls[]
    coefficients = if request["boundary"] in ("liquid", "gas")
        k1 = 2pi * request["frequency_hz"] / request["target_sound_speed_m_s"]
        ratio = request["target_density_kg_m3"] / request["medium_density_kg_m3"]
        first(LiquidSpheroidBridge.Coeff_Pro(ratio, k0, k1, a, b, M, method, eta))
    elseif request["boundary"] in ("rigid", "pressure_release")
        LiquidSpheroidBridge.Coeff_ProDN(k0, a, b, M, method, eta,
            request["boundary"] == "rigid" ? "N" : "D")
    else
        error("Unsupported boundary")
    end
    pattern = LiquidSpheroidBridge.Pattern_LiquidPro(k0, a, b, M, method, eta, coefficients, deta)
    native_eta = LiquidSpheroidBridge.ReadFileToArrayBF2("Out_S.dat", ',', :, 2)
    receiver = liquid_receiver(native_eta, pattern, -eta)
    finite = isfinite(receiver.magnitude) && receiver.magnitude > 0
    (status = finite ? "ok" : "numerical_error",
     ts_db = finite ? 20log10(receiver.magnitude) : NaN, amplitude = nothing,
     diagnostics = Dict("M" => M, "N" => M, "method" => method,
        "convergence_assessed" => false, "reference_qualified" => false,
        "qualification" => "finite public output at a fixed modal ceiling",
        "native_precision_bits" => 256, "amplitude_magnitude_m" => receiver.magnitude,
        "complex_amplitude_available" => false, "receiver" => receiver,
        "incident_eta_native" => string(eta), "receiver_eta_native" => string(-eta),
        "axis_mapping" => "axis reversed, incident eta=-cos(theta), receiver eta=cos(theta) at phi=pi",
        "native_subprocess_seconds" => LiquidSpheroidBridge.native_seconds[] - native_start,
        "native_subprocess_calls" => LiquidSpheroidBridge.native_calls[] - calls_start,
        "pattern_points" => size(pattern, 1)))
end

function worker(request, output_path)
    answer = Dict{String,Any}("status" => "setup_error", "elapsed_seconds" => nothing,
        "ts_db" => nothing, "amplitude_real_m" => nothing, "amplitude_imag_m" => nothing,
        "warmup_seconds" => 0.0, "diagnostics" => Dict{String,Any}(), "message" => "",
        "runtime_version" => string(VERSION))
    phase, started = "setup", nothing
    try
        BLAS.set_num_threads(1)
        Threads.nthreads() == 1 || error("Serial benchmarks require Julia --threads=1")
        software = request["software"]
        println("SETUP ", software)
        flush(stdout)
        calculate, controls = if software == "liquid_spheroid"
            if !(Sys.iswindows() || Sys.islinux()) || Sys.ARCH != :x86_64
                answer["status"] = "unsupported"
                answer["message"] = "Upstream liquid_spheroid distributes only Windows/Linux x64 native binaries"
                return answer
            end
            root = abspath(request["source_path"])
            native = get(request, "native_executable", nothing)
            native === nothing && (native = joinpath(root,
                Sys.iswindows() ? "windows/pro_sphwv.exe" : "linux/pro_sphwv"))
            isfile(native) || error("Missing liquid_spheroid executable: $native")
            setprecision(BigFloat, 256)
            LiquidSpheroidBridge.load_source(root, abspath(native))
            answer["diagnostics"]["native_executable_sha256"] = bytes2hex(sha256(read(native)))
            answer["diagnostics"]["source_sha256"] = Dict(name => bytes2hex(sha256(read(joinpath(root,"linux",name))))
                for name in ("JUL.auxiliar.jl", "JUL.routines.jl"))
            liquid_calculation, liquid_controls(request)
        else
            @eval using AcousticScattering
            merge!(answer["diagnostics"], scattering_package_metadata())
            scattering_calculation, scattering_controls(request)
        end
        answer["diagnostics"]["controls"] = controls
        answer["diagnostics"]["julia_threads"] = Threads.nthreads()
        answer["diagnostics"]["blas_threads"] = BLAS.get_num_threads()
        answer["diagnostics"]["timer"] = "time_ns monotonic"
        answer["diagnostics"]["timing_scope"] = "fresh public problem construction, solve and backscatter extraction"
        if get(request, "warmup", true)
            phase = "warmup"
            println("WARMUP distinct 1 mm spheroid at 1 kHz")
            flush(stdout)
            warm = merge(request, Dict("semimajor_m" => .001, "semiminor_m" => .0005, "frequency_hz" => 1000))
            warm_started = time_ns()
            if software == "liquid_spheroid"
                # Isolate native scratch files, so no special functions from
                # the warmup can be reused during the timed calculation.
                warm_dir = joinpath(dirname(abspath(output_path)), "warmup")
                mkpath(joinpath(warm_dir, "data"))
                cd(warm_dir) do
                    Base.invokelatest(calculate, warm, controls)
                end
            else
                Base.invokelatest(calculate, warm, controls)
            end
            answer["warmup_seconds"] = (time_ns() - warm_started) / 1e9
        end
        GC.gc()
        work_dir = dirname(abspath(output_path))
        software == "liquid_spheroid" && mkpath(joinpath(work_dir, "data"))
        memory_boundary(request, "ready")
        phase = "solve"
        println("SOLVE ", request["frequency_hz"], " Hz, ", request["incidence_deg"], " deg")
        flush(stdout)
        if haskey(request, "timing_started_path")
            open(request["timing_started_path"], "w") do io
                JSON3.write(io, Dict("started_epoch" => time(), "phase" => phase))
            end
        end
        started = time_ns()
        value = cd(work_dir) do
            Base.invokelatest(calculate, request, controls)
        end
        answer["elapsed_seconds"] = (time_ns() - started) / 1e9
        memory_boundary(request, "done")
        phase = "postprocess"
        answer["status"], answer["ts_db"] = value.status, value.ts_db
        if value.amplitude !== nothing
            answer["amplitude_real_m"] = real(value.amplitude)
            answer["amplitude_imag_m"] = imag(value.amplitude)
        end
        merge!(answer["diagnostics"], value.diagnostics)
        value.status != "ok" && (answer["message"] = "Finite-value or spatial convergence checks failed")
    catch exception
        if started !== nothing
            answer["elapsed_seconds"] === nothing && (answer["elapsed_seconds"] = (time_ns() - started) / 1e9)
            memory_boundary(request, "done")
        end
        detail = sprint(showerror, exception)
        nonconverged = occursin(r"(?i)did not converge|failed to converge|failed convergence|refinement budget", detail)
        answer["status"] = phase in ("setup", "warmup") ? "setup_error" :
            nonconverged ? "nonconverged" : "numerical_error"
        answer["message"] = phase * ": " * detail
    end
    answer["diagnostics"]["phase"] = phase
    answer
end

if abspath(PROGRAM_FILE) == @__FILE__
    length(ARGS) == 2 || error("Usage: julia --threads=1 worker_julia.jl REQUEST_JSON OUTPUT_JSON")
    request = JSON3.read(read(ARGS[1], String), Dict{String,Any})
    answer = worker(request, ARGS[2])
    open(ARGS[2], "w") do io
        JSON3.write(io, json_safe(answer))
    end
end
