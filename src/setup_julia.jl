# Install into the campaign environment without modifying source repositories.
# Source checkout/archive verification is the caller's responsibility.
using Pkg, TOML

length(ARGS) in (2, 3) || error("Usage: setup_julia.jl ENVIRONMENT ACOUSTIC_SOURCE_OR_DASH [LIQUID_SOURCE]")
environment = abspath(ARGS[1])
acoustic_source = ARGS[2] == "-" ? nothing : abspath(ARGS[2])
acoustic_source !== nothing && !isfile(joinpath(acoustic_source, "Project.toml")) &&
    error("Missing AcousticScattering Project.toml")
ENV["JULIA_PKG_PRECOMPILE_AUTO"] = "0"
mkpath(environment)
Pkg.activate(environment)
preferences = joinpath(environment, "LocalPreferences.toml")
current = isfile(preferences) ? TOML.parsefile(preferences) : Dict{String,Any}()
if acoustic_source !== nothing
    current["AcousticScattering"] = merge(get(current, "AcousticScattering", Dict()),
                                         Dict("precompile_workload" => false))
end
open(preferences, "w") do io
    TOML.print(io, current)
end
acoustic_source !== nothing && Pkg.develop(PackageSpec(path = acoustic_source))
protocol_packages = [PackageSpec(name = "JSON3"), PackageSpec(name = "Preferences")]
if length(ARGS) == 3
    isfile(joinpath(abspath(ARGS[3]), "linux", "JUL.routines.jl")) || error("Missing liquid_spheroid source")
    push!(protocol_packages, PackageSpec(name = "GSL"))
end
Pkg.add(protocol_packages)
Pkg.instantiate()
Pkg.precompile()
println("Julia environment and resolved Manifest.toml: ", environment)
