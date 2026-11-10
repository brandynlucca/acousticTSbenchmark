# Install into the campaign environment without modifying source repositories.
# Source checkout/archive verification is the caller's responsibility.
using Pkg, TOML

# Reproduce the Julia dependencies recorded with the retained liquid timings.
const LIQUID_PROJECT = raw"""
[deps]
GSL = "92c85e6c-cbff-5e0c-80f7-495c94daaecd"
JSON3 = "0f8b85d8-7281-11e9-16c2-39a750bddbf1"
Preferences = "21216c6a-2e73-6563-6e65-726566657250"
"""
const LIQUID_MANIFEST = raw"""
# This file is machine-generated - editing it directly is not advised

julia_version = "1.12.1"
manifest_format = "2.0"
project_hash = "b332abd902b2d1f910b98ea5fed967c1a0bfdc47"

[[deps.Artifacts]]
uuid = "56f22d72-fd6d-98f1-02f0-08ddc0907c33"
version = "1.11.0"

[[deps.Base64]]
uuid = "2a0f44e3-6c83-55bd-87e4-b1978d98bd5f"
version = "1.11.0"

[[deps.Dates]]
deps = ["Printf"]
uuid = "ade2ca70-3891-5945-98fb-dc099432e06a"
version = "1.11.0"

[[deps.GSL]]
deps = ["GSL_jll", "Libdl", "Markdown"]
git-tree-sha1 = "3ebd07d519f5ec318d5bc1b4971e2472e14bd1f0"
uuid = "92c85e6c-cbff-5e0c-80f7-495c94daaecd"
version = "1.0.1"

[[deps.GSL_jll]]
deps = ["Artifacts", "JLLWrappers", "Libdl"]
git-tree-sha1 = "7da3a878517e1420850569d35d9ba583b745e39d"
uuid = "1b77fbbe-d8ee-58f0-85f9-836ddc23a7a4"
version = "2.8.1+0"

[[deps.JLLWrappers]]
deps = ["Artifacts", "Preferences"]
git-tree-sha1 = "7204148362dafe5fe6a273f855b8ccbe4df8173e"
uuid = "692b3bcd-3c85-4b1f-b108-f13ce0eb3210"
version = "1.8.0"

[[deps.JSON3]]
deps = ["Dates", "Mmap", "Parsers", "PrecompileTools", "StructTypes", "UUIDs"]
git-tree-sha1 = "34a52631602d7233eb1dca1a358ae82b656f9f2a"
uuid = "0f8b85d8-7281-11e9-16c2-39a750bddbf1"
version = "1.14.4"

    [deps.JSON3.extensions]
    JSON3ArrowExt = ["ArrowTypes"]

    [deps.JSON3.weakdeps]
    ArrowTypes = "31f734f8-188a-4ce0-8406-c8a06bd891cd"

[[deps.JuliaSyntaxHighlighting]]
deps = ["StyledStrings"]
uuid = "ac6e5ff7-fb65-4e79-a425-ec3bc9c03011"
version = "1.12.0"

[[deps.Libdl]]
uuid = "8f399da3-3557-5675-b5ff-fb832c97cbdb"
version = "1.11.0"

[[deps.Markdown]]
deps = ["Base64", "JuliaSyntaxHighlighting", "StyledStrings"]
uuid = "d6f4376e-aef5-505a-96c1-9c027394607a"
version = "1.11.0"

[[deps.Mmap]]
uuid = "a63ad114-7e13-5084-954f-fe012c677804"
version = "1.11.0"

[[deps.Parsers]]
deps = ["Dates", "PrecompileTools", "UUIDs"]
git-tree-sha1 = "e24b87585a307a3c3225cb9b64d03802a1e01edd"
uuid = "69de0a69-1ddd-5017-9359-2bf0b02dc9f0"
version = "2.8.10"

[[deps.PrecompileTools]]
deps = ["Preferences"]
git-tree-sha1 = "edbeefc7a4889f528644251bdb5fc9ab5348bc2c"
uuid = "aea7be01-6a6a-4083-8856-8a6e6704d82a"
version = "1.3.4"

[[deps.Preferences]]
deps = ["TOML"]
git-tree-sha1 = "aef1987d90dcc2e2cbd919ad10b2d25bcce4f418"
uuid = "21216c6a-2e73-6563-6e65-726566657250"
version = "1.6.1"

[[deps.Printf]]
deps = ["Unicode"]
uuid = "de0858da-6303-5e67-8744-51eddeeeb8d7"
version = "1.11.0"

[[deps.Random]]
deps = ["SHA"]
uuid = "9a3f8284-a2c9-5f02-9a11-845980a1fd5c"
version = "1.11.0"

[[deps.SHA]]
uuid = "ea8e919c-243c-51af-8825-aaa63cd721ce"
version = "0.7.0"

[[deps.StructTypes]]
deps = ["Dates", "UUIDs"]
git-tree-sha1 = "fd6a55e88450cb4256e49103a5a243f065c7b781"
uuid = "856f2bd8-1eba-4b0a-8007-ebc267875bd4"
version = "1.11.1"

[[deps.StyledStrings]]
uuid = "f489334b-da3d-4c2e-b8f0-e476e12c162b"
version = "1.11.0"

[[deps.TOML]]
deps = ["Dates"]
uuid = "fa267f1f-6049-4f14-aa54-33bafae1ed76"
version = "1.0.3"

[[deps.UUIDs]]
deps = ["Random", "SHA"]
uuid = "cf7118a7-6976-5b1a-9a39-7adc72f591a4"
version = "1.11.0"

[[deps.Unicode]]
uuid = "4ec0a83e-493e-50e2-b9ac-8f72acf5a8f5"
version = "1.11.0"
"""

length(ARGS) in (2, 3) || error("Usage: setup_julia.jl ENVIRONMENT ACOUSTIC_SOURCE_OR_DASH [LIQUID_SOURCE]")
environment = abspath(ARGS[1])
acoustic_source = ARGS[2] == "-" ? nothing : abspath(ARGS[2])
acoustic_source !== nothing && !isfile(joinpath(acoustic_source, "Project.toml")) &&
    error("Missing AcousticScattering Project.toml")
ENV["JULIA_PKG_PRECOMPILE_AUTO"] = "0"
mkpath(environment)
if length(ARGS) == 3
    acoustic_source === nothing || error("Liquid dependency lock requires an isolated environment")
    isfile(joinpath(abspath(ARGS[3]), "linux", "JUL.routines.jl")) || error("Missing liquid_spheroid source")
    write(joinpath(environment, "Project.toml"), LIQUID_PROJECT)
    write(joinpath(environment, "Manifest.toml"), LIQUID_MANIFEST)
end
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
if length(ARGS) != 3
    Pkg.add(protocol_packages)
end
Pkg.instantiate()
Pkg.precompile()
if length(ARGS) == 3
    expected = TOML.parse(LIQUID_MANIFEST)
    actual = TOML.parsefile(joinpath(environment, "Manifest.toml"))
    pop!(expected, "project_hash", nothing)
    pop!(actual, "project_hash", nothing)
    actual == expected || error("Liquid dependency installation differs from the saved manifest")
end
println("Julia environment and resolved Manifest.toml: ", environment)
