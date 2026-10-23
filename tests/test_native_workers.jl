using Test
include(joinpath(@__DIR__, "..", "src", "worker_reference.jl"))

@testset "Prediction memory boundaries are optional and isolated" begin
    mktempdir() do directory
        request = Dict("memory_$(name)_path" => joinpath(directory, name)
                       for name in ("ready", "ready_ack", "done", "done_ack", "failed"))
        touch(request["memory_ready_ack_path"])
        touch(request["memory_done_ack_path"])
        memory_boundary(request, "ready")
        @test JSON3.read(read(request["memory_ready_path"], String))["pid"] == getpid()
        @test !isfile(request["memory_done_path"])
        memory_boundary(request, "done")
        @test isfile(request["memory_done_path"])
        @test !isfile(request["memory_failed_path"])
        @test memory_boundary(Dict(), "ready") === nothing
        request["memory_ready_path"] = directory
        @test memory_boundary(request, "ready") === nothing
        @test isfile(request["memory_failed_path"])
    end
end

@testset "Reference Fourier check uses public solve with expanded resolution" begin
    request = Dict("controls" => Dict("adaptive" => true, "target_tol" => .005, "mode_threads" => 4),
                   "policy" => Dict("spatial_tolerance_db" => .005, "mode_increment" => 8),
                   "incidence_deg" => 37)
    calls = Dict[]
    function mock_calculation(request, controls)
        push!(calls, copy(controls))
        (status="ok", ts_db=-40.0, amplitude=.01+0im,
         diagnostics=Dict("solver_report" => (solver_options=(m_max=get(controls,:m_max,20),),)))
    end
    value = reference_calculation(request; calculate=mock_calculation)
    @test length(calls) == 2
    @test calls[1][:mode_threads] == calls[2][:mode_threads] == 4
    @test !haskey(calls[1], :m_max)
    @test calls[2][:m_max] == 28
    @test value["expanded"].diagnostics["solver_report"].solver_options.m_max == 28
    empty!(calls)
    value = reference_calculation(merge(request, Dict("incidence_deg" => 0)); calculate=mock_calculation)
    @test length(calls) == 1
    @test value["expanded"] === nothing
end

@testset "Late-loaded package metadata on Julia 1.12" begin
    function metadata_after_binding_creation()
        # A self-contained module reproduces the binding age of a dynamic
        # package import without loading the numerical solver stack.
        @eval module AcousticScattering end
        scattering_package_metadata()
    end
    metadata = metadata_after_binding_creation()
    @test metadata["package_version"] == "nothing"
    @test metadata["package_path"] === nothing
end

@testset "Public liquid_spheroid receiving coordinates" begin
    # Legacy second-half angle labels assume a symmetric grid. This asymmetric
    # fixture forces selection by native eta and azimuth instead of that label.
    receiver = BigFloat(cosd(37))
    native_eta = reshape(BigFloat[-1, receiver], :, 1)
    pattern = [deg2rad(37) 11.0; pi 22.0; 2pi 33.0]
    selected = liquid_receiver(native_eta, pattern, receiver)
    @test selected.index == 3
    @test selected.magnitude == 33
    @test selected.receiver_theta_radian ≈ deg2rad(37)
    @test selected.receiver_phi_radian == Float64(pi)
    @test selected.reported_pattern_angle_radian == 2pi
    broadside = liquid_receiver(BigFloat[-1, 0, 1],
        [0.0 1.0; pi/2 2.0; pi 3.0; 3pi/2 4.0; 2pi 5.0], BigFloat(0))
    @test broadside.index == 4
    @test broadside.magnitude == 4
    axial = liquid_receiver(BigFloat[-1, 1], [0.0 1.0; pi 2.0; 2pi 3.0], BigFloat(1))
    @test axial.index == 3
    @test_throws ErrorException liquid_receiver(native_eta, pattern, big"0.5")
    @test_throws ErrorException liquid_receiver(native_eta, pattern, BigFloat(-1))
end

@testset "Numerical controls and portable diagnostics" begin
    @test scattering_controls(Dict("boundary" => "gas"))[:order] == 6
    @test scattering_controls(Dict("boundary" => "liquid"))[:order] == 8
    @test_throws ErrorException scattering_controls(Dict("boundary" => "rigid",
        "controls" => Dict("mode_threads" => 2)))
    @test_throws ErrorException liquid_controls(Dict("controls" => Dict("M" => 4, "N" => 5)))
    @test_throws ErrorException liquid_controls(Dict("controls" => Dict()))
    payload = json_safe((finite = 1.2, invalid = Inf, huge = big"1e1000",
                         amplitude = 2 + 3im, matrix = [1.0 NaN; 2.0 4.0]))
    roundtrip = JSON3.read(JSON3.write(payload), Dict{String,Any})
    @test roundtrip["invalid"] === nothing
    @test roundtrip["huge"] === nothing
    @test roundtrip["matrix"]["dimensions"] == [2, 2]
    @test roundtrip["matrix"]["values"] == [1.0, 2.0, nothing, 4.0]
    @test roundtrip["amplitude"] == Dict("real" => 2, "imag" => 3)
end
