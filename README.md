# acousticTSbenchmark

Comparative timing benchmarks for acoustic backscattering by prolate spheroids.

Times acousticTS, Prol_Spheroid (vectorized), echoSMs, and liquid_spheroid. AcousticScattering.jl supplies cached BEM/FEM-BEM references for ΔTS comparisons.

In **Actions → Dense spheroid timings → Run workflow**, choose `new` and set the frequency range/interval (kHz), semimajor axis `a` (m), aspect ratios, incidences, boundaries, software, and platform. Defaults cover all four boundaries at 0°, 37°, and 90° with acousticTS and Prol_Spheroid from 1 to 300 kHz. Unsupported combinations are recorded as skips. Cases alternate across boundaries and angles at each frequency.

Choose `resume` and enter the previous run ID to continue its saved cases with its original settings and ordering, including platforms that were queued when the run stopped. The default budget is 240 minutes per platform, including setup, with 45 and 120 minutes also available. Large grids require multiple sessions. References run separately and are cached. Results are downloadable artifacts.

[Live progress](https://brandynlucca.github.io/acousticTSbenchmark/) shows time and ΔTS together, a cost–error view, and sampled peak additional RAM per prediction in MiB. The incidence selector filters every plot and the coverage table.
