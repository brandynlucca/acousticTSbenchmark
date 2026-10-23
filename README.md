# acousticTSbenchmark

Comparative timing benchmarks for acoustic backscattering by prolate spheroids.

Times acousticTS, Prol_Spheroid (vectorized), echoSMs, and liquid_spheroid. AcousticScattering.jl supplies cached BEM/FEM-BEM references for ΔTS comparisons.

In **Actions → Dense spheroid timings → Run workflow**, choose `new` and set the frequency range/interval (kHz), semimajor axis `a` (m), aspect ratios, incidences, boundaries, and software. Defaults run acousticTS and Prol_Spheroid from 1 to 300 kHz.

Choose `resume` and enter the previous run ID to continue its saved cases with its original settings. Runs have a 45-minute budget. References run separately and are cached. Results are downloadable artifacts.

[Live progress](https://brandynlucca.github.io/acousticTSbenchmark/) shows time and ΔTS together, a cost–error view, and sampled peak additional RAM per prediction in MiB.
