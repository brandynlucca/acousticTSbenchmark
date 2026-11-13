# acousticTSbenchmark

Comparative timing benchmarks for acoustic backscattering by prolate spheroids.

Measures solve times, memory usage, $\Delta \textit{TS}$ (relative to spatial references), and $\textit{RMSE}$ for various parameterized cases across different software.

**Benchmarked software**

| Software                            | Version          | Language                           | Distribution                            |
| ----------------------------------- | ---------------- | ---------------------------------- | ----------------------------------------|
| `acousticTS`                        | 2.1.0            | `R`, `Fortran`<sup>3</sup>, `C++`  | [`CRAN`](https://cran.r-project.org/web/packages/acousticTS/index.html), [`GitHub`](https://github.com/brandynlucca/acousticTS) | 
| `echoSMs`                           | 0.26.0           | `Python`<sup>3</sup>               | [`PyPI`](https://pypi.org/project/echosms/), [`GitHub`](https://github.com/ices-tools-dev/echoSMs) |
| `Prol_Spheroid`                     | 1.0 [Vectorized] | `Python`, `Fortran`<sup>3</sup>    | [`GitHub`](https://github.com/CRIMAC-WP4-Machine-learning/Prol_Spheroid/tree/vectorized) |
| `liquid_spheroid`                   | -                | `Julia`<sup>4</sup>                | [`GitHub`](https://github.com/elavia/liquid_spheroid)|
| `scattering`<sup>1</sup>            | -                | `MATLAB`                           | [`GitHub`](https://github.com/radelman/scattering) |
| `AcousticScattering.jl`<sup>2</sup> | 1.2.0            | `Julia`                            | [`Julia Registry`](https://platform.juliahub.com/ui/Packages/AcousticScattering), [`GitHub`](https://github.com/brandynlucca/AcousticScattering.jl) |

<sup>1</sup> Used for **local** $\Delta \textit{TS}$ and $\textit{RMSE}$ comparisons, but solve times and memory usage were not benchmarked using `GitHub Actions` due to issues configuring a `MATLAB`-based workflow. <br>
<sup>2</sup> Used for the BEM and coupled FEM-BEM spatial references. <br>
<sup>3</sup> These packages each adapt the `Fortran`-based spheroidal wave function kernels hosted by [Arnie Lee Van Buren (`MathieuandSpheroidalWaveFunctions`)](https://github.com/MathieuandSpheroidalWaveFunctions). <br>
<sup>4</sup> `Liquid_Spheroid` adapts the same spheroidal wave function algorithms developed by the `scattering` library.

**Parameterized cases**

All models were benchmarked using shared parameterizations:

| Variable                              | Values                                         |
| ------------------------------------- | ---------------------------------------------- |
| Frequency ($f$, kHz)                  | $[1, 3, 5, \ldots, 297, 299, 300]$<sup>1</sup> |
| Semimajor-axis length ($a$, m)        | 0.001, 0.01, 0.07, 0.1                         |
| Aspect ratio ($\varepsilon$)          | 1.001, 1.01, 5, 10, 50, 100                    |
| Incidence angle ($\theta$, $\degree$) | 0, 37, 90                                      | 

<sup>1</sup> Due to computational constraints, `scattering` and `Liquid_Spheroid` were only benchmarked at $f \in [1, 3, 5, \ldots, 97, 99]$ kHz.

Shared material properties:

| Medium          | Density (kg m<sup>-3</sup>) | Sound speed (m s<sup>-1</sup>) |
| --------------- | --------------------------- | ------------------------------ |
| Exterior fluid  | 1026.80                     | 1447.3                         |
| Liquid interior | 1028.90                     | 1480.3                         | 
| Gas interior    | 1.24                        | 345.0                          | 

Boundary conditions included: **fixed rigid**, **pressure release**, **liquid-filled**, and **gas-filled**

**Software-specific parameterizations**

| Software           | Configuration                                                                                                                |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| `acousticTS`       | `precision = 'quad'` <br> `adaptive = TRUE`                                                                                  |
| `echoSMs`          | Default                                                                                                                      | 
| `Prol_Spheroid`    | `precision_fbs = 1e-5` (gas) <br> `precision_fbs = 1e-6` (liquid) <br> `linear_tolerance = 1e-5` <br> `linear_maxiter = 100` |
| `liquid_spheroid`  | `M = 4` <br> `N = 4` <br> `precision_bits = 256` <br> `method = 2`                                                           |
| `scattering`       | `M = 8` <br> `N = 8` <br> `precision = 500`                                                                                  |

**Container images**

All software were benchmarked using an `ubuntu-latest` container. For cross-platform comparisons, only `acousticTS` was assessed across `ubuntu-latest`, `macos-latest`, and `windows-latest`.

**Dashboards and downloadable artifacts**

[Live progress](https://brandynlucca.github.io/acousticTSbenchmark/) shows time and $\Delta \textit{TS}$ together, a cost–error view, and sampled peak additional RAM per prediction in MiB. Colored X markers show failed predictions at their recorded time to failure. Select `a` (0.001, 0.01, 0.07, 0.1, or 0.5 m) to view its latest saved campaign on the chosen platform. Sizes without results stay empty. Size, aspect ratio, and incidence selections apply to every plot and the coverage table.

[Download all results](results/) as SQLite or compressed CSV. The dataset updates from saved artifacts and keeps earlier software versions. Duplicate copies from resumed runs are removed.
