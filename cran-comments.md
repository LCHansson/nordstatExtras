## R CMD check results

0 errors | 0 warnings | 0 notes

## Test environments

- Local: Windows 11, R 4.5.2
- GitHub Actions: ubuntu-latest (R-release, R-devel), macos-latest (R-release),
  windows-latest (R-release)

## Notes

This is the first CRAN submission of nordstatExtras.

The package provides an optional SQLite-backed cache for the nordstat
family of R packages (rKolada, rTrafa, pixieweb). mirai is listed in
Suggests for optional async background writes; when not installed,
writes fall back to synchronous mode with a one-time warning.
