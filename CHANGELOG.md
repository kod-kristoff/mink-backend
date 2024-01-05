# Changelog

## [1.1.0] - 2024-01-05

### Added

- Added new resource type: metadata YAML files. There are now calls for creating, uploading and downloading these.
- It is now possible to upload of source files with uppercase file extensions.

### Changed

- The corpus registry and the job queue have been combined. Now, upon resource creation a job item is created immediately
  (instead of it being created first upon starting a Sparv job).
- The `/check-status`-call has been replaced with `/resource-info` with a different response format.


## [1.0.0] - 2023-09-19

This is the first release of the Mink backend! This application contains functionality for uploading and downloading
corpus-related files, processing corpora with [Sparv](https://spraakbanken.gu.se/sparv/) and installing them in
[Korp](https://spraakbanken.gu.se/korp) and [Strix](https://spraakbanken.gu.se/strix).
