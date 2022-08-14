# Changelog and versioning

## Versioning Policy

Our API and its client libraries adopt MAJOR.MINOR.PATCH format
for version numbers. These version numbers conform to
[semantic versioning](https://semver.org). This means that you can upgrade
minor or patch versions to pick up new functionality and fixes, without breaking
your integration.

Most often, we introduce backward-compatible changes between minor versions
in the form of:

- New data schemas or encodings
- Additional fields to existing data schemas
- Additional batch download customizations

Our API and official client libraries are kept in sync with same-day releases
for major and minor versions. For instance, `v1.2.x` of the Python client
library will use the same functionality found in `v1.2.y` of the HTTP API.

Each major version is guaranteed to operate for two years after the date
of the subsequent major release. For example, if `v2.0.0` is released on
January 1, 2022, then all versions `v1.x.y` of the API and client libraries
are deprecated after January 1, 2024. When a version is deprecated,
any calls made to the API will be defaulted to the next oldest, usable version.
However, you should assume that the calls will not work. Likewise, the client
libraries rely on the API and cannot be assumed to work against later versions
of the API.

We recommend to pin your version requirements against `0.x.*` or `0.x.y`.
Either one of the following is fine:

- `databento>=0.2,<1.0`
- `databento==0.3.0`

Additionally note:
- All undocumented APIs are considered internal. They are not part of this contract.

- Certain features (e.g. integrations) may be explicitly called out as
"experimental" or "unstable" in the documentation.



## 0.2.0 - 2021-12-10
 - Added backend endpoint APIs
 - Added async stream support
 - Added `Bento` convenience objects
 - Updated Changelog and versioning policy

## 0.1.0 - 2021-08-30
 - Added support for remote procedure call (RPC) streaming requests
 - Refactored legacy code
