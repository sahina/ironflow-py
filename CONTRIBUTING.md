# Contributing

**This repository is a read-only mirror.** External contributions are not accepted here.

The Ironflow Python SDK source is maintained in a private engine repository. This mirror is updated automatically by the release pipeline at each version tag.

## Reporting bugs

Issues are disabled here. Everything goes to one tracker: [sahina/ironflow-issues](https://github.com/sahina/ironflow-issues/issues/new/choose).

- SDK bugs (in `ironflow-py`) → file there and pick **Python SDK** as the component. Include your Python version, platform, `pip --version`, repro steps, and a minimal example.
- Engine, CLI, dashboard, and desktop bugs → same tracker, pick the matching component.
- Security issues → [private advisory](https://github.com/sahina/ironflow-issues/security/advisories/new) or see [SECURITY.md](SECURITY.md). Do **not** open a public issue.

## Pull requests

Pull requests opened against this repository will be closed without review. The only commits expected here come from the release pipeline mirroring source from the engine repo.

If you have a fix in mind, file an issue describing the bug and the proposed fix. The engine team will land the change in the private repo and it will appear here at the next release.

## Generated code

`ironflow/_gen/` is generated from the engine's protobuf definitions and vendored here as source. Do not hand-edit it, in this repo or anywhere else — the next release overwrites it. The generator lives in the engine repo (`make proto-python`).

## License inquiries

For commercial licensing, see the contact in [LICENSE](LICENSE).
