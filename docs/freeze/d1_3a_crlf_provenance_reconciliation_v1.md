# D1.3a CRLF Provenance Reconciliation

Date: 2026-10-08
Pre-fix5 head: `80ff463b7bff8e7cfe92e05a1912c90a3801fd6b`
Canonical rebuild commit: `cf8f90ec33186163054b1b14ca5592b68bada0cd`

The v4/v5 receipts were produced from a Windows checkout whose CRLF bytes did
not equal the LF bytes stored in Git. This was an exact-byte reproducibility
defect, not evidence that the semantic input content differed.

| Input | Committed Git-byte SHA-256 | Previously recorded worktree SHA-256 | Result |
|---|---|---|---|
| `data_real/pilot_queries/independent_template_pack_v4.yaml` | `c57d05cb42989f0a406a3e48a4fb6f3dda822121b0db17bb1f72e5be91f66d8f` | `7e38cb8ccac24cd93695b25b131cbe0e188b93dfba89316eb032288a6e5c47cd` | CRLF transform of committed LF bytes |
| `data_real/pilot_queries/schema_metadata.yaml` | `d3e0ee543e603a3b545c406c4fd3b7275c779c9d4c64617b2edaa0c7a2f2d201` | `d213ff4981c1c93fd581c6095e0710cdb9cfd8dccba15b3bee363962b09c907d` | CRLF transform of committed LF bytes |

No v4/v5 history was rewritten. The v6 development regression was rebuilt in
a detached checkout from `cf8f90e` with checkout conversion disabled. Every
tracked direct generation/evaluation input was compared byte-for-byte against
`git cat-file blob`; the canonical Git-byte gate passed. v6 is therefore the
first exact-Git-byte canonical D1.3a regression evidence.
