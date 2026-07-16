# S7 cross-market external evidence candidate

This directory freezes the official source bytes used to redesign S7 cross-market identity
handling. It is a schema-review artifact only: it is **not** an adjudication plan, approval,
registry release, FullRunPlan, or PublishPlan.

The three OpenFIGI request/response pairs were captured without an API key. Requests use
`includeUnlistedEquities=true`; this is required for inactive or delisted instruments. Response
headers contain only an allowlisted public server response and no request credentials.

`identity-cross-market-external-evidence-manifest.candidate.json` binds every raw file by relative
path, byte count, and SHA-256. Its nine group assertions preserve the provider-observed foreign
Composite FIGI and nominate a separate canonical U.S. Composite FIGI for future human review.
The 19 existing detector cases remain unchanged and are referenced only by the already frozen
preview ID/count until a separately approved candidate-promotion step supplies exact case IDs.

OpenFIGI is a current identifier snapshot, not a 2022 point-in-time source. It establishes the
Composite/Share-Class hierarchy; the provider-contamination conclusion additionally depends on
the pinned Massive U.S.-locale lineage and the frozen SEC/issuer action-date evidence. Names and
tickers in current OpenFIGI responses are not treated as historical identity truth.
