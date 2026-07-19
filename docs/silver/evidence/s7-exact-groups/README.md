# S7 three-group external evidence candidate

This directory freezes the official external evidence used to review the SOR, XZO, and ANABV
identity groups. It is evidence only: it does not approve an override, transition, tradability
decision, Full run, or Publish.

The content-addressed candidate is
`identity-exact-group-external-evidence-manifest.candidate.json`:

- Manifest ID: `30e3cd9f009c995ce594fd19344ce551ea39133a9c60caea661a7c7211743fdd`
- Candidate SHA-256: `e1a6d365fb2d12f913461576f51003ecabf1bb91a74e546cc717660578f0b17b`
- Latest capture: 2026-07-19 16:26:49 UTC
- Operational availability: 2026-07-20, the first XNYS session opening after capture

Publication/event time, actual project capture time, and operational availability are separate.
In particular, the OpenFIGI responses are a current 2026-07-19 snapshot and are not backdated to
the 2024-2026 event dates.

## Official sources and supported facts

### SOR

- [Source Capital 2024-11-19 Form 8-K](https://www.sec.gov/Archives/edgar/data/91847/000110465924120436/tm2428756d1_8k.htm): announced a reorganization after the 2024-12-31 close and continued NYSE trading under SOR.
- [Issuer/adviser release dated 2024-11-19](https://fpa.com/wp-content/uploads/2025/09/press-release-2024-11.pdf): same announced boundary and ticker continuity.
- [Source Capital Form N-CSR filed 2025-03-07](https://www.sec.gov/Archives/edgar/data/91847/000110465925021647/tm255164d1_ncsr.htm): confirms the Delaware Trust reorganization became effective 2025-01-01 and SOR continued.
- Frozen OpenFIGI v3 results: current SOR is `BBG01RK6N4M5 / BBG01RK6N5G9`; `BBG000KMY6N2` currently represents `XSORX`, also under `BBG01RK6N5G9`; old Share Class `BBG001S5W848` currently projects to `2538080D / BBG000BTBNC7` in the exact US-filtered query.

These facts support separate modeling: a genuine asset transition at the official boundary and,
only for exact post-transition SOR source rows, a provider-scoped stale-Composite correction.
They do not support a global rewrite of `BBG000KMY6N2`.

### XZO

- [Exzeo 2025-11-06 Form 8-K](https://www.sec.gov/Archives/edgar/data/1873951/000119312525270136/d77578d8k.htm): one class of common stock commenced NYSE trading as XZO on 2025-11-05; the IPO closed on 2025-11-06.
- [Issuer closing release, SEC Exhibit 99.2](https://www.sec.gov/Archives/edgar/data/1873951/000119312525270136/d77578dex992.htm): confirms the same trading and closing dates.
- Frozen OpenFIGI v3 results: XZO is `BBG01XL8FHT0 / BBG01227MF17`; the exact US-filtered lookup of observed `BBG01XL8FJS7` returned `No identifier found.`

The no-match is only a property of this frozen response, not proof that the identifier never
existed. Combined with the issuer evidence, it supports an exact-scope Share Class correction
without changing XZO's Composite, asset ID, or 2025-11-04 membership.

### ANABV

- [First Tracks 2026-04-02 Form 8-K](https://www.sec.gov/Archives/edgar/data/2091349/000119312526140835/d111394d8k.htm): explicitly defines regular-way ANAB with the First Tracks entitlement and ex-distribution ANABV without it, with the 2026-04-06 record date and expected 2026-04-20 distribution.
- [AnaptysBio 2026-04-20 Form 8-K](https://www.sec.gov/Archives/edgar/data/1370053/000119312526164330/d120122d8k.htm): confirms completion of the spin-off on 2026-04-20.
- [Issuer completion release, SEC Exhibit 99.1](https://www.sec.gov/Archives/edgar/data/1370053/000119312526164330/d120122dex991.htm): confirms ANAB continued and TRAX began regular-way trading.
- Frozen OpenFIGI v3 results: ANABV is `BBG021DMXXT2 / BBG021GNPBR6`, while ordinary ANAB is `BBG0026ZDHR0 / BBG0026ZDHT8`.

ANABV is therefore modeled as a separate temporary asset. Correcting exact ANABV rows that carry
ordinary ANAB's Share Class must not merge price history, inactivity, or liquidation semantics into
ANAB.

## OpenFIGI request form and admitted fields

All requests use `POST https://api.openfigi.com/v3/mapping`, no API key, exact JSON request bytes,
`marketSecDes=Equity`, and `includeUnlistedEquities=true`. Ticker and Share Class jobs are additionally
restricted to `exchCode=US`. Response headers are allowlisted and contain no cookie or credential.

The review admits `figi`, `ticker`, `exchCode`, `compositeFIGI`, `shareClassFIGI`, `securityType`,
`securityType2`, `marketSector`, `name`, and `securityDescription`. Current name/ticker fields help
interpret the snapshot but are not historical truth.

The official hierarchy definitions are reused from the already frozen
`../s7-cross-market/openfigi-api-documentation.html` and
`../s7-cross-market/figi-allocation-rules.pdf`: Composite FIGI joins trading-venue instruments within
one country/market, while Share Class FIGI joins the same equity class across countries.
