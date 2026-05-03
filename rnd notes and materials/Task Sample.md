# Task Sample

Quick taste of the project so you can decide if it fits: A quick note — I'm not in the trucking industry. I'm using it as an example because it makes the data shape easy to visualise. The real dataset is in a different domain, but everything below — structure, scale, analyses, delivery requirements — is accurate.
Imagine a public dataset that lists every commercial truck operating across a region. Each row is one truck, for one year. A simplified preview of what one slice of it looks like

| Truck ID | Operator | Type | Size (t) | Age | Fuel (kL) | CO₂ (t) | km | ... | Voluntary metric A | Voluntary metric B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10001 | C-A12 | Long-haul | 40 | 6 | 124.3 | 328.1 | 198,400 | ... | 87.2 | — |
| 10002 | C-A12 | Long-haul | 40 | 11 | 141.8 | 374.5 | 182,700 | ... | — | — |
| 10003 | C-B07 | Refrigerated | 26 | 3 | 89.6 | 236.7 | 142,100 | ... | 91.4 | 0.42 |
| 10004 | C-B07 | Regional | 18 | 9 | 52.1 | 137.6 | 88,300 | ... | — | — |
| 10005 | C-C33 | Tanker | 32 | 14 | 118.9 | 314.0 | 156,800 | ... | 76.8 | 0.31 |
| 10006 | C-D04 | Long-haul | 40 | 2 | 109.7 | 289.6 | 211,200 | ... | 92.1 | 0.48 |
| ... | ... | ... | ... | ... | ... | ... | ... | ... | ... | ... |

What I need the analysis to do, in plain terms:

1. Peer benchmarking of a single truck. Given Truck X, find its true peer group — same type, similar size, similar age — and rank its performance against that peer group. Output a percentile, a trend over years, and a clean visual.
2. Operator-level benchmarking. Roll truck-level performance up to the operator. Two questions: (a) how do an operator's trucks perform relative to the broader fleet, and (b) how does the operator perform relative to other operators with similar fleet composition?
3. Rankings. Top and bottom performers at both truck and operator level, segmented by type. Refreshed annually.
4. Anomaly detection. Within a single truck's own history, detect when its performance suddenly deviates from its baseline — controlling for what's normal for its peer group. The "did something change?" signal.
5. Forward projection. Project each truck's performance in 3, 5, 10 years given current trends. Identify trucks likely to cross defined thresholds in future years.
On delivery — this matters as much as the analysis itself. The output of this work needs to be consumable by other systems and other people, not just by me in a notebook. Specifically:

A clean internal API that exposes per-truck and per-operator analytical outputs (percentiles, rankings, projections, anomaly flags) so they can be queried programmatically.
Clean report delivery — quarterly or annual outputs in PDF/Excel/CSV.