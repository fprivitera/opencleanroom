# Chicago Taxi Trips: a real dataset

[Chicago Taxi Trips](https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz) is published by the City of Chicago on its Data Portal, under the portal's terms of use. Each trip carries a hashed `taxi_id`, which makes the taxi a natural privacy unit: one taxi contributes hundreds of trips a month. The NIST Differential Privacy Temporal Map Challenge (2020, sprint 3) used the same data and asked for taxi-level privacy.

```bash
mkdir chicago && cd chicago
PYTHON=python bash /path/to/packages/opencleanroom/examples/chicago/quickstart.sh
```

`fetch_chicago_taxi.py` reads the portal's CSV API one day at a time and writes `chicago_taxi.duckdb` with two tables:

- `trips` (private, truncated): `taxi_id`, `trip_date`, pickup and dropoff community area, `trip_seconds`, `trip_miles` in tenths, `fare` and `tips` in cents, `payment_type`, `company`. Integers only, as the noise works on integers.
- `community_areas` (public): the 77 community areas from the city's boundary dataset, the keyset for pickups.

The data file holds real records. It stays out of version control (`*.duckdb` is ignored); the policy and the scripts are all the repository carries.

## Policy choices (`packages/cleanroom-sql/examples/chicago_taxi.yaml`)

The admin sets `k`, the clamps and the keysets (ADR 0005); nothing below is learned at query time. They were chosen on March 2025 (562,670 trips, 2,664 taxis):

| Trips per taxi in the month | median 190, 90th percentile 392, maximum 950 |
|---|---|
| Share of trips truncation keeps | k = 300: 88.9%, k = 400: 95.8%, k = 500: 98.6% |

`k = 400` keeps 95.8% of the trips; a larger `k` buys little truncation bias and costs proportionally more noise. Clamps sit near the 99th percentile of the month: 4,800 seconds, 30 miles, $80 fare, $20 tip. The budget epoch is the month (`2025-03`); the payment types are an admin-declared list, `company` has no keyset, so grouping by it is refused.

## What the answers look like

Median relative error over 20 draws on March 2025, against the truncated true value:

| Query | ε | Median relative error |
|---|---|---|
| `SELECT COUNT(*) FROM trips` | 0.1 | 0.4% |
| `SELECT SUM(fare) FROM trips` | 0.1 | 1.8% |
| `SELECT AVG(fare) FROM trips` | 0.2 | 1.5% |
| `COUNT(*)` per payment type, groups of 66,000 to 198,000 trips | 0.2 | 0.5% to 1.3% |
| `COUNT(*)` per community area, groups above 50,000 trips | 0.3 | about 1% |
| the same, Lincoln Park (11,805 trips) | 0.3 | 10% |
| the same, Hyde Park (4,763 trips) | 0.3 | 19% |
| the same, Austin (1,098 trips) | 0.3 | 53% |

The noise is set by `k` and the clamp, not by the size of a group: large groups are accurate, small ones are not, and a consumer sees this through the 95% interval returned with every answer. An `AVG` over a small group divides two noisy values and can land far from the truth, even below zero.
