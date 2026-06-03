# Schema reference — Bronze layer

All Bronze tables include these system columns appended at ingestion time:
- `source_file VARCHAR` — original XLSX filename
- `ingested_at TIMESTAMP` — UTC timestamp of ingestion run

ANRT XLSX files often have merged cells and multi-row headers.
Always inspect the file before assuming header row index.

---

## bronze_anrt_mobile
Source: Parc téléphonie mobile (2006–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | 'Q1'/'Q2'/'Q3'/'Q4' or NULL for annual rows |
| operator | VARCHAR | Normalize: 'Maroc Telecom', 'Orange Maroc', 'Inwi', 'Total' |
| total_subs | BIGINT | Total subscribers |
| prepaid_subs | BIGINT | Prepaid subscribers |
| postpaid_subs | BIGINT | Postpaid subscribers |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_internet
Source: Parc internet (2006–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| technology | VARCHAR | 'ADSL', '4G', 'Fiber', 'FH', 'Total' |
| subscribers | BIGINT | |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_fixed
Source: Parc téléphonie fixe (2006–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| total_subs | BIGINT | |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_qos
Source: Qualité de service réseaux mobiles (2017–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| operator | VARCHAR | |
| indicator | VARCHAR | Raw indicator name from XLSX |
| value | DOUBLE | Numeric value |
| unit | VARCHAR | '%', 'dBm', 'ms', etc. if present |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_traffic
Source: Trafic sortant voix & SMS (2006–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| segment | VARCHAR | 'mobile_mobile', 'mobile_fixe', 'mobile_international', 'fixe_mobile', etc. |
| voice_minutes | BIGINT | Minutes of outgoing voice |
| sms_count | BIGINT | Outgoing SMS count |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_arpm
Source: ARPM & Facture internet (2010–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| arpm | DOUBLE | Revenue per minute (MAD/min) |
| internet_bill_avg | DOUBLE | Average monthly internet bill (MAD) |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_bandwidth
Source: Bande passante Internet internationale (2006–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| capacity_gbps | DOUBLE | International bandwidth in Gbps |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_complaints
Source: Plaintes consommateurs (2019–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| operator | VARCHAR | |
| complaint_type | VARCHAR | Raw category from XLSX |
| count | INTEGER | Number of complaints |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_portability
Source: Portabilité mobile + Portabilité fixe (merged, 2016–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| segment | VARCHAR | 'mobile' or 'fixed' |
| ported_numbers | INTEGER | |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_usage_avg
Source: Usage moyen mensuel (2010–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| mobile_minutes | DOUBLE | Avg monthly outgoing mobile minutes per subscriber |
| fixed_minutes | DOUBLE | Avg monthly outgoing fixed minutes per subscriber |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_ip
Source: Usage adresses IP (2013–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| ipv4_count | BIGINT | Number of allocated IPv4 addresses |
| ipv6_prefixes | INTEGER | Number of IPv6 prefixes |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_data_links
Source: Liaisons Data Entreprises (2018–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| link_type | VARCHAR | Raw type from XLSX |
| count | INTEGER | Number of enterprise data links |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_tic_survey
Source: Résultats enquêtes TIC (2004–2024)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| indicator | VARCHAR | Raw indicator name |
| value | DOUBLE | |
| unit | VARCHAR | '%', 'per 100 inhabitants', etc. |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_domains
Source: Attribution noms de domaine .ma (2010–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| active_domains | INTEGER | Active .ma domains |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_anrt_payphones
Source: Parc des Publiphones (2006–2025)

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| quarter | VARCHAR | |
| total_payphones | INTEGER | |
| source_file | VARCHAR | |
| ingested_at | TIMESTAMP | |

---

## bronze_itu_morocco
Source: ITU DataHub — Morocco slice

| Column | Type | Notes |
|---|---|---|
| year | INTEGER | |
| indicator_code | VARCHAR | ITU indicator code (e.g., 'i99H') |
| indicator_name | VARCHAR | Human-readable name |
| value | DOUBLE | |
| unit | VARCHAR | '%', 'per 100 inhabitants', 'USD', etc. |
| ingested_at | TIMESTAMP | No source_file (HTTP download) |

Key indicators to extract:
- Mobile cellular subscriptions per 100 inhabitants
- Fixed broadband subscriptions per 100 inhabitants
- Individuals using the internet (%)
- ICT Development Index (IDI)
- Revenue (USD millions)
- Investment (USD millions)
