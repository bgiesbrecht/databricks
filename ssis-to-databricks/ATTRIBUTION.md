# Attribution & Sample Data

## Sample packages in this repo

Everything under `samples/synthetic/` was **authored for this project** — small,
hand-built `.dtsx` packages created to exercise specific SSIS components (Pivot, Unpivot,
Fuzzy Lookup, Cache Transform, ADO ForEach enumerator). They contain no third-party
content and no real connection details.

## Third-party sample packages (NOT redistributed here)

During development this tool was tested against several public GitHub repositories of
real SSIS packages. Those packages are **not** included in this repo — some are
GPL-licensed and others carry no license (all rights reserved), so redistribution isn't
appropriate here. Instead, `scripts/fetch_samples.sh` clones them locally on demand for
testing, and full credit is given below.

| Repository | Author | License | Used for |
|---|---|---|---|
| [GoodmanNeil/SSIS-Examples](https://github.com/GoodmanNeil/SSIS-Examples) | GoodmanNeil | GPL-3.0 | Microsoft "Creating a Simple ETL Package" tutorial (Lessons 1–6) — Lookup, ForeachLoop, Script Component |
| [RanaGaballah/DataWareHouse_SSIS](https://github.com/RanaGaballah/DataWareHouse_SSIS) | RanaGaballah | No license (all rights reserved) | SCD via Conditional Split + OLE DB Command |
| [NirmalAndrews/IntegrationServicesSamples](https://github.com/NirmalAndrews/IntegrationServicesSamples) | NirmalAndrews | No license (all rights reserved) | Reference examples: Pivot, Unpivot, Fuzzy Lookup, Cache, Row Count, all ForEach enumerators |
| [Henokagb/ETL-EBusiness-data_SSIS](https://github.com/Henokagb/ETL-EBusiness-data_SSIS) | Henokagb | No license (all rights reserved) | Aggregate, Multicast, Union All, Data Convert |
| [marcelmotta/IMSports-ETL](https://github.com/marcelmotta/IMSports-ETL) | marcelmotta | No license (all rights reserved) | Merge Join, Sort, Data Convert (40-node staging package) |
| [safizaidi98/Inremental-Load-SCD-Merge-Join-Lookup-Knowledge-Star-Project-SSIS](https://github.com/safizaidi98/Inremental-Load-SCD-Merge-Join-Lookup-Knowledge-Star-Project-SSIS) | safizaidi98 | No license (all rights reserved) | Merge Left Join + SCD |
| [niroshank/sttm-dimenisonal-dw-ssis-scd-tutorial](https://github.com/niroshank/sttm-dimenisonal-dw-ssis-scd-tutorial) | niroshank | No license (all rights reserved) | Native Slowly Changing Dimension wizard (Microsoft.SCD) |

Some of the tutorial packages mirrored above are derived from Microsoft's official SSIS
tutorial content (© Microsoft). Refer to each upstream repository for its own terms.

## Relationship to Databricks Lakebridge

This project is an independent, experimental **complement** to
[Databricks Lakebridge](https://github.com/databrickslabs/lakebridge) — the official,
supported SSIS→Databricks migration tooling. It is not affiliated with or endorsed by the
Lakebridge project, and reuses none of its code. See the README's
"When to consider this approach" section.
