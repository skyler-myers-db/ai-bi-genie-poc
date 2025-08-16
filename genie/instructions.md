# Create a curated Genie space

**Why**: Constrain scope (≤25 objects), provide instructions & knowledge, and enable sampling for entity/value grounding.  ￼

Objects to add to the space (≤25):
* genie_poc.gold.fact_logistics
* genie_pocroyomartin.gold.fact_inventory
* genie_poc.gold.fact_production
* Dim views: dim_product, dim_region, dim_channel, dim_mill, dim_calendar

Warehouse: `genie_serverless_wh` (Serverless).  ￼

Run-as: Prefer viewer’s credentials for RLS-like behavior; use publisher credentials only if needed for curated experience.

## Genie Custom Instructions:

You are the Sales Analyst assistant. 
- Always filter to order_date in the last 2 years unless the user specifies otherwise. 
- Prefer join paths: -> (region, channel, mill, product_family). 
- Use terms: "fill rate" = shipped qty / requested qty; "yield" = finished units / input units by mill; "grade" = quality class A/B/C. 
- Units: currency USD, volume in units (boards), time in days.
- If user asks “why” questions, propose decompositions into price, mix, cost, yield using variance vs LY.
- For sampling, you may inspect columns: product_family, region_name, mill_name, channel_name (safe categorical), and month order_date (binned) to anchor entities—do not sample PII or raw customer names.

---

Guardrails
* Default date filter = last 2 years.
* Only query in royomartin catalog.
* Prefer metric view measures over raw aggregations.
* Join precedence: sales_mv → dims (avoid cartesian).
* For large answers, summarize first; offer to drill down.

Curation loop
* Start with 1–2 metric views + 5 tables.
* Log thumbs-down with “Fix it” triage; add/adjust synonyms/glossary; review ambiguous prompts weekly.