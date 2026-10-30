
Statement
The core problem in your own words. Must-haves vs nice-to-haves.

core problem is spotify-analytics-service .
user should be able to get his analytics and montly and yearly-wrap of his activity  .
we get all the analytics of how much user spent and what is his ranking 

functional-requirments:
1. analytics api will be generated from clicks , plays and all the user activity .
2. out of order events will be arranged by the client timeline itself 
3. the analytics will be realtime 

nice-to-have :
ui to showcase the dashboards and trends .
recommendation engine to show the feed .
personal chatbot that has the context as the user-memory .

What you're solving and NOT solving. 3–4 sentences

solving the realtime problem for analytics .
solving the out-of-order issue broughtup in the spec .
solving the unstable schema problem. 
solving tenant isolation , one tenant should never see another tenant's data .

not-solving :
collection from the ui .
display of data-dashboards in the uis .
the primary service / write path , other teams put users and tracks in mongo and plays and clicks in kafka , we start from there .
user ranking (his position among other users in the tenant) , not built yet , the report only shows his own totals and top tracks .


Shared
Architecture
Block diagram of the full system. All major components, data flows,
and integration points. 1 diagram + labels

https://excalidraw.com/#json=cHuYMzqb2RjgVMuh4Cl4h,GH3_ht0i6KIKb092PwM_8g


how multi-tenant works :
1. every event and every mongo doc has a tenant_id . all tenants share the same tables , tenant_id is first in every sort key .
2. web-app calls the analytics-service with a signed jwt . missing , bad or expired token = 401 .
3. tenant comes from the token only , never from a header , url or body .
4. service runs every clickhouse query as a read-only user with SQL_tenant_id set to that tenant .
5. two filters apply . the query's own WHERE tenant_id , and a row policy in clickhouse (tenant_id = SQL_tenant_id) . if the setting is missing the query fails , it doesnt return everything .
6. a user id from another tenant gives 404 , not 403 .

Your
Component
Which piece you own. Inputs, outputs, interfaces. How it connects to
the rest of the architecture. 3–5 bullet points

i own the analytics-service , live-sync pipeline and clickhouse data for the reports generation .

input:
data from the frontending service which will be populated into mongo-db and kafka , from various sources .

outputs:

analytics 
rest api for user reports , wrapped , recommendations and tenant dashboards , plus a small ops api (pipeline lag , schema fields) .

how it connects :
- kafka -> clickhouse kafka engine -> materialized views -> rollups -> nestjs analytics-service .
- mongo -> flink cdc -> kafka , same path from there .
- jwt http api on the front , contracts/events.md is what producers agree to .

Key Trade-offs 2–3 group decisions and why. What alternatives were considered and
rejected. Be specific. 3–5 bullets

- plays and clicks go straight to kafka , only users and tracks use cdc . users/tracks are business data the app reads back so the db stays the source of truth , telemetry has no reason to go through a db . rejected : cdc for everything (more load on mongo , slower) .
- clickhouse reads kafka directly , no flink in between . one less hop and deployment , flink only stays for cdc . rejected : flink for all ingestion .
- shared tables with row policies , not a table or db per tenant . 500+ tenants would mean 500+ schemas to migrate . cost is that isolation depends on policies and tokens , so the db enforces it too . rejected : filtering in app code only .
- keep the full message as raw plus typed columns , at-least-once with dedup on event_id . rejected : strict schema at ingest (breaks every producer release) , exactly-once end to end (too costly) .
- dashboards read 1-minute rollups not raw tables , so cost grows with time range not event volume .

Task
Breakdown
3–6 buildable tasks for the whole system. Prioritized — which first and
why. Mark YOUR task. Numbered list

1. event contract + kafka topics keyed by tenant_id:user_id (shared , first because everything depends on it) .
2. primary db for users and tracks (other teams) .
3. flink cdc mongo -> kafka with savepoints (MINE) .
4. clickhouse ingest , validation , rollups and row policies (MINE) .
5. analytics-service : auth , reports , wrapped , recommendations , dashboards (MINE) .
6. next : user ranking per tenant , from a rollup so it stays cheap .
7. next : data lake from the same topics for batch history and nightly rollup reconciliation .

Scale & Failure Where the design breaks first under load. What would need to
change. Known failure modes. 2–3 sentences 

breaks first : clickhouse merges and ingest on a single node , and one heavy tenant slowing the rest (no per-tenant limits yet) . at 50k events/s we need more partitions (48-96) , more consumers , clickhouse sharded by tenant with replicas and per-tenant query limits .
failure modes : clickhouse down (kafka holds the stream , it resumes from its offset) . bad message (goes to ingest_errors , stream keeps moving) . producer writes the wrong tenant_id (outside our boundary , needs producer auth) . jwt secret leaks (anyone can mint tokens , needs asymmetric keys) .
