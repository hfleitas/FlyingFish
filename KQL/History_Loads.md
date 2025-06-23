# 🤿 Generate Daily Ingestion Queries for Historical Data
The kql commands below are to be ran from the Fabric Eventhouse Query Set connected to the Fabric Eventhouse Database - destination. The commands will connect cross-cluster via query and copy the data to the Fabric Eventhouse Database with a specific indexed creation time. Therefore, writing the historical data as of its actual date, instead of the day the commands are run ie. today. This allows the hot cache to be used effectively and its retention settings. 

### 1. Generate Ingestion Commands
The following KQL query generates 34 `.set-or-append` commands to ingest data from the `silver` table in the `dedb-oiultra` database into the `silver` table. Each command covers a single day's data based on `ingestion_time()`, from `2025-05-01 00:00:00` to `2025-06-03 20:44:02.324`, setting `creationTime` to the date of each day (e.g., `2025-05-01` for May 1).

```kql
// Generates daily .set-or-append commands for ingesting historical data from silver to silver_hist.
// Each command covers a 24-hour period (except June 3, which ends at 20:44:02.324).
// creationTime is set to the date of each day (e.g., '2025-05-01') to preserve historical extent metadata.
let StartDate = datetime(2025-05-01 00:00:00);
let EndDate = datetime(2025-06-03 20:44:02.324);
let Days = range Day from StartDate to EndDate step 1d
| summarize Days=make_list(format_datetime(Day, 'yyyy-MM-dd'));
let Queries = Days
| mv-expand Day=Days to typeof(string)
| extend StartTime = todatetime(strcat(Day, ' 00:00:00')),
         EndTime = iff(Day == '2025-06-03', EndDate, todatetime(strcat(Day, ' 23:59:59.9999999'))),
         CreationTime = iff(Day == '2025-06-03', format_datetime(EndDate, 'yyyy-MM-dd'), format_datetime(todatetime(strcat(Day, ' 23:59:59.9999999')), 'yyyy-MM-dd'))
| project Command=strcat(
    ".set-or-append async silver with(creationTime='", CreationTime, "') <| ",
    "cluster('https://adx-ultra-useast.eastus.kusto.windows.net/').database('cluster('https://adx-ultra-useast.eastus.kusto.windows.net').database('dedb-oiultra').silver ').silver ",
    "| where ingestion_time() between (datetime(", format_datetime(StartTime, 'yyyy-MM-dd HH:mm:ss'), ") .. datetime(", format_datetime(EndTime, 'yyyy-MM-dd HH:mm:ss.fffffff'), "))"
);
Queries
| project Command
```

### 2. Get Example queries to test.
This helps confirm the individual commands batch size doesnt time out and copies across the data successfully. You can alter the query to get a `| count` and use `| take 10` to preview the data thats gonna be copied. For reconsiliation verify the count mataches. Note, when using `async` and operation id is returned that can be monitored via `.show operations | where Operation_ID == guid('<myid>')` and `.show operation details`. 

```kql
// May 1, 2025
.set-or-append async silver with(creationTime='2025-05-01') <| 
cluster('https://adx-ultra-useast.eastus.kusto.windows.net').database('dedb-oiultra').silver 
| where ingestion_time() between (datetime(2025-05-01 00:00:00) .. datetime(2025-05-01 23:59:59.9999999))

// May 2, 2025
.set-or-append async silver with(creationTime='2025-05-02') <| 
cluster('https://adx-ultra-useast.eastus.kusto.windows.net').database('dedb-oiultra').silver 
| where ingestion_time() between (datetime(2025-05-02 00:00:00) .. datetime(2025-05-02 23:59:59.9999999))
```

### 3. Run multiple commands in-parrallel as a script.
This helps make the whole process faster without having to attend individual commands. Note, when using `async` and operation id is returned that can be monitored via `.show operations | where Operation_ID == guid('<myid>')` and `.show operation details`. 

```kql
.execute database script <|
// 2025-05-03
.set-or-append async silver with(creationTime='2025-05-03') <| cluster('https://adx-ultra-useast.eastus.kusto.windows.net').database('dedb-oiultra').silver | where ingestion_time() between datetime(2025-05-03 00:00:00) .. datetime(2025-05-03 23:59:59.9999999))
// 2025-05-04
.set-or-append async silver with(creationTime='2025-05-04') <| cluster('https://adx-ultra-useast.eastus.kusto.windows.net').database('dedb-oiultra').silver | where ingestion_time() between (datetime(2025-05-04 00:00:00) .. datetime(2025-05-04 23:59:59.9999999))
// 2025-05-05
.set-or-append async silver with(creationTime='2025-05-05') <| cluster('https://adx-ultra-useast.eastus.kusto.windows.net').database('dedb-oiultra').silver | where ingestion_time() between (datetime(2025-05-05 00:00:00) .. datetime(2025-05-05 23:59:59.9999999))
// 2025-05-06
.set-or-append async silver with(creationTime='2025-05-06') <| cluster('https://adx-ultra-useast.eastus.kusto.windows.net').database('dedb-oiultra').silver | where ingestion_time() between (datetime(2025-05-06 00:00:00) .. datetime(2025-05-06 23:59:59.9999999))
// 2025-05-07
// 2025-05-08
// 2025-05-09
// 2025-05-10
```

### 4. Final command
Should have the right end time like so, granted the `min(ingestion_time())` of the destination `silver` table is greater.

```kql
// 2025-06-03
.set-or-append async silver with(creationTime='2025-06-03') <| 
cluster('https://adx-ultra-useast.eastus.kusto.windows.net').database('dedb-oiultra').silver 
| where ingestion_time() between (datetime(2025-06-03 00:00:00) .. datetime(2025-06-03 20:44:02.324))
