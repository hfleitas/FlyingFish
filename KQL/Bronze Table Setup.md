# 🤿 Bronze Table Setup and Diagnostic Queries

## Pre-requisites
- The `ingest` table exists.
- Target tables (`Silver_IRIMeasurements`, `Silver_IRIDefects`, `Silver_TESMeasurements`, `Silver_BlankWatchTemperature`, `Silver_BlankWatchGobLoading`) are created with appropriate schemas.
- Familiarity with KQL, ADX Web UI (https://dataexplorer.azure.com/), or Fabric Real-Time Intelligence (RTI) KQL Queryset.

## 🏅 Objectives
- Create and configure the `bronze` table to ingest data from the `ingest` table with an update policy.
- Define transformation functions and update policies for `bronze`  and target tables (`Silver_IRIMeasurements`, `Silver_IRIDefects`, `Silver_TESMeasurements`, `Silver_BlankWatchTemperature`, `Silver_BlankWatchGobLoading`) to process measurement and defect data.
- Verify table setup, ingestion batching policies, and data flow.

## 📃 Tasks
1. Bronze and Silver Tables Update Policies and Assignments
2. Verify setup, and data with diagnostic queries.

## 🪜 Steps

### 1. Bronze and Silver Tables Update Policies and Assignments
```kql
// Creates the bronze table, defines transformation functions for silver and target tables, and attaches update policies.
// Ensures data flows from ingest to bronze  and is transformed into target tables for IRI measurements, IRI defects, cold system measurements, blank watch temperature, and blank watch gob loading.
    // Create bronze table to ingest data from the ingest table.
    .create table bronze (
        timestamp: datetime,
        device: string,
        category: string,
        ['location']: string,
        productionLine: string,
        lineUpl: string,
        ['data']: dynamic
    )

    // Define function to transform ingest table data to bronze table.
    .create-or-alter function with (
        docstring = "Defines the transformation from ingest table to bronze table",
        folder = "UpdatePolicies"
    )
    UpdatePolicy_Ingest() {
        ingest
        | project
            timestamp,
            device=tostring(properties.device),
            category=tostring(properties.category),
            location=tostring(properties.location),
            productionLine=tostring(properties.productionLine),
            lineUpl=tostring(properties.lineUpl),
            data
    }

    // Attach update policy to bronze to automate data ingestion from ingest.
    .alter table bronze policy update
    @'[{"Source": "ingest", "Query": "UpdatePolicy_Ingest()", "IsEnabled": true}]'

    // Define function for IRI measurements, extracting details from data dynamic column.
    .create-or-alter function with (
        docstring = "Transforms bronze table data for IRI measurements into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToIRIMeasurements() {
        bronze
        | where device == 'iri' and category == 'measurements'
        | project
            utctimestamp=timestamp,
            location,
            production_line=productionLine,
            lineup=lineUpl,
            id_container=tostring(data.idContainer),
            leg=tostring(data.fp),
            pocket=tostring(data.pocket),
            cavity=tostring(data.cavity),
            position=tostring(data.position),
            measurements=data.measurements
        | mv-expand kind=array measurements
        | extend
            inspection_name=tostring(measurements[0]),
            measurements=measurements[1]
        | mv-expand kind=array measurements
        | extend
            measurement_name=tostring(measurements[0]),
            measurements=bag_remove_keys(measurements[1], dynamic(['probeNames','thresholds'])),
            probe_names=measurements[1]['probeNames'],
            thresholds=measurements[1]['thresholds']
        | mv-expand kind=array measurements
        | extend
            probe_number=substring(measurements[0], 6, 1),
            value=toreal(measurements[1])
        | extend
            probe_label=strcat(probe_number, " ", probe_names[probe_number]),
            threshold=thresholds[probe_number]
        | project-away measurements, probe_names, thresholds, probe_number
    }

    // Drop and recreate Silver_IRIMeasurements table with appropriate schema.
    .drop table Silver_IRIMeasurements ifexists
    .create table Silver_IRIMeasurements (
        utctimestamp: datetime,
        location: string,
        production_line: string,
        lineup: string,
        id_container: string,
        leg: string,
        pocket: string,
        cavity: string,
        position: string,
        inspection_name: string,
        measurement_name: string,
        value: real,
        probe_label: string,
        threshold: dynamic
    )

    // Attach update policy to Silver_IRIMeasurements.
    .alter table Silver_IRIMeasurements policy update
    @'[{"Source": "bronze", "Query": "BronzeToIRIMeasurements()", "IsEnabled": true}]'

    // Define function for IRI defects, extracting defect details.
    .create-or-alter function with (
        docstring = "Transforms bronze table data for IRI defects into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToIRIDefects() {
        bronze
        | where device == "iri" and category == "defects"
        | project
            utctimestamp = timestamp,
            lineupl = lineUpl,
            location,
            id_container = tostring(data.idContainer),
            leg = tostring(data.fp),
            position = tostring(data.position),
            defects = data.defects
        | mv-expand defects
        | extend
            inspection_name = tostring(defects[0]),
            defects = defects[1]
        | mv-expand probe = bag_keys(defects)
        | extend defect_name = tostring(defects[tostring(probe)])
        | project-away defects
    }

    // Drop and recreate Silver_IRIDefects table.
    .drop table Silver_IRIDefects ifexists
    .create table Silver_IRIDefects (
        utctimestamp: datetime,
        lineupl: string,
        location: string,
        id_container: string,
        leg: string,
        position: string,
        inspection_name: string,
        probe: dynamic,
        defect_name: string
    )

    // Attach update policy to Silver_IRIDefects.
    .alter table Silver_IRIDefects policy update
    @'[{"Source": "bronze", "Query": "BronzeToIRIDefects()", "IsEnabled": true}]'

    // Drop and recreate function for cold system measurements.
    .drop function BronzeToColdsystemMx ifexists
    .create-or-alter function with (
        docstring = "Transforms bronze table data for cold system measurements into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToColdsystemMx() {
        bronze
        | where device == "coldsystem" and category == "mx"
        | project
            utctimestamp=timestamp,
            location,
            production_line=productionLine,
            lineupl=lineUpl,
            id_container=tostring(data.ContainerID),
            leg=tostring(data.Leg),
            pocket="",
            cavity=tostring(data.MoldNumber),
            position=tostring(data.Position),
            measurements=data.measurements
        | mv-expand kind=array measurements
        | extend
            inspection_name=tostring(measurements[0]),
            measurements=measurements[1].measures
        | where isnotempty(measurements)
        | mv-expand kind=array measurements
        | extend
            probe_label = "",
            threshold=dynamic(null)
        | extend
            measurement_name=tostring(measurements[0]),
            value=toreal(measurements[1])
        | project-away measurements
    }

    // Drop and recreate Silver_TESMeasurements table.
    .drop table Silver_TESMeasurements ifexists
    .create table Silver_TESMeasurements (
        utctimestamp: datetime,
        location: string,
        production_line: string,
        lineupl: string,
        id_container: string,
        leg: string,
        pocket: string,
        cavity: string,
        position: string,
        inspection_name: string,
        probe_label: string,
        threshold: dynamic,
        measurement_name: string,
        value: real
    )

    // Attach update policy to Silver_TESMeasurements.
    .alter table Silver_TESMeasurements policy update
    @'[{"Source": "bronze", "Query": "BronzeToColdsystemMx()", "IsEnabled": true}]'

    // Define function for blank watch temperature measurements.
    .create-or-alter function with (
        docstring = "Transforms bronze table data for blank watch temperature into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToBlankWatchTemperature() {
        bronze
        | where device == 'blank-watch' and category == 'temperature'
        | project
            utctimestamp = timestamp,
            location,
            production_line = productionLine,
            lineupl = lineUpl,
            cycle = tostring(data.cycle),
            section = tostring(data.section),
            gob = tostring(data.gob),
            temp_timestamps = parse_json(data.temp_timestamps),
            temperatures = parse_json(data.temperatures)
        | mv-expand measurement_name = bag_keys(temperatures)
        | extend
            temperature = todouble(temperatures[tostring(measurement_name)]),
            utctimestamp_measurement = todatetime(temp_timestamps[tostring(measurement_name)])
        | project-away temp_timestamps, temperatures
    }

    // Drop and recreate Silver_BlankWatchTemperature table.
    .drop table Silver_BlankWatchTemperature ifexists
    .create table Silver_BlankWatchTemperature (
        utctimestamp: datetime,
        location: string,
        production_line: string,
        lineupl: string,
        cycle: string,
        section: string,
        gob: string,
        measurement_name: dynamic,
        temperature: real,
        utctimestamp_measurement: datetime
    )

    // Attach update policy to Silver_BlankWatchTemperature.
    .alter table Silver_BlankWatchTemperature policy update
    @'[{"Source": "bronze", "Query": "BronzeToBlankWatchTemperature()", "IsEnabled": true}]'

    // Define function for blank watch gob loading measurements.
    .create-or-alter function with (
        docstring = "Transforms bronze table data for blank watch gob loading into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToBlankWatchGobLoading() {
        bronze
        | where device == "blank-watch" and category == "gob-loading"
        | project
            utctimestamp=timestamp,
            location,
            production_line=productionLine,
            lineupl=lineUpl,
            cycle=tostring(data.cycle),
            section=tostring(data.section),
            gob=tostring(data.gob),
            position=tostring(data.position),
            data=bag_remove_keys(data, dynamic(["position", "cycle", "section", "gob"]))
        | mv-expand kind=array data
        | extend
            measurement_name=tostring(data[0]),
            value=toreal(data[1])
        | project-away data
    }

    // Create Silver_BlankWatchGobLoading table.
    .create table Silver_BlankWatchGobLoading (
        utctimestamp: datetime,
        location: string,
        production_line: string,
        lineupl: string,
        cycle: string,
        section: string,
        gob: string,
        position: string,
        measurement_name: string,
        value: real
    )

    // Attach update policy to Silver_BlankWatchGobLoading.
    .alter table Silver_BlankWatchGobLoading policy update
    @'[{"Source": "bronze", "Query": "BronzeToBlankWatchGobLoading()", "IsEnabled": true}]'
```

### 2. Verify the setup with diagnostic queries.
```kql
// Run the queries to validate the bronze, silver tables and their update policies.

.show tables
| where TableName in (
    'bronze',
    'Silver_IRIMeasurements',
    'Silver_IRIDefects',
    'Silver_TESMeasurements',
    'Silver_BlankWatchTemperature',
    'Silver_BlankWatchGobLoading'
)

// Sample silver table data.
bronze
| take 10

// Verify update policies for all tables.
.execute database script <|
    .show table bronze policy update
    .show table Silver_IRIMeasurements policy update
    .show table Silver_IRIDefects policy update
    .show table Silver_TESMeasurements policy update
    .show table Silver_BlankWatchTemperature policy update
    .show table Silver_BlankWatchGobLoading policy update

// List transformation functions.
.show functions
| where Folder == 'UpdatePolicies'

// Sample and count data in target tables to verify transformation.
Silver_IRIMeasurements
| take 100

Silver_IRIMeasurements
| where ingestion_time() >= ago(5s)
| take 10

Silver_IRIDefects
| count

Silver_IRIDefects
| where ingestion_time() >= ago(5s)
| take 10

Silver_TESMeasurements
| count

Silver_TESMeasurements
| where ingestion_time() >= ago(5s)
| take 10

Silver_BlankWatchTemperature
| count

Silver_BlankWatchTemperature
| where ingestion_time() >= ago(5s)
| take 10

Silver_BlankWatchGobLoading
| count

Silver_BlankWatchGobLoading
| where ingestion_time() >= ago(5s)
| take 10
