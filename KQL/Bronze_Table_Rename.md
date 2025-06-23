# 🤿 Setup Silver Table Transformations and Update Policies

## 🏅 Objectives
- Rename the `bronze` table to `silver` to align with data processing conventions.
- Define KQL functions to transform `silver` table data for various measurement and defect categories (IRI measurements, IRI defects, cold system measurements, blank watch temperature, blank watch gob loading).
- Attach update policies to target tables to automate real-time data transformation from `silver`.
- Verify table renaming, function creation, policy attachment, and data flow.

## 📃 Tasks
1. Execute Database Script for Table Rename and Update Policies.
2. Verify the setup with diagnostic queries.

## 🪜 Steps

### 1. Execute Database Script for Table Rename and Update Policies
```kql
// Renames bronze to silver, updates transformation functions, and attaches update policies.
// Ensures atomic execution to prevent dependency issues.
.execute database script <|
    // Rename bronze table to silver, preserving schema and data.
    .rename table bronze to silver;

    // Create/update function for IRI measurements, extracting details from data dynamic column.
    .create-or-alter function with (
        docstring = "Transforms silver table data for IRI measurements into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToIRIMeasurements() {
        silver
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

    // Attach update policy to Silver_IRIMeasurements for real-time transformation.
    .alter table Silver_IRIMeasurements policy update
    @'[{"Source": "silver", "Query": "BronzeToIRIMeasurements()", "IsEnabled": true}]';

    // Create/update function for IRI defects, extracting defect details.
    .create-or-alter function with (
        docstring = "Transforms silver table data for IRI defects into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToIRIDefects() {
        silver
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

    // Attach update policy to Silver_IRIDefects.
    .alter table Silver_IRIDefects policy update
    @'[{"Source": "silver", "Query": "BronzeToIRIDefects()", "IsEnabled": true}]';

    // Create/update function for cold system measurements.
    .create-or-alter function with (
        docstring = "Transforms silver table data for cold system measurements into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToColdsystemMx() {
        silver
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

    // Attach update policy to Silver_TESMeasurements.
    .alter table Silver_TESMeasurements policy update
    @'[{"Source": "silver", "Query": "BronzeToColdsystemMx()", "IsEnabled": true}]';

    // Create/update function for blank watch temperature measurements.
    .create-or-alter function with (
        docstring = "Transforms silver table data for blank watch temperature into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToBlankWatchTemperature() {
        silver
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

    // Attach update policy to Silver_BlankWatchTemperature.
    .alter table Silver_BlankWatchTemperature policy update
    @'[{"Source": "silver", "Query": "BronzeToBlankWatchTemperature()", "IsEnabled": true}]';

    // Create/update function for blank watch gob loading measurements.
    .create-or-alter function with (
        docstring = "Transforms silver table data for blank watch gob loading into a structured format",
        folder = "UpdatePolicies"
    )
    BronzeToBlankWatchGobLoading() {
        silver
        | where device == "blank-watch" and category == "gob-loading"
        | project
            utctimestamp=timestamp,
            location,
            production_line=productionLine,
            lineupl=lineUpl,
            cycle=tostring(data.cycle),
            section=tostring(data.section),
            gob=tostring(data.gob),
            position=tostring(data.user),
            data=bag_remove_keys(data, dynamic(["position", "cycle", "section", "user"]))
        | mv-expand kind=array data
        | extend
            measurement_name=tostring(data[0]),
            value=toreal(data[1])
        | project-away data
    }

    // Attach update policy to Silver_BlankWatchGobLoading.
    .alter table Silver_BlankWatchGobLoading policy update
    @'[{"Source": "silver", "Query": "BronzeToBlankWatchGobLoading()", "IsEnabled": true}]';
```

### 2. Verify the setup with diagnostic queries.
```kql
// Run the queries to validate the silver tables and their update policies.
// Ensures atomic execution to prevent dependency issues.
.show tables
| where TableName in (
    'silver',
    'Silver_IRIMeasurements',
    'Silver_IRIDefects',
    'Silver_TESMeasurements',
    'Silver_BlankWatchTemperature',
    'Silver_BlankWatchGobLoading'
)

// Sample silver table data.
silver
| take 10

// Verify update policies for all tables.
.execute database script <|
    .show table silver policy update
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
