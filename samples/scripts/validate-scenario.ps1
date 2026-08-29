param(
    [Parameter(Mandatory = $true)]
    [string]$ScenarioPath,

    [string]$EventCatalogPath = "samples/events/event-types.json"
)

$ErrorActionPreference = "Stop"

function Write-CheckResult {
    param(
        [string]$Check,
        [bool]$Passed,
        [string]$Details
    )

    $status = if ($Passed) { "PASS" } else { "FAIL" }

    [PSCustomObject]@{
        Status  = $status
        Check   = $Check
        Details = $Details
    }
}

if (-not (Test-Path $ScenarioPath)) {
    Write-Error "Scenario file not found: $ScenarioPath"
}

try {
    $scenario = Get-Content $ScenarioPath -Raw | ConvertFrom-Json
}
catch {
    Write-Error "Invalid JSON: $($_.Exception.Message)"
}





if (-not (Test-Path $EventCatalogPath)) {
    Write-Error "Event catalog not found: $EventCatalogPath"
}

try {
    $eventCatalog = Get-Content $EventCatalogPath -Raw | ConvertFrom-Json
}
catch {
    Write-Error "Invalid event catalog JSON: $($_.Exception.Message)"
}




$results = @()
$events = @($scenario.events)

$results += Write-CheckResult `
    -Check "Scenario contains events" `
    -Passed ($events.Count -gt 0) `
    -Details "Event count: $($events.Count)"

if ($events.Count -eq 0) {
    $results | Format-Table -AutoSize
    exit 1
}

$invalidAggregateEvents = @(
    $events | Where-Object {
        $_.aggregateId -ne $scenario.aggregateId
    }
)

$results += Write-CheckResult `
    -Check "Aggregate identifiers" `
    -Passed ($invalidAggregateEvents.Count -eq 0) `
    -Details "Invalid events: $($invalidAggregateEvents.Count)"

$versionsAreValid = $true
$expectedVersion = 1

foreach ($event in $events) {
    if ([int]$event.aggregateVersion -ne $expectedVersion) {
        $versionsAreValid = $false
        break
    }

    $expectedVersion++
}

$results += Write-CheckResult `
    -Check "Aggregate versions" `
    -Passed $versionsAreValid `
    -Details "Expected sequence: 1..$($events.Count)"

$duplicateEventIds = @(
    $events |
        Group-Object eventId |
        Where-Object Count -gt 1
)

$results += Write-CheckResult `
    -Check "Unique event identifiers" `
    -Passed ($duplicateEventIds.Count -eq 0) `
    -Details "Duplicate identifiers: $($duplicateEventIds.Count)"









$unknownEventTypes = @()
$invalidProducerTypes = @()
$missingPayloadFields = @()

foreach ($event in $events) {
    $eventDefinitionProperty = $eventCatalog.eventTypes.PSObject.Properties |
        Where-Object Name -eq $event.eventType |
        Select-Object -First 1

    if ($null -eq $eventDefinitionProperty) {
        $unknownEventTypes += $event.eventType
        continue
    }

    $eventDefinition = $eventDefinitionProperty.Value

    if ($event.producerType -ne $eventDefinition.producerType) {
        $invalidProducerTypes += "$($event.eventType):$($event.producerType)"
    }

    foreach ($requiredField in @(
        $eventDefinition.requiredPayloadFields
    )) {
        $payloadProperty = $event.payload.PSObject.Properties |
            Where-Object Name -eq $requiredField |
            Select-Object -First 1

        if ($null -eq $payloadProperty) {
            $missingPayloadFields += "$($event.eventType):$requiredField"
        }
    }
}

$results += Write-CheckResult `
    -Check "Known event types" `
    -Passed ($unknownEventTypes.Count -eq 0) `
    -Details "Unknown types: $($unknownEventTypes.Count)"

$results += Write-CheckResult `
    -Check "Producer types" `
    -Passed ($invalidProducerTypes.Count -eq 0) `
    -Details "Invalid producers: $($invalidProducerTypes.Count)"

$results += Write-CheckResult `
    -Check "Required payload fields" `
    -Passed ($missingPayloadFields.Count -eq 0) `
    -Details "Missing fields: $($missingPayloadFields.Count)"











$timestampsAreOrdered = $true
$previousTimestamp = $null

foreach ($event in $events) {
    try {
        $currentTimestamp = [datetime]::Parse($event.occurredAt)
    }
    catch {
        $timestampsAreOrdered = $false
        break
    }

    if (
        $null -ne $previousTimestamp -and
        $currentTimestamp -lt $previousTimestamp
    ) {
        $timestampsAreOrdered = $false
        break
    }

    $previousTimestamp = $currentTimestamp
}

$results += Write-CheckResult `
    -Check "Chronological timestamps" `
    -Passed $timestampsAreOrdered `
    -Details "Timestamps must be ordered"

$correlationGroups = @(
    $events | Group-Object correlationId
)

$singleCorrelationId = (
    $correlationGroups.Count -eq 1 -and
    $correlationGroups[0].Name
)

$results += Write-CheckResult `
    -Check "Correlation identifier" `
    -Passed $singleCorrelationId `
    -Details "Correlation groups: $($correlationGroups.Count)"

$firstEventIsCreation = (
    $events[0].eventType -eq "ORDER_CREATED"
)

$results += Write-CheckResult `
    -Check "First event" `
    -Passed $firstEventIsCreation `
    -Details "Actual: $($events[0].eventType)"

$expectedVersionMatches = (
    [int]$scenario.expectedFinalState.version -eq
    [int]$events[-1].aggregateVersion
)

$results += Write-CheckResult `
    -Check "Expected final version" `
    -Passed $expectedVersionMatches `
    -Details "Expected: $($scenario.expectedFinalState.version), event: $($events[-1].aggregateVersion)"

$totalDelay = 0

foreach ($delayEvent in @(
    $events | Where-Object eventType -eq "DELIVERY_DELAYED"
)) {
    $totalDelay += [int]$delayEvent.payload.delayMinutes
}

$expectedDelay = [int]$scenario.expectedFinalState.totalDelayMinutes
$delayMatches = $totalDelay -eq $expectedDelay

$results += Write-CheckResult `
    -Check "Accumulated delay" `
    -Passed $delayMatches `
    -Details "Calculated: $totalDelay, expected: $expectedDelay"

$createdAt = [datetime]::Parse($events[0].occurredAt)

$lastEventAt = [datetime]::Parse($events[-1].occurredAt)

$durationHours = ($lastEventAt - $createdAt).TotalHours

$producerSummary = $events |
    Group-Object producerId |
    Sort-Object Name |
    Select-Object @{
        Name = "Producer"
        Expression = { $_.Name }
    }, Count

Write-Host ""
Write-Host "Scenario validation: $($scenario.scenarioId)"
Write-Host "File: $ScenarioPath"
Write-Host ""

$results | Format-Table -AutoSize

Write-Host ""
Write-Host "Scenario summary"
Write-Host "Aggregate:       $($scenario.aggregateId)"
Write-Host "Events:          $($events.Count)"
Write-Host "Duration hours:  $durationHours"
Write-Host "Total delay:     $totalDelay minutes"
Write-Host "Final status:    $($scenario.expectedFinalState.status)"
Write-Host ""
Write-Host "Distributed producers"
$producerSummary | Format-Table -AutoSize

$failedChecks = @(
    $results | Where-Object Status -eq "FAIL"
)

if ($failedChecks.Count -gt 0) {
    Write-Host "Validation failed: $($failedChecks.Count) check(s)."
    exit 1
}

Write-Host "Validation completed successfully."
exit 0