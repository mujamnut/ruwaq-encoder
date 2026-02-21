param(
  [Parameter(Mandatory = $true)]
  [string]$LogPath,

  [string]$ExportJsonPath = '',

  [string]$ExportMarkdownPath = '',

  [double]$GcSpikeTotalMsThreshold = 120,

  [double]$GcSpikePauseMsThreshold = 8,

  [int]$BufferingCorrelationWindowMs = 5000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $LogPath)) {
  throw "Log file not found: $LogPath"
}

$perfRegex = [regex]'\[PERF\](?:\[[^\]]+\])?\s+(?<event>[a-zA-Z0-9._-]+)(?<rest>.*)$'
$kvRegex = [regex]'(?<key>[a-zA-Z0-9._-]+)=(?<value>[^\s]+)'
$relativeDeltaRegex = [regex]'^\[\s*\+(?<delta>\d+)\s*ms\]'
$gcFreedRegex = [regex]'GC freed\s+(?<freed_kb>\d+)KB.*?(?<heap_used_mb>\d+)MB\/(?<heap_max_mb>\d+)MB,\s*paused\s*(?<pause1_ms>[0-9.]+)ms(?:,(?<pause2_ms>[0-9.]+)ms)?\s*total\s*(?<total_ms>[0-9.]+)ms'
$waitForGcRegex = [regex]'WaitForGcToComplete blocked .* for (?<blocked_ms>[0-9.]+)ms'

$metrics = @{}
$bufferingStartTimelineMs = New-Object System.Collections.Generic.List[double]
$gcSpikeTimelineMs = New-Object System.Collections.Generic.List[double]
$currentTimelineMs = 0.0

function Add-Sample {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Metric,
    [Parameter(Mandatory = $true)]
    [double]$Value
  )

  if (-not $metrics.ContainsKey($Metric)) {
    $metrics[$Metric] = New-Object System.Collections.Generic.List[double]
  }
  $metrics[$Metric].Add($Value)
}

function Try-ParseDouble {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Raw,
    [ref]$Value
  )

  return [double]::TryParse(
    $Raw,
    [System.Globalization.NumberStyles]::Float,
    [System.Globalization.CultureInfo]::InvariantCulture,
    $Value
  )
}

function Get-PercentileNearestRank {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Values,
    [Parameter(Mandatory = $true)]
    [double]$Percentile
  )

  $arrayValues = @($Values)
  if ($arrayValues.Length -eq 0) {
    return $null
  }

  $sorted = $arrayValues | Sort-Object
  $sortedCount = @($sorted).Length
  $rank = [Math]::Ceiling($Percentile * $sortedCount)
  if ($rank -lt 1) {
    $rank = 1
  }

  $index = [int]$rank - 1
  return [double]$sorted[$index]
}

Get-Content -LiteralPath $LogPath | ForEach-Object {
  $line = $_

  $deltaMatch = $relativeDeltaRegex.Match($line)
  if ($deltaMatch.Success) {
    [double]$deltaMs = 0
    if (Try-ParseDouble -Raw $deltaMatch.Groups['delta'].Value -Value ([ref]$deltaMs)) {
      $currentTimelineMs += $deltaMs
    }
  }

  $gcFreedMatch = $gcFreedRegex.Match($line)
  if ($gcFreedMatch.Success) {
    [double]$freedKb = 0
    [double]$heapUsedMb = 0
    [double]$heapMaxMb = 0
    [double]$totalMs = 0

    if (Try-ParseDouble -Raw $gcFreedMatch.Groups['freed_kb'].Value -Value ([ref]$freedKb)) {
      Add-Sample -Metric 'gc.freed_kb' -Value $freedKb
    }
    if (Try-ParseDouble -Raw $gcFreedMatch.Groups['heap_used_mb'].Value -Value ([ref]$heapUsedMb)) {
      Add-Sample -Metric 'gc.heap_used_mb' -Value $heapUsedMb
    }
    if (Try-ParseDouble -Raw $gcFreedMatch.Groups['heap_max_mb'].Value -Value ([ref]$heapMaxMb)) {
      Add-Sample -Metric 'gc.heap_max_mb' -Value $heapMaxMb
    }
    if (Try-ParseDouble -Raw $gcFreedMatch.Groups['total_ms'].Value -Value ([ref]$totalMs)) {
      Add-Sample -Metric 'gc.total_ms' -Value $totalMs
    }

    $gcPrefix = 'gc.other'
    if ($line -match '\bBackground\b') {
      $gcPrefix = 'gc.background'
    } elseif ($line -match '\bForeground\b') {
      $gcPrefix = 'gc.foreground'
    }
    if ($line -match '\byoung\b') {
      $gcPrefix = "$gcPrefix.young"
    }

    if ($freedKb -gt 0) {
      Add-Sample -Metric "$gcPrefix.freed_kb" -Value $freedKb
    }
    if ($totalMs -gt 0) {
      Add-Sample -Metric "$gcPrefix.total_ms" -Value $totalMs
    }

    $pauseMaxMs = 0.0
    $pauseSumMs = 0.0
    $pauseCount = 0
    foreach ($pauseGroupName in @('pause1_ms', 'pause2_ms')) {
      $pauseRaw = $gcFreedMatch.Groups[$pauseGroupName].Value
      if ($pauseRaw -eq '') {
        continue
      }

      [double]$parsedPauseMs = 0
      if (Try-ParseDouble -Raw $pauseRaw -Value ([ref]$parsedPauseMs)) {
        $pauseCount++
        $pauseSumMs += $parsedPauseMs
        if ($parsedPauseMs -gt $pauseMaxMs) {
          $pauseMaxMs = $parsedPauseMs
        }
      }
    }

    if ($pauseCount -gt 0) {
      Add-Sample -Metric 'gc.pause_max_ms' -Value $pauseMaxMs
      Add-Sample -Metric 'gc.pause_sum_ms' -Value $pauseSumMs
      Add-Sample -Metric "$gcPrefix.pause_max_ms" -Value $pauseMaxMs
      Add-Sample -Metric "$gcPrefix.pause_sum_ms" -Value $pauseSumMs
    }

    if ($totalMs -ge $GcSpikeTotalMsThreshold -or $pauseMaxMs -ge $GcSpikePauseMsThreshold) {
      $gcSpikeTimelineMs.Add($currentTimelineMs)
    }
  }

  $waitForGcMatch = $waitForGcRegex.Match($line)
  if ($waitForGcMatch.Success) {
    [double]$blockedMs = 0
    if (Try-ParseDouble -Raw $waitForGcMatch.Groups['blocked_ms'].Value -Value ([ref]$blockedMs)) {
      Add-Sample -Metric 'gc.wait_for_gc.blocked_ms' -Value $blockedMs
      if ($blockedMs -ge $GcSpikePauseMsThreshold) {
        $gcSpikeTimelineMs.Add($currentTimelineMs)
      }
    }
  } elseif ($line -match 'Waiting for a blocking GC') {
    Add-Sample -Metric 'gc.wait_for_gc.blocking_events' -Value 1
  }

  $match = $perfRegex.Match($line)
  if (-not $match.Success) {
    return
  }

  $event = $match.Groups['event'].Value
  $rest = $match.Groups['rest'].Value

  $fields = @{}
  foreach ($fieldMatch in $kvRegex.Matches($rest)) {
    $key = $fieldMatch.Groups['key'].Value
    $value = $fieldMatch.Groups['value'].Value
    $fields[$key] = $value
  }

  if ($event -like 'home.bootstrap.*') {
    foreach ($key in $fields.Keys) {
      [double]$parsedValue = 0
      if (Try-ParseDouble -Raw $fields[$key] -Value ([ref]$parsedValue)) {
        Add-Sample -Metric "$event.$key" -Value $parsedValue
      }
    }
  }

  if ($event -like 'feed.video.*') {
    foreach ($key in $fields.Keys) {
      $isTimingField = ($key -eq 'ms' -or $key.EndsWith('_ms'))
      if (-not $isTimingField -or $key -eq 'position_ms') {
        continue
      }

      [double]$parsedValue = 0
      if (Try-ParseDouble -Raw $fields[$key] -Value ([ref]$parsedValue)) {
        Add-Sample -Metric "$event.$key" -Value $parsedValue
      }
    }
  }

  if ($event -eq 'feed.video.startup') {
    $latencyRaw = ''
    if ($fields.ContainsKey('latency_ms')) {
      $latencyRaw = $fields['latency_ms']
    }

    [double]$latency = 0
    if (Try-ParseDouble -Raw $latencyRaw -Value ([ref]$latency)) {
      if ($fields.ContainsKey('preloaded')) {
        Add-Sample -Metric "feed.video.startup.latency_ms.preloaded.$($fields['preloaded'])" -Value $latency
      }
    }
  }

  if ($event -eq 'feed.video.buffering.start') {
    $bufferingStartTimelineMs.Add($currentTimelineMs)
  }
}

$bufferingStartEvents = $bufferingStartTimelineMs.Count
$gcSpikeEvents = $gcSpikeTimelineMs.Count
$bufferingWithRecentGcSpike = 0

if ($bufferingStartEvents -gt 0 -and $gcSpikeEvents -gt 0) {
  foreach ($bufferingMs in $bufferingStartTimelineMs) {
    $matched = $false
    foreach ($gcSpikeMs in $gcSpikeTimelineMs) {
      $deltaSinceGcMs = $bufferingMs - $gcSpikeMs
      if ($deltaSinceGcMs -ge 0 -and $deltaSinceGcMs -le $BufferingCorrelationWindowMs) {
        $matched = $true
        break
      }
    }

    if ($matched) {
      $bufferingWithRecentGcSpike++
    }
  }
}

$bufferingWithRecentGcSpikePct = if ($bufferingStartEvents -gt 0) {
  [math]::Round(($bufferingWithRecentGcSpike * 100.0) / $bufferingStartEvents, 2)
} else {
  0
}

Add-Sample -Metric 'gc.correlation.buffering_start_events' -Value $bufferingStartEvents
Add-Sample -Metric 'gc.correlation.gc_spike_events' -Value $gcSpikeEvents
Add-Sample -Metric 'gc.correlation.buffering_with_recent_gc_spike' -Value $bufferingWithRecentGcSpike
Add-Sample -Metric 'gc.correlation.buffering_with_recent_gc_spike_pct' -Value $bufferingWithRecentGcSpikePct
Add-Sample -Metric 'gc.correlation.window_ms' -Value $BufferingCorrelationWindowMs
Add-Sample -Metric 'gc.correlation.gc_spike_total_ms_threshold' -Value $GcSpikeTotalMsThreshold
Add-Sample -Metric 'gc.correlation.gc_spike_pause_ms_threshold' -Value $GcSpikePauseMsThreshold

$rows = @()
foreach ($metric in ($metrics.Keys | Sort-Object)) {
  $values = [double[]]$metrics[$metric]
  $min = ($values | Measure-Object -Minimum).Minimum
  $max = ($values | Measure-Object -Maximum).Maximum
  $avg = ($values | Measure-Object -Average).Average
  $p50 = Get-PercentileNearestRank -Values $values -Percentile 0.50
  $p95 = Get-PercentileNearestRank -Values $values -Percentile 0.95

  $rows += [pscustomobject]@{
    metric = $metric
    samples = $values.Count
    min = [math]::Round($min, 2)
    p50 = [math]::Round($p50, 2)
    p95 = [math]::Round($p95, 2)
    max = [math]::Round($max, 2)
    avg = [math]::Round($avg, 2)
  }
}

if ($ExportJsonPath -ne '') {
  $parent = Split-Path -Parent $ExportJsonPath
  if ($parent -ne '' -and -not (Test-Path -LiteralPath $parent)) {
    New-Item -ItemType Directory -Path $parent | Out-Null
  }
  $rows | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $ExportJsonPath -Encoding UTF8
}

if ($ExportMarkdownPath -ne '') {
  $parent = Split-Path -Parent $ExportMarkdownPath
  if ($parent -ne '' -and -not (Test-Path -LiteralPath $parent)) {
    New-Item -ItemType Directory -Path $parent | Out-Null
  }

  $markdown = @(
    '# Runtime Perf Summary'
    ''
    ('Source log: `' + $LogPath + '`')
    "Generated at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss K')"
    ''
    '| Metric | Samples | Min | p50 | p95 | Max | Avg |'
    '|---|---:|---:|---:|---:|---:|---:|'
  )

  foreach ($row in ($rows | Sort-Object metric)) {
    $markdown += "| $($row.metric) | $($row.samples) | $($row.min) | $($row.p50) | $($row.p95) | $($row.max) | $($row.avg) |"
  }

  $markdown += ''
  $markdown += '## GC Correlation Summary'
  $markdown += ''
  $markdown += '| Signal | Value |'
  $markdown += '|---|---:|'
  $markdown += "| Buffering start events | $bufferingStartEvents |"
  $markdown += "| GC spike events | $gcSpikeEvents |"
  $markdown += "| Buffering with recent GC spike | $bufferingWithRecentGcSpike |"
  $markdown += "| Buffering with recent GC spike % | $bufferingWithRecentGcSpikePct |"
  $markdown += "| Correlation window (ms) | $BufferingCorrelationWindowMs |"
  $markdown += "| GC spike total threshold (ms) | $GcSpikeTotalMsThreshold |"
  $markdown += "| GC spike pause threshold (ms) | $GcSpikePauseMsThreshold |"

  Set-Content -LiteralPath $ExportMarkdownPath -Value $markdown -Encoding UTF8
}

$rows | Sort-Object metric | Format-Table -AutoSize
