param(
    [ValidateSet('up', 'approval-digest', 'rows', 'pointer', 'cleanup', 'all')]
    [string] $Command
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($Command)) {
    Write-Error 'usage: .\run.ps1 {up|approval-digest|rows|pointer|cleanup|all}'
    exit 2
}

$FixtureDir = $PSScriptRoot
$RuntimeDir = Join-Path $FixtureDir '.ptah-inference'
$RuntimeSpec = Join-Path $RuntimeDir 'spec.yaml'
$DockerContext = if ($env:PTAH_DOCKER_CONTEXT) { $env:PTAH_DOCKER_CONTEXT } else { 'default' }
$FixtureHost = if ($env:PTAH_FIXTURE_HOST) { $env:PTAH_FIXTURE_HOST } else { '127.0.0.1' }
# Empty unless pinned, which publishes to a host port Docker chooses. The two
# variables stay for a caller that needs a fixed address. They defaulted to
# 55432 and 58080, both inside Linux's default ephemeral range, so a run could
# lose the port to one of its own outbound connections (stokaro/ptah#2673).
$PostgresPort = if ($env:PTAH_INFERENCE_POSTGRES_PORT) { $env:PTAH_INFERENCE_POSTGRES_PORT } else { '' }
$EmbedPort = if ($env:PTAH_INFERENCE_EMBED_PORT) { $env:PTAH_INFERENCE_EMBED_PORT } else { '' }
$Project = if ($env:PTAH_INFERENCE_PROJECT) { $env:PTAH_INFERENCE_PROJECT } else { 'ptah-inference-quick-start' }
$PtahBin = if ($env:PTAH_BIN) { $env:PTAH_BIN } else { 'ptah' }
$DatabaseUrl = if ($env:PTAH_DB_URL) {
    $env:PTAH_DB_URL
} else {
    "postgres://ptah:ptah@${FixtureHost}:${PostgresPort}/ptah?sslmode=disable"
}
$RunId = if ($env:PTAH_RUN_ID) { $env:PTAH_RUN_ID } else { 'quick-start' }

function Invoke-Compose {
    $env:PTAH_INFERENCE_POSTGRES_PORT = $PostgresPort
    $env:PTAH_INFERENCE_EMBED_PORT = $EmbedPort
    & docker --context $DockerContext compose -p $Project -f (Join-Path $FixtureDir 'compose.yaml') @args
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Ptah {
    & $PtahBin @args
    if ($LASTEXITCODE -ne 0) {
        throw "ptah failed with exit code $LASTEXITCODE"
    }
}

function Write-RuntimeSpec {
    New-Item -ItemType Directory -Path $RuntimeDir -Force | Out-Null
    $template = [IO.File]::ReadAllText((Join-Path $FixtureDir 'spec.yaml'))
    $placeholder = '__PTAH_INFERENCE_ENDPOINT__'
    if (($template.Split($placeholder).Count - 1) -ne 1) {
        throw 'spec.yaml must contain exactly one inference endpoint placeholder'
    }
    $endpoint = "http://${FixtureHost}:${EmbedPort}/v1"
    $encoding = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($RuntimeSpec, $template.Replace($placeholder, $endpoint), $encoding)
}

function Assert-Runtime {
    if (-not (Test-Path -LiteralPath $RuntimeSpec -PathType Leaf)) {
        throw 'run ".\run.ps1 up" before this command'
    }
}

# Get-PublishedPort asks Docker which host port a container port ended up on.
function Get-PublishedPort {
    param([string]$Service, [string]$ContainerPort)
    $mapping = Invoke-Compose @('port', $Service, $ContainerPort)
    return ($mapping -split ':')[-1].Trim()
}

function Start-Services {
    # Up first, then the specification: the ports are Docker's to choose, so
    # they are not knowable until the containers exist (stokaro/ptah#2673).
    Invoke-Compose @('up', '-d', '--build', '--wait')
    $script:PostgresPort = Get-PublishedPort 'postgres' '5432'
    $script:EmbedPort = Get-PublishedPort 'embeddings' '8080'
    if (-not $script:PostgresPort -or -not $script:EmbedPort) {
        Write-Error 'run.ps1: docker did not report the published ports'
        exit 1
    }
    $script:DatabaseUrl = "postgres://ptah:ptah@$FixtureHost`:$script:PostgresPort/ptah?sslmode=disable"
    Write-RuntimeSpec
    Write-Output "runtime specification: $RuntimeSpec"
    Write-Output "database URL: $script:DatabaseUrl"
    Write-Output "embeddings endpoint: http://$FixtureHost`:$script:EmbedPort/v1"
}

function Get-ApprovalDigest {
    Assert-Runtime
    $output = & $PtahBin inference cutover --spec $RuntimeSpec --db-url $DatabaseUrl --run-id $RunId 2>&1
    $status = $LASTEXITCODE
    if ($status -eq 0) {
        throw 'unapproved cutover unexpectedly succeeded'
    }
    foreach ($line in $output) {
        [Console]::Error.WriteLine($line.ToString())
    }
    $matches = @($output | ForEach-Object { [regex]::Match($_.ToString(), '^plan (.+)$') } | Where-Object { $_.Success })
    if ($matches.Count -ne 1) {
        throw "cutover refusal printed $($matches.Count) plan digests instead of one"
    }
    Write-Output $matches[0].Groups[1].Value
}

function Show-Rows {
    Invoke-Compose @('exec', '-T', 'postgres', 'psql', '-U', 'ptah', '-d', 'ptah', '-c',
        'SELECT id, embedding_generation, embedding_state FROM docs ORDER BY id;')
}

function Show-Pointer {
    Invoke-Compose @('exec', '-T', 'postgres', 'psql', '-U', 'ptah', '-d', 'ptah', '-c',
        'SELECT target_table, active_generation FROM ptah_embedding_pointer;')
}

function Remove-Fixture {
    Invoke-Compose @('down', '-v', '--rmi', 'local')
    Remove-Item -LiteralPath $RuntimeDir -Recurse -Force -ErrorAction SilentlyContinue
}

function Invoke-All {
    Start-Services
    Invoke-Ptah @('inference', 'plan', '--spec', $RuntimeSpec, '--db-url', $DatabaseUrl)
    Invoke-Ptah @('inference', 'prepare', '--spec', $RuntimeSpec, '--db-url', $DatabaseUrl, '--run-id', $RunId)
    Invoke-Ptah @('inference', 'backfill', '--spec', $RuntimeSpec, '--db-url', $DatabaseUrl,
        '--run-id', $RunId, '--batch-rows', '10')
    Invoke-Ptah @('inference', 'catchup', '--spec', $RuntimeSpec, '--db-url', $DatabaseUrl,
        '--run-id', $RunId, '--batch-rows', '10')
    Invoke-Ptah @('inference', 'index', '--spec', $RuntimeSpec, '--db-url', $DatabaseUrl, '--run-id', $RunId)
    Invoke-Ptah @('inference', 'verify', '--spec', $RuntimeSpec, '--db-url', $DatabaseUrl, '--run-id', $RunId)
    Invoke-Ptah @('inference', 'status', '--spec', $RuntimeSpec, '--db-url', $DatabaseUrl, '--run-id', $RunId)
    Show-Rows
    $digest = Get-ApprovalDigest
    Invoke-Ptah @('inference', 'cutover', '--spec', $RuntimeSpec, '--db-url', $DatabaseUrl,
        '--run-id', $RunId, '--approve', $digest, '--approver', 'quick-start helper')
    Show-Pointer
}

switch ($Command) {
    'up' { Start-Services }
    'approval-digest' { Get-ApprovalDigest }
    'rows' { Show-Rows }
    'pointer' { Show-Pointer }
    'cleanup' { Remove-Fixture }
    'all' {
        try {
            Invoke-All
        } finally {
            Remove-Fixture
        }
    }
}
