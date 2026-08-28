<#
.SYNOPSIS
Installs the Ptah command-line tools on Windows.

.DESCRIPTION
Detects the processor architecture, resolves a release tag, downloads the
matching .zip from the Ptah releases, verifies it against the published
checksums.txt, extracts it, and copies the binaries into a per-user directory.

    irm https://stokaro.github.io/ptah/install.ps1 | iex

`iex` evaluates the script text with no arguments, so the parameters below are
reachable only through the script-block form:

    &([scriptblock]::Create((irm https://stokaro.github.io/ptah/install.ps1))) -Version v0.2.0

Every parameter therefore has an environment twin, and the twin is the form to
use in the piped command:

    $env:PTAH_INSTALL_VERSION = 'v0.2.0'; irm https://stokaro.github.io/ptah/install.ps1 | iex

.PARAMETER Version
Release tag to install, with or without the leading `v`. Default: the newest
release. Environment: PTAH_INSTALL_VERSION.

.PARAMETER BinDir
Directory the binaries are copied into. Default: $env:LOCALAPPDATA\Ptah\bin.
Environment: PTAH_INSTALL_DIR.

.PARAMETER Only
Comma-separated subset of ptah,ptah-compat,ptah-ls. Environment:
PTAH_INSTALL_BINARIES.

.PARAMETER NoModifyPath
Leave the user PATH and $env:GITHUB_PATH alone. Environment:
PTAH_INSTALL_NO_MODIFY_PATH.

.PARAMETER VerifySignature
Also verify checksums.txt with cosign. Fails when cosign is absent rather than
skipping. Environment: PTAH_INSTALL_VERIFY_SIGNATURE.

.PARAMETER DryRun
Print the plan, download nothing, write nothing. Environment:
PTAH_INSTALL_DRY_RUN.

.PARAMETER Quiet
Errors only. Environment: PTAH_INSTALL_QUIET.

.PARAMETER Help
Print usage and return.

.NOTES
A failure sets $LASTEXITCODE and raises a terminating error. It does not call
`exit`: measured on Windows PowerShell 5.1, `exit` inside an `iex` block ends
the whole session, so a failed install would close the console the one-liner
was typed into. A terminating error leaves that console standing, and
`powershell -Command "irm ... | iex"` still exits non-zero, so a build step
still fails.

This file is ASCII only, and a gate keeps it that way. Windows PowerShell 5.1
reads a .ps1 with no byte-order mark in the machine's ANSI code page, so a
UTF-8 em dash arrives as three Windows-1252 characters, the last of which is a
right double quotation mark. PowerShell accepts that character as a string
delimiter, so one dash inside one message closed the string it sat in and the
whole file failed to parse.

$LASTEXITCODE values:

    0  success
    1  unexpected failure
    2  usage or configuration error
    3  unsupported platform
    4  missing prerequisite
    5  download failure
    6  integrity failure
    7  install-directory failure
#>

param(
    [string]$Version,
    [string]$BinDir,
    [string]$Only,
    [switch]$NoModifyPath,
    [switch]$VerifySignature,
    [switch]$DryRun,
    [switch]$Quiet,
    [switch]$Help
)

$ErrorActionPreference = 'Stop'

# Invoke-WebRequest renders a progress bar per read on Windows PowerShell 5.1,
# which costs more than the transfer itself on a 28 MB archive.
$ProgressPreference = 'SilentlyContinue'

# $PSBoundParameters is function-scoped, and every parameter above is read from
# inside a function, so the script's own copy is captured here. It is empty
# under `irm | iex`, which is the point: the environment twins then decide.
$PtahArguments = $PSBoundParameters

$script:PtahExitCode = $null
$script:PtahQuiet = $false

$PtahKnownBinaries = @('ptah', 'ptah-compat', 'ptah-ls')
$PtahDefaultBaseUrl = 'https://github.com/stokaro/ptah/releases'

function Stop-Ptah {
    param(
        [int]$Code,
        [string]$Message
    )

    $script:PtahExitCode = $Code
    $global:LASTEXITCODE = $Code
    throw "ptah: $Message"
}

function Write-PtahNote {
    param([string]$Message)

    if (-not $script:PtahQuiet) {
        Write-Output "ptah: $Message"
    }
}

# Resolve-PtahBoolean reads one PTAH_* boolean the way internal/envbool reads
# the ones the Go binaries own: absence selects the default, and a present value
# that is not a boolean is a configuration error rather than a default.
#
# The Unix installer additionally treats an exported empty value as the
# configuration error it is. Windows cannot: assigning the empty string to an
# environment variable deletes it, so `PTAH_INSTALL_QUIET=` and an unset
# PTAH_INSTALL_QUIET reach this function as the same thing. Measured, not
# assumed -- `docker run -e PTAH_PROBE=` leaves `Test-Path Env:PTAH_PROBE`
# answering False.
function Resolve-PtahBoolean {
    param(
        [string]$Name,
        [string]$Parameter,
        [bool]$Default
    )

    if ($PtahArguments.ContainsKey($Parameter)) {
        return [bool]$PtahArguments[$Parameter]
    }

    $item = Get-Item -LiteralPath "Env:$Name" -ErrorAction SilentlyContinue
    if ($null -eq $item) {
        return $Default
    }

    switch -Regex ($item.Value) {
        '^(1|true|yes)$' { return $true }
        '^(0|false|no)$' { return $false }
    }

    Stop-Ptah 2 "invalid boolean value `"$($item.Value)`" for $Name"
}

function Resolve-PtahString {
    param(
        [string]$Parameter,
        [string]$Name,
        [string]$Default
    )

    if (-not [string]::IsNullOrEmpty($Parameter)) {
        return $Parameter
    }

    $item = Get-Item -LiteralPath "Env:$Name" -ErrorAction SilentlyContinue
    if ($null -eq $item) {
        return $Default
    }
    # Unreachable on Windows for the reason Resolve-PtahBoolean states, and kept
    # for the PowerShell builds that do keep an empty variable rather than
    # deleting it.
    if ([string]::IsNullOrEmpty($item.Value)) {
        Stop-Ptah 2 "$Name is set to an empty value; unset it to take the default"
    }
    return $item.Value
}

# Resolve-PtahArchitecture refuses what it does not recognize instead of
# guessing. PROCESSOR_ARCHITEW6432 is read first because a 32-bit PowerShell on
# 64-bit Windows reports x86 in PROCESSOR_ARCHITECTURE and the real answer in
# the W6432 twin.
function Resolve-PtahArchitecture {
    $raw = $env:PROCESSOR_ARCHITEW6432
    if ([string]::IsNullOrEmpty($raw)) {
        $raw = $env:PROCESSOR_ARCHITECTURE
    }

    switch ($raw) {
        'AMD64' { return 'amd64' }
        'ARM64' { return 'arm64' }
    }

    Stop-Ptah 3 ("no Ptah release for windows/$raw " +
        "(PROCESSOR_ARCHITECTURE = $($env:PROCESSOR_ARCHITECTURE), " +
        "PROCESSOR_ARCHITEW6432 = $($env:PROCESSOR_ARCHITEW6432)); " +
        "see $PtahDefaultBaseUrl")
}

function Resolve-PtahVersion {
    param(
        [string]$Requested,
        [string]$BaseUrl
    )

    if ($Requested -ne 'latest') {
        $tag = $Requested
        if ($tag -notlike 'v*') {
            $tag = "v$tag"
        }
        if ($tag -notmatch '^v[0-9][0-9A-Za-z.+-]*$') {
            Stop-Ptah 2 "`"$Requested`" is not a release version; write it as v0.2.0 or 0.2.0"
        }
        return $tag
    }

    # The releases page answers JSON when asked for it, and unlike the REST API
    # it advertises no rate limit.
    $url = "$BaseUrl/latest"
    $tag = ''
    try {
        $release = Invoke-RestMethod -Uri $url -Headers @{ Accept = 'application/json' } -UseBasicParsing
        $tag = [string]$release.tag_name
    } catch {
        Stop-Ptah 5 "could not determine the latest Ptah release from $url"
    }

    if ([string]::IsNullOrEmpty($tag)) {
        Stop-Ptah 5 "could not determine the latest Ptah release from $url"
    }
    return $tag
}

# Test-PtahDirectoryWritable answers by writing, because a permission model
# nobody can read off the access-control list still refuses the copy. Creating
# the directory is part of the question: the default one does not exist on a
# machine that has never installed Ptah.
function Test-PtahDirectoryWritable {
    param([string]$Path)

    try {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
        $probe = Join-Path $Path ('.ptah-install-probe-' + [guid]::NewGuid().ToString('N'))
        [System.IO.File]::WriteAllText($probe, '')
        Remove-Item -LiteralPath $probe -Force
    } catch {
        return $false
    }
    return $true
}

function Get-PtahAsset {
    param(
        [string]$Url,
        [string]$OutFile
    )

    try {
        Invoke-WebRequest -Uri $Url -OutFile $OutFile -UseBasicParsing
    } catch {
        $status = 0
        $response = $_.Exception.Response
        if ($null -ne $response) {
            $status = [int]$response.StatusCode
        }
        if ($status -eq 404) {
            Stop-Ptah 5 "no release asset $(Split-Path -Leaf $Url); check the version"
        }
        Stop-Ptah 5 "failed to download $Url"
    }
}

# Get-PtahExpectedHash extracts the one checksums.txt line for this archive.
#
# The refusal on an empty result is the load-bearing line. Get-FileHash reports
# a hash and compares nothing, so an archive with no entry would otherwise be
# compared against the empty string and reported as a mismatch, which sends the
# reader looking for a corrupt download rather than a renamed asset.
function Get-PtahExpectedHash {
    param(
        [string]$ChecksumsPath,
        [string]$AssetName
    )

    $found = @()
    foreach ($line in [System.IO.File]::ReadAllLines($ChecksumsPath)) {
        $fields = $line -split '\s+', 2
        if ($fields.Count -ne 2) {
            continue
        }
        if ($fields[1].Trim() -ceq $AssetName) {
            $found += $fields[0].Trim()
        }
    }

    if ($found.Count -eq 0) {
        Stop-Ptah 6 "checksums.txt has no entry for $AssetName"
    }
    if ($found.Count -gt 1) {
        Stop-Ptah 6 "checksums.txt has $($found.Count) entries for $AssetName"
    }
    return $found[0]
}

function Assert-PtahSignature {
    param(
        [string]$BaseUrl,
        [string]$Tag,
        [string]$WorkDir
    )

    if ($null -eq (Get-Command cosign -ErrorAction SilentlyContinue)) {
        Stop-Ptah 4 'need cosign on PATH to verify the signature on checksums.txt'
    }

    $bundle = Join-Path $WorkDir 'checksums.txt.sigstore.json'
    Get-PtahAsset "$BaseUrl/download/$Tag/checksums.txt.sigstore.json" $bundle

    # $ErrorActionPreference governs cmdlet errors and says nothing about a
    # native program's exit status, so the status is read here rather than
    # assumed.
    & cosign verify-blob --bundle $bundle (Join-Path $WorkDir 'checksums.txt') `
        --certificate-identity-regexp 'github.com/stokaro/ptah' `
        --certificate-oidc-issuer 'https://token.actions.githubusercontent.com'
    if ($LASTEXITCODE -ne 0) {
        Stop-Ptah 6 'cosign refused the signature on checksums.txt'
    }
    Write-PtahNote 'signature on checksums.txt verified with cosign'
}

# Test-PtahOnPath compares a PATH value entry by entry. A substring test would
# call C:\Tools\ptah present because C:\Tools\ptah-old is.
function Test-PtahOnPath {
    param(
        [string]$PathValue,
        [string]$Directory
    )

    if ([string]::IsNullOrEmpty($PathValue)) {
        return $false
    }
    $wanted = $Directory.TrimEnd('\')
    foreach ($entry in $PathValue.Split(';')) {
        if ($entry.Trim().TrimEnd('\') -ieq $wanted) {
            return $true
        }
    }
    return $false
}

function Write-PtahHelp {
    $text = @'
ptah installer

    irm https://stokaro.github.io/ptah/install.ps1 | iex

Options, reachable through the script-block form:

    &([scriptblock]::Create((irm https://stokaro.github.io/ptah/install.ps1))) -Version v0.2.0

  -Version <tag>      release to install, v0.2.0 or 0.2.0   [env: PTAH_INSTALL_VERSION]
  -BinDir <dir>       where the binaries go                 [env: PTAH_INSTALL_DIR]
  -Only <list>        subset of ptah,ptah-compat,ptah-ls    [env: PTAH_INSTALL_BINARIES]
  -NoModifyPath       leave the user PATH alone             [env: PTAH_INSTALL_NO_MODIFY_PATH]
  -VerifySignature    also verify checksums.txt with cosign [env: PTAH_INSTALL_VERIFY_SIGNATURE]
  -DryRun             print the plan and stop               [env: PTAH_INSTALL_DRY_RUN]
  -Quiet              errors only                           [env: PTAH_INSTALL_QUIET]
  -Help               this text

                      release or mirror root                [env: PTAH_INSTALL_BASE_URL]

$LASTEXITCODE: 0 success, 1 unexpected, 2 usage, 3 unsupported platform,
4 missing prerequisite, 5 download failure, 6 integrity failure,
7 install-directory failure.
'@
    Write-Output $text
}

# Set-PtahPath makes the install directory reachable and says what it did.
#
# The Unix installer edits no startup file and this one still writes the
# user-scope PATH, because the two platforms are not in the same position.
# $HOME/.local/bin is a cross-desktop convention that Debian's and Ubuntu's own
# /etc/skel/.profile already puts on PATH; %LOCALAPPDATA%\Ptah\bin is on no
# Windows PATH by default and there is no file a reader could have edited in
# advance. The Windows value is also one setting with an official editor and an
# official way to undo it, rather than a line appended to each of several shell
# startup files. -NoModifyPath declines it.
function Set-PtahPath {
    param(
        [string]$Directory,
        [bool]$NoModify
    )

    # The current session first, so the binaries answer in the window the
    # installer ran in. This value dies with the session.
    if (-not (Test-PtahOnPath $env:Path $Directory)) {
        $env:Path = "$Directory;$env:Path"
    }

    if ($NoModify) {
        Write-PtahNote "$Directory is on PATH for this session only; -NoModifyPath declined the persistent change"
        return
    }

    # A workflow step gets the directory for the steps that follow it, which is
    # what actions/setup-* does and what a `run: irm ... | iex` step needs to be
    # followed by a step that can call ptah.
    if (-not [string]::IsNullOrEmpty($env:GITHUB_PATH)) {
        [System.IO.File]::AppendAllText(
            $env:GITHUB_PATH,
            "$Directory$([Environment]::NewLine)",
            (New-Object System.Text.UTF8Encoding $false))
        Write-PtahNote "added $Directory to `$GITHUB_PATH"
        return
    }

    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    if (Test-PtahOnPath $userPath $Directory) {
        Write-PtahNote "$Directory is already on your user PATH"
        return
    }

    $updated = $Directory
    if (-not [string]::IsNullOrEmpty($userPath)) {
        $updated = "$Directory;$userPath"
    }
    [Environment]::SetEnvironmentVariable('Path', $updated, 'User')
    Write-PtahNote "added $Directory to your user PATH; terminals already open keep the old value"
}

function Install-Ptah {
    # Every value is resolved before anything else happens, help included, so a
    # malformed variable fails on the invocation that carries it rather than on
    # the first run that reaches the branch it governs.
    $script:PtahQuiet = Resolve-PtahBoolean 'PTAH_INSTALL_QUIET' 'Quiet' $false
    $skipPath = Resolve-PtahBoolean 'PTAH_INSTALL_NO_MODIFY_PATH' 'NoModifyPath' $false
    $checkSignature = Resolve-PtahBoolean 'PTAH_INSTALL_VERIFY_SIGNATURE' 'VerifySignature' $false
    $planOnly = Resolve-PtahBoolean 'PTAH_INSTALL_DRY_RUN' 'DryRun' $false

    $requested = Resolve-PtahString $Version 'PTAH_INSTALL_VERSION' 'latest'
    $baseUrl = (Resolve-PtahString '' 'PTAH_INSTALL_BASE_URL' $PtahDefaultBaseUrl).TrimEnd('/')

    $localAppData = $env:LOCALAPPDATA
    if ([string]::IsNullOrEmpty($localAppData)) {
        $localAppData = Join-Path $HOME 'AppData\Local'
    }
    $installDir = Resolve-PtahString $BinDir 'PTAH_INSTALL_DIR' (Join-Path (Join-Path $localAppData 'Ptah') 'bin')

    $onlyValue = Resolve-PtahString $Only 'PTAH_INSTALL_BINARIES' ($PtahKnownBinaries -join ',')
    $selected = @()
    foreach ($name in $onlyValue.Split(',')) {
        $trimmed = $name.Trim()
        if ($trimmed -eq '') {
            continue
        }
        if ($PtahKnownBinaries -notcontains $trimmed) {
            Stop-Ptah 2 "unknown binary `"$trimmed`"; choose from $($PtahKnownBinaries -join ', ')"
        }
        if ($selected -notcontains $trimmed) {
            $selected += $trimmed
        }
    }
    if ($selected.Count -eq 0) {
        Stop-Ptah 2 'no binaries selected; -Only takes a subset of ptah, ptah-compat, ptah-ls'
    }

    if ($Help) {
        Write-PtahHelp
        return
    }

    if ($PSVersionTable.PSVersion.Major -lt 5) {
        Stop-Ptah 4 "need PowerShell 5 or later; this is $($PSVersionTable.PSVersion)"
    }
    foreach ($cmdlet in @('Get-FileHash', 'Expand-Archive')) {
        if ($null -eq (Get-Command $cmdlet -ErrorAction SilentlyContinue)) {
            Stop-Ptah 4 "need the $cmdlet cmdlet to install Ptah"
        }
    }

    # Windows PowerShell 5.1 on an older machine negotiates TLS 1.0, which the
    # release host refuses. 0 is SystemDefault, which lets the operating system
    # choose and is already right on anything current; 3072 is Tls12. The
    # numbers rather than the enum members, because SystemDefault is absent
    # from the .NET Framework versions this branch exists for.
    $protocol = [int][Net.ServicePointManager]::SecurityProtocol
    if ($protocol -ne 0 -and ($protocol -band 3072) -eq 0) {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]($protocol -bor 3072)
    }

    $arch = Resolve-PtahArchitecture
    $tag = Resolve-PtahVersion $requested $baseUrl
    $number = $tag.TrimStart('v')
    $assetName = "ptah_${number}_windows_${arch}.zip"

    if ($requested -eq 'latest') {
        Write-PtahNote "resolved latest to $tag"
    }
    Write-PtahNote "platform windows/$arch"

    if ($planOnly) {
        Write-PtahNote "would download $assetName from $baseUrl/download/$tag/"
        Write-PtahNote 'would verify sha256 against checksums.txt'
        Write-PtahNote "would install $($selected -join ', ') in $installDir"
        return
    }

    # Before the download rather than after it: a 28 MB transfer that ends in
    # "cannot write there" spent the transfer to learn something the probe
    # answers in milliseconds.
    if (-not (Test-PtahDirectoryWritable $installDir)) {
        Stop-Ptah 7 "cannot write to $installDir; set PTAH_INSTALL_DIR to a directory you own"
    }

    $workDir = Join-Path ([System.IO.Path]::GetTempPath()) ('ptah-install-' + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $workDir -Force | Out-Null
    try {
        $archive = Join-Path $workDir $assetName
        $checksums = Join-Path $workDir 'checksums.txt'

        Write-PtahNote "downloading $assetName"
        Get-PtahAsset "$baseUrl/download/$tag/$assetName" $archive
        Get-PtahAsset "$baseUrl/download/$tag/checksums.txt" $checksums

        if ($checkSignature) {
            Assert-PtahSignature $baseUrl $tag $workDir
        }

        $expected = Get-PtahExpectedHash $checksums $assetName
        # Get-FileHash answers in upper case and checksums.txt is written in
        # lower case, so the comparison is case-insensitive on purpose.
        $actual = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash
        if ($expected -ine $actual) {
            Stop-Ptah 6 ("checksum mismatch for $assetName" + [Environment]::NewLine +
                "  want: $($expected.ToLower())" + [Environment]::NewLine +
                "  got:  $($actual.ToLower())")
        }
        Write-PtahNote 'sha256 verified against checksums.txt'

        $extracted = Join-Path $workDir 'extracted'
        Expand-Archive -LiteralPath $archive -DestinationPath $extracted -Force

        # The archive is flat: LICENSE, README.md and the three .exe files sit
        # at its root. Copying it whole into the bin directory would leave the
        # license and the readme there too, so the binaries are copied by name,
        # and that copy is the last write of the run.
        $replaced = @()
        foreach ($name in $selected) {
            $source = Join-Path $extracted "$name.exe"
            if (-not (Test-Path -LiteralPath $source)) {
                Stop-Ptah 1 "$assetName does not contain $name.exe"
            }
            $target = Join-Path $installDir "$name.exe"
            $existed = Test-Path -LiteralPath $target
            try {
                Copy-Item -LiteralPath $source -Destination $target -Force
            } catch {
                Stop-Ptah 7 "cannot write $target; close any program running it and try again"
            }
            if ($existed) {
                $replaced += $target
            }
        }

        foreach ($target in $replaced) {
            Write-PtahNote "replaced $target"
        }
        Write-PtahNote "installed $($selected -join ', ') in $installDir"
    } finally {
        Remove-Item -LiteralPath $workDir -Recurse -Force -ErrorAction SilentlyContinue
    }

    if ($selected -contains 'ptah') {
        # Running it is what turns "the file is there" into "it runs here".
        $reported = & (Join-Path $installDir 'ptah.exe') version
        if ($LASTEXITCODE -ne 0) {
            Stop-Ptah 1 "installed ptah.exe, and `"ptah version`" exited $LASTEXITCODE"
        }
        if (-not $script:PtahQuiet) {
            Write-Output ($reported | Select-Object -First 1)
        }
    }

    Set-PtahPath $installDir $skipPath
}

try {
    Install-Ptah
    $global:LASTEXITCODE = 0
} catch {
    if ($null -eq $script:PtahExitCode) {
        $global:LASTEXITCODE = 1
    }
    throw
}
