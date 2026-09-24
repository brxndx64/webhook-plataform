<#
.SYNOPSIS
    Roda o mesmo teste de carga contra as implementacoes Go e Python e
    gera um relatorio HTML comparativo.

.DESCRIPTION
    O que este script garante - e sem o que a comparacao nao valeria:

      * mesmo gerador de carga (cmd/loadgen) para os dois lados;
      * mesmo provedor simulado (cmd/provider), com a mesma latencia
        e a mesma taxa de erro;
      * mesmos parametros: workers, fila, retries, payload, duracao,
        concorrencia e aquecimento;
      * os dois medidos DE FORA (CPU e memoria do processo), na mesma
        unidade;
      * so um servidor no ar por vez, para um nao roubar CPU do outro.

.EXAMPLE
    .\compare.ps1
    .\compare.ps1 -Duration 60 -Concurrency 128 -Scenario carga-alta

.EXAMPLE
    # Cenario de malha aberta: taxa fixa, mede latencia sob carga controlada
    .\compare.ps1 -Rate 5000 -Scenario taxa-fixa-5k
#>
[CmdletBinding()]
param(
    [int]    $Duration          = 30,
    [int]    $Concurrency       = 64,
    # Taxa fixa por padrao (malha aberta). Com Rate=0 o gerador dispara
    # o mais rapido que consegue, a ingestao supera em muito a entrega
    # e a fila satura: o teste passa a medir a recusa por backpressure,
    # nao o pipeline. Isso e um cenario valido, mas nao o padrao util.
    [double] $Rate              = 3000,
    [int]    $Workers           = 256,
    [int]    $QueueSize         = 20000,
    [int]    $Retries           = 3,
    [int]    $Payload           = 256,
    [int]    $Warmup            = 5,
    [string] $ProviderLatency   = "5ms",
    [string] $ProviderJitter    = "2ms",
    [double] $ProviderErrorRate = 0,
    [string] $Scenario          = "",
    [ValidateSet("go", "python", "both")]
    [string] $Targets           = "both",
    [switch] $KeepResults,
    [switch] $NoReport
)

$ErrorActionPreference = "Stop"
$root      = Split-Path -Parent $PSScriptRoot
$goDir     = Join-Path $root "go"
$pyDir     = Join-Path $root "python"
$binDir    = Join-Path $PSScriptRoot "bin"
$resultDir = Join-Path $PSScriptRoot "results"

if ($Scenario -eq "") {
    if ($Rate -gt 0) { $Scenario = "taxa-$([int]$Rate)" } else { $Scenario = "vazao-maxima" }
}

$GO_PORT       = 8080
$PY_PORT       = 8081
$PROVIDER_PORT = 9000
$DEST          = "http://127.0.0.1:$PROVIDER_PORT/hook"

# ------------------------------------------------------------------ helpers

function Write-Step([string] $msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}

# Start-Process -ArgumentList junta os elementos com espaco e NAO cita
# nada. Um caminho como "...\Area de Trabalho\..." chegaria partido em
# tres argumentos. Citar aqui e obrigatorio.
function ConvertTo-CliArgs([string[]] $items) {
    $out = @()
    foreach ($i in $items) {
        if ($i -match '\s') { $out += '"' + $i + '"' } else { $out += $i }
    }
    return $out
}

function Stop-Port([int] $port) {
    $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conns) {
        Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Milliseconds 300
}

# Descobre QUEM realmente escuta a porta.
#
# Necessario porque o python.exe de um venv no Windows e um
# redirecionador: ele executa o interpretador real como processo FILHO.
# Amostrar o PID devolvido pelo Start-Process mediria o stub (0s de CPU,
# 4 MB de memoria) em vez do servidor.
function Get-ListenerPid([int] $port) {
    $c = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
         Select-Object -First 1
    if ($c) { return [int]$c.OwningProcess }
    return 0
}

function Wait-Healthy([int] $port, [int] $timeoutSec = 30) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -Uri "http://127.0.0.1:$port/health" -UseBasicParsing -TimeoutSec 2
            if ($r.StatusCode -eq 200) { return $true }
        } catch {
            Start-Sleep -Milliseconds 300
        }
    }
    return $false
}

# Amostra CPU e memoria do processo alvo enquanto o gerador roda.
# Medir de fora e o que coloca Go e Python na mesma unidade: o
# "heap" que cada runtime reporta internamente nao e comparavel.
function Invoke-LoadAndSample {
    param(
        [int] $TargetPid,
        [string] $Exe,
        [string[]] $ExeArgs
    )

    $proc = Get-Process -Id $TargetPid -ErrorAction Stop
    $cpuBefore = $proc.TotalProcessorTime.TotalSeconds

    $lg = Start-Process -FilePath $Exe -ArgumentList (ConvertTo-CliArgs $ExeArgs) -PassThru -NoNewWindow

    $peak = 0L
    $sum  = 0L
    $n    = 0
    while (-not $lg.HasExited) {
        try {
            $p = Get-Process -Id $TargetPid -ErrorAction Stop
            $ws = [int64]$p.WorkingSet64
            if ($ws -gt $peak) { $peak = $ws }
            $sum += $ws
            $n++
        } catch { }
        Start-Sleep -Milliseconds 250
    }
    $lg.WaitForExit()

    $cpuAfter = 0.0
    try {
        $p = Get-Process -Id $TargetPid -ErrorAction Stop
        $cpuAfter = $p.TotalProcessorTime.TotalSeconds
    } catch { $cpuAfter = $cpuBefore }

    $avg = 0L
    if ($n -gt 0) { $avg = [int64]($sum / $n) }

    return [pscustomobject]@{
        CpuSeconds = [math]::Round($cpuAfter - $cpuBefore, 3)
        PeakRSS    = $peak
        AvgRSS     = $avg
        ExitCode   = $lg.ExitCode
    }
}

# Enxerta as medidas externas dentro do JSON produzido pelo loadgen.
function Merge-ProcessStats([string] $jsonPath, [string] $name, $sample, [double] $seconds) {
    $data = Get-Content $jsonPath -Raw | ConvertFrom-Json
    $pct = 0.0
    if ($seconds -gt 0) { $pct = [math]::Round(100.0 * $sample.CpuSeconds / $seconds, 1) }

    $data | Add-Member -NotePropertyName process -NotePropertyValue ([pscustomobject]@{
        name                     = $name
        cpu_seconds              = $sample.CpuSeconds
        cpu_percent_of_one_core  = $pct
        peak_rss_bytes           = $sample.PeakRSS
        avg_rss_bytes            = $sample.AvgRSS
    }) -Force

    # Sem BOM: o Set-Content -Encoding utf8 do PowerShell 5.1 grava um
    # BOM que o parser JSON do Go recusa.
    $json = $data | ConvertTo-Json -Depth 12
    [System.IO.File]::WriteAllText($jsonPath, $json, [System.Text.UTF8Encoding]::new($false))
}

function Invoke-Target {
    param(
        [string] $Label,
        [int]    $Port,
        [string] $Exe,
        [string[]] $ExeArgs,
        [string] $WorkDir
    )

    Write-Step "$Label - subindo o servidor na porta $Port"
    Stop-Port $Port

    $stdout = Join-Path $resultDir "$Label.server.log"
    $stderr = Join-Path $resultDir "$Label.server.err.log"

    $srv = Start-Process -FilePath $Exe -ArgumentList (ConvertTo-CliArgs $ExeArgs) -PassThru `
        -WindowStyle Hidden -WorkingDirectory $WorkDir `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr

    try {
        if (-not (Wait-Healthy $Port)) {
            Write-Host "  falhou ao subir. Log:" -ForegroundColor Red
            Get-Content $stdout, $stderr -ErrorAction SilentlyContinue | Select-Object -First 25
            return $false
        }
        $realPid = Get-ListenerPid $Port
        if ($realPid -eq 0) { $realPid = $srv.Id }
        Write-Host "  no ar (pid $realPid)"

        $outFile = Join-Path $resultDir "$Scenario-$Label.json"
        $lgArgs = @(
            "-target",      "http://127.0.0.1:$Port",
            "-dest",        $DEST,
            "-duration",    "${Duration}s",
            "-warmup",      "${Warmup}s",
            "-concurrency", "$Concurrency",
            "-rate",        "$Rate",
            "-payload",     "$Payload",
            "-label",       $Label,
            "-scenario",    $Scenario,
            "-out",         $outFile
        )

        Write-Step "$Label - carga: ${Duration}s, concorrencia $Concurrency"
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $sample = Invoke-LoadAndSample -TargetPid $realPid `
            -Exe (Join-Path $binDir "loadgen.exe") -ExeArgs $lgArgs
        $sw.Stop()

        if (-not (Test-Path $outFile)) {
            Write-Host "  o gerador nao produziu resultado" -ForegroundColor Red
            return $false
        }

        Merge-ProcessStats $outFile $Label $sample $sw.Elapsed.TotalSeconds
        Write-Host ("  CPU {0}s  |  RSS pico {1:N1} MB" -f `
            $sample.CpuSeconds, ($sample.PeakRSS / 1MB)) -ForegroundColor Green
        return $true
    }
    finally {
        Stop-Process -Id $srv.Id -Force -ErrorAction SilentlyContinue
        Stop-Port $Port
        Start-Sleep -Milliseconds 500
    }
}

# --------------------------------------------------------------------- setup

Write-Host "Comparacao Go x Python - plataforma de webhooks" -ForegroundColor White
Write-Host "cenario=$Scenario duracao=${Duration}s concorrencia=$Concurrency workers=$Workers taxa=$Rate"

New-Item -ItemType Directory -Force $binDir    | Out-Null
New-Item -ItemType Directory -Force $resultDir | Out-Null

if (-not $KeepResults) {
    Get-ChildItem $resultDir -Filter "$Scenario-*.json" -ErrorAction SilentlyContinue | Remove-Item -Force
}

Write-Step "compilando os binarios Go"
Push-Location $goDir
try {
    foreach ($cmd in @("api", "provider", "loadgen", "report")) {
        go build -o (Join-Path $binDir "$cmd.exe") "./cmd/$cmd"
        if ($LASTEXITCODE -ne 0) { throw "falha ao compilar cmd/$cmd" }
    }
} finally { Pop-Location }
Write-Host "  ok"

$pyExe = Join-Path $pyDir ".venv\Scripts\python.exe"
if (-not (Test-Path $pyExe)) { $pyExe = "python" }

# ------------------------------------------------------------------ provider

Write-Step "subindo o provedor simulado (latencia=$ProviderLatency erro=$ProviderErrorRate)"
Stop-Port $PROVIDER_PORT
$provider = Start-Process -FilePath (Join-Path $binDir "provider.exe") `
    -ArgumentList @("-addr", ":$PROVIDER_PORT", "-latency", $ProviderLatency,
                    "-jitter", $ProviderJitter, "-error-rate", "$ProviderErrorRate") `
    -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 2
Write-Host "  no ar (pid $($provider.Id))"

$ok = @{}

try {
    # ------------------------------------------------------------------ Go
    if ($Targets -eq "go" -or $Targets -eq "both") {
        $ok["go"] = Invoke-Target -Label "go" -Port $GO_PORT `
            -Exe (Join-Path $binDir "api.exe") -WorkDir $goDir `
            -ExeArgs @("-addr", ":$GO_PORT", "-workers", "$Workers",
                       "-queue", "$QueueSize", "-retries", "$Retries",
                       "-allow-private", "-log", "warn")
    }

    # -------------------------------------------------------------- Python
    if ($Targets -eq "python" -or $Targets -eq "both") {
        $ok["python"] = Invoke-Target -Label "python" -Port $PY_PORT `
            -Exe $pyExe -WorkDir $pyDir `
            -ExeArgs @((Join-Path $pyDir "app.py"), "--port", "$PY_PORT",
                       "--workers", "$Workers", "--queue", "$QueueSize",
                       "--retries", "$Retries", "--allow-private")
    }
}
finally {
    Stop-Process -Id $provider.Id -Force -ErrorAction SilentlyContinue
    Stop-Port $PROVIDER_PORT
    Stop-Port $GO_PORT
    Stop-Port $PY_PORT
}

# ------------------------------------------------------------------ relatorio

if (-not $NoReport) {
    Write-Step "gerando o relatorio"
    $reportPath = Join-Path $PSScriptRoot "report.html"
    & (Join-Path $binDir "report.exe") -in $resultDir -out $reportPath
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "relatorio: $reportPath" -ForegroundColor Green
        Write-Host "abra com: start `"`" `"$reportPath`"" -ForegroundColor DarkGray
    }
}

Write-Host ""
foreach ($k in $ok.Keys) {
    if ($ok[$k]) { Write-Host "  $k : ok" -ForegroundColor Green }
    else         { Write-Host "  $k : FALHOU" -ForegroundColor Red }
}
Write-Host ""
Write-Host "Lembre-se: uma execucao nao e resultado. Rode 3 a 5 vezes e" -ForegroundColor Yellow
Write-Host "observe a variacao antes de concluir qualquer coisa." -ForegroundColor Yellow
