# Roda os casos de validação com o detector de corridas (-race).
# Uso, na raiz do repositório: powershell -ExecutionPolicy Bypass -File validar.ps1
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$falhas = 0
$casos = 0
function Falha($msg) { Write-Host "FALHA: $msg" -ForegroundColor Red; $script:falhas++ }

go vet ./...
if ($LASTEXITCODE -ne 0) { Falha "go vet" }
go build -race -o fil.exe ./filosofos
go build -race -o pc.exe ./produtor-consumidor
if (-not (Test-Path fil.exe) -or -not (Test-Path pc.exe)) { Write-Host "erro de compilação"; exit 1 }

Write-Host "Filósofos: 3 estratégias x N = 2, 5, 20"
foreach ($e in "deadlock", "hierarchy", "conductor") {
    foreach ($n in 2, 5, 20) {
        $casos++
        $saida = (& .\fil.exe -strategy $e -n $n -r 20) -join "`n"
        $esperado = if ($e -eq "deadlock") { "DEADLOCK" } else { "terminaram as refei" }
        if ($LASTEXITCODE -ne 0 -or $saida -notmatch $esperado) { Falha "filosofos $e n=$n" }
    }
}

Write-Host "Produtor/consumidor: 2 modos x K = 1, 2, 10, 100 x (P,C) x itens = 0, 1, 500"
foreach ($modo in "channel", "semaphore") {
    foreach ($k in 1, 2, 10, 100) {
        foreach ($pc in @(@(1, 1), @(3, 7), @(4, 4), @(8, 2))) {
            foreach ($n in 0, 1, 500) {
                $casos++
                $saida = (& .\pc.exe -mode $modo -k $k -p $pc[0] -c $pc[1] -n $n -timeout 1ms) -join "`n"
                if ($LASTEXITCODE -ne 0 -or $saida -notmatch "exatamente uma vez: true") {
                    Falha "$modo k=$k p=$($pc[0]) c=$($pc[1]) n=$n"
                }
            }
        }
    }
}

Write-Host "Timeout com produtor lento e consumidor sem timeout"
foreach ($modo in "channel", "semaphore") {
    $casos++
    $saida = (& .\pc.exe -mode $modo -p 1 -c 2 -n 10 -producer-delay 20ms -timeout 2ms) -join "`n"
    if ($LASTEXITCODE -ne 0 -or $saida -notmatch "timeout 1" -or $saida -notmatch "exatamente uma vez: true") { Falha "timeout $modo" }
    $casos++
    & .\pc.exe -mode $modo -timeout 0 -n 200 | Out-Null
    if ($LASTEXITCODE -ne 0) { Falha "sem timeout $modo" }
}

Write-Host "Parâmetros inválidos devem ser recusados"
foreach ($a in @(@("-n", "1"), @("-r", "0"), @("-strategy", "x"))) {
    $casos++
    & .\fil.exe @a 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { Falha "filosofos aceitou $a" }
}
foreach ($a in @(@("-k", "0"), @("-p", "0"), @("-c", "0"), @("-n", "-1"), @("-mode", "x"), @("-timeout", "-1ms"))) {
    $casos++
    & .\pc.exe @a 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { Falha "produtor-consumidor aceitou $a" }
}

Remove-Item fil.exe, pc.exe
Write-Host "$casos casos, $falhas falhas"
if ($falhas -gt 0) { exit 1 }
