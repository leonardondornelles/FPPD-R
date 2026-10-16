# Roda os experimentos do Problema 2 e salva as saídas em resultados/.
# Uso, na raiz do repositório: powershell -ExecutionPolicy Bypass -File coletar.ps1
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
New-Item -ItemType Directory -Force resultados | Out-Null

go version | Out-File -Encoding utf8 resultados\ambiente.txt
Get-CimInstance Win32_Processor | Select-Object -ExpandProperty Name | Out-File -Append -Encoding utf8 resultados\ambiente.txt

go build -o pc.exe ./produtor-consumidor
Write-Host "Benchmark com o timeout do consumidor 0 (pode levar alguns minutos)"
& .\pc.exe -bench -p 4 -c 4 -n 125000 | Tee-Object -FilePath resultados\bench_com_timeout.txt
Write-Host "`nBenchmark sem timeout (comparação controlada)"
& .\pc.exe -bench -p 4 -c 4 -n 125000 -timeout 0 | Tee-Object -FilePath resultados\bench_sem_timeout.txt
Remove-Item pc.exe
