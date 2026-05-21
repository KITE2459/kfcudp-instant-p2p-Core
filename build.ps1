$ErrorActionPreference = "Stop"

$targets = @(
    @{ GOOS = "linux";   GOARCH = "amd64";  OUT = "openfriend-linux-amd64";            CMD = "./cmd/openfriend" },
    @{ GOOS = "linux";   GOARCH = "amd64";  OUT = "openfriend-quic-linux-amd64";       CMD = "./cmd/openfriend-quic" },
    @{ GOOS = "windows"; GOARCH = "amd64";  OUT = "openfriend-windows-amd64.exe";      CMD = "./cmd/openfriend" },
    @{ GOOS = "windows"; GOARCH = "amd64";  OUT = "openfriend-quic-windows-amd64.exe"; CMD = "./cmd/openfriend-quic" }
)

# 이전 빌드 파일 삭제 (UPX AlreadyPackedException 방지)
foreach ($t in $targets) {
    if (Test-Path $t.OUT) {
        Remove-Item $t.OUT -Force
    }
}

foreach ($t in $targets) {
    $env:GOOS   = $t.GOOS
    $env:GOARCH = $t.GOARCH
    Write-Host "Building $($t.OUT)..." -ForegroundColor Cyan
    go build -ldflags="-s -w" -o $t.OUT $t.CMD
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $($t.OUT)" -ForegroundColor Red
        exit 1
    }
    Write-Host "OK: $($t.OUT)" -ForegroundColor Green
}

Write-Host ""

# UPX 압축
$upx = Get-Command upx -ErrorAction SilentlyContinue
if ($upx) {
    Write-Host "Compressing with UPX..." -ForegroundColor Cyan
    $bins = Get-Item "openfriend-*" | Where-Object { -not $_.PSIsContainer }
    foreach ($bin in $bins) {
        Write-Host "  upx $($bin.Name)" -ForegroundColor Cyan
        upx --best $bin.FullName
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  FAILED: $($bin.Name)" -ForegroundColor Red
            exit 1
        }
    }
    Write-Host "UPX done." -ForegroundColor Green
} else {
    Write-Host "UPX not found, skipping compression." -ForegroundColor Yellow
    Write-Host "Install: winget install upx" -ForegroundColor Yellow
}

Write-Host "`nAll builds completed." -ForegroundColor Yellow