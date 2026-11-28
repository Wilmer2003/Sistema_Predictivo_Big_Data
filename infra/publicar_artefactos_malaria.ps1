param(
    [Parameter(Mandatory)][string]$Bucket,
    [Parameter(Mandatory)][string]$Region,
    [string]$Python = 'python'
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$version = git rev-parse HEAD
if ($LASTEXITCODE -ne 0 -or $version -notmatch '^[a-f0-9]{40}$') {
    throw 'No se pudo identificar el commit.'
}
$cambios = git status --porcelain --untracked-files=all -- src infra/package_emr.py infra/publicar_artefactos_malaria.ps1
if ($LASTEXITCODE -ne 0 -or $cambios) {
    throw 'Primero confirma los cambios de código y migración del release.'
}
$carpeta = Join-Path ([IO.Path]::GetTempPath()) ("release-malaria-$version-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $carpeta | Out-Null
$zip = Join-Path $carpeta 'pipeline_src.zip'
& $Python -B infra/package_emr.py $zip
if ($LASTEXITCODE -ne 0) { throw 'Falló el empaquetado.' }

$archivos = @{
    'pipeline_src.zip' = $zip
    'run_malaria.py' = (Resolve-Path 'src/orchestration/run_malaria.py').Path
    '001_vigilancia_malaria.sql' = (Resolve-Path 'src/serving/migrations/001_vigilancia_malaria.sql').Path
}
foreach ($nombre in ($archivos.Keys | Sort-Object)) {
    $archivo = $archivos[$nombre]
    $huella = (Get-FileHash -LiteralPath $archivo -Algorithm SHA256).Hash
    $key = "scripts/malaria/$version/$nombre"
    aws s3api put-object --region $Region --bucket $Bucket --key $key --body $archivo --if-none-match '*'
    if ($LASTEXITCODE -ne 0) { throw "No se publicó $key. Comprueba si la clave ya existe y verifica su contenido." }
    Write-Output "$huella  s3://$Bucket/$key"
}
