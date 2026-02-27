[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RepoUrl,

    [string]$Branch = "main",
    [string]$ResourceGroup = "rg-options-scanner",
    [string]$Location = "eastus",
    [string]$VmName = "vm-options-scanner",
    [string]$VmSize = "Standard_B2s",
    [string]$Image = "Canonical:ubuntu-24_04-lts:server:latest",
    [string]$AdminUsername = "azureuser",
    [string]$SshPublicKeyPath = "$HOME/.ssh/id_rsa.pub",
    [switch]$GenerateSshKeys,
    [string]$TimerOnCalendar = "hourly",
    [string]$PipelineArgs = "--container qc-backtest --skip-filter",
    [switch]$RunPipelineAfterProvision
)

$ErrorActionPreference = "Stop"

function Assert-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    if (-not (Get-Command -Name $Name -ErrorAction SilentlyContinue)) {
        throw "Missing required command '$Name'. Install it and retry."
    }
}

function Convert-ToBashSingleQuoted {
    param([AllowNull()][string]$Value)
    if ($null -eq $Value) {
        $Value = ""
    }
    return "'" + ($Value -replace "'", "'\"'\"'") + "'"
}

Assert-Command -Name "az"

if (-not $GenerateSshKeys -and -not (Test-Path -Path $SshPublicKeyPath)) {
    throw "SSH public key not found at '$SshPublicKeyPath'. Provide -SshPublicKeyPath or use -GenerateSshKeys."
}

if ($AdminUsername -notmatch "^[a-z_][a-z0-9_-]*$") {
    throw "AdminUsername must match Linux username pattern: ^[a-z_][a-z0-9_-]*$"
}
if ($TimerOnCalendar -match "[\r\n]") {
    throw "TimerOnCalendar cannot contain newline characters."
}

Write-Host "Checking Azure login..."
az account show --only-show-errors --output none
if ($LASTEXITCODE -ne 0) {
    throw "Azure CLI is not logged in. Run 'az login' and retry."
}

$repoBash = Convert-ToBashSingleQuoted -Value $RepoUrl
$branchBash = Convert-ToBashSingleQuoted -Value $Branch
$adminBash = Convert-ToBashSingleQuoted -Value $AdminUsername
$pipelineArgsBash = Convert-ToBashSingleQuoted -Value $PipelineArgs
$runAfterProvision = if ($RunPipelineAfterProvision) { "1" } else { "0" }
$timerRaw = $TimerOnCalendar
$adminRaw = $AdminUsername

$cloudInitTemplate = @'
#cloud-config
package_update: true
packages:
  - python3
  - python3-venv
  - python3-pip
  - git
  - tmux
  - util-linux
write_files:
  - path: /usr/local/bin/bootstrap-options-scanner.sh
    owner: root:root
    permissions: "0755"
    content: |
      #!/usr/bin/env bash
      set -euo pipefail

      REPO_URL=__REPO_URL__
      BRANCH=__BRANCH__
      APP_DIR="/opt/options-scanner"
      APP_OWNER=__ADMIN_USERNAME__

      mkdir -p /opt
      if [ ! -d "${APP_DIR}/.git" ]; then
        rm -rf "${APP_DIR}"
        git clone --branch "${BRANCH}" "${REPO_URL}" "${APP_DIR}"
      else
        cd "${APP_DIR}"
        git fetch --all
        git checkout "${BRANCH}"
        git pull --ff-only
      fi

      cd "${APP_DIR}"
      python3 -m venv .venv
      . .venv/bin/activate
      pip install --upgrade pip
      pip install -r requirements.txt

      if [ ! -f ".env" ] && [ -f ".env.example" ]; then
        cp .env.example .env
      fi

      if [ -f ".env" ]; then
        chmod 600 .env
      fi

      chown -R "${APP_OWNER}:${APP_OWNER}" "${APP_DIR}"
  - path: /usr/local/bin/run-options-pipeline.sh
    owner: root:root
    permissions: "0755"
    content: |
      #!/usr/bin/env bash
      set -euo pipefail

      APP_DIR="/opt/options-scanner"
      BRANCH=__BRANCH__
      PIPELINE_ARGS=__PIPELINE_ARGS__
      LOCK_FILE="/var/lock/options-scanner.lock"

      cd "${APP_DIR}"

      git fetch --all
      git checkout "${BRANCH}"
      git pull --ff-only

      python3 -m venv .venv
      . .venv/bin/activate
      pip install --upgrade pip
      pip install -r requirements.txt

      if [ -n "${PIPELINE_ARGS}" ]; then
        /usr/bin/flock -n "${LOCK_FILE}" bash -lc ". .venv/bin/activate && python run_full_pipeline.py ${PIPELINE_ARGS}" || {
          echo "Another pipeline run is active. Skipping this cycle."
          exit 0
        }
      else
        /usr/bin/flock -n "${LOCK_FILE}" bash -lc ". .venv/bin/activate && python run_full_pipeline.py" || {
          echo "Another pipeline run is active. Skipping this cycle."
          exit 0
        }
      fi
  - path: /etc/systemd/system/options-scanner.service
    owner: root:root
    permissions: "0644"
    content: |
      [Unit]
      Description=Options Scanner Pipeline
      After=network-online.target
      Wants=network-online.target

      [Service]
      Type=oneshot
      User=__ADMIN_USERNAME_RAW__
      WorkingDirectory=/opt/options-scanner
      ExecStart=/usr/local/bin/run-options-pipeline.sh
  - path: /etc/systemd/system/options-scanner.timer
    owner: root:root
    permissions: "0644"
    content: |
      [Unit]
      Description=Run Options Scanner Pipeline on schedule

      [Timer]
      OnBootSec=7min
      OnCalendar=__TIMER_ON_CALENDAR__
      Persistent=true
      RandomizedDelaySec=60
      Unit=options-scanner.service

      [Install]
      WantedBy=timers.target
runcmd:
  - /usr/local/bin/bootstrap-options-scanner.sh
  - systemctl daemon-reload
  - systemctl enable --now options-scanner.timer
  - bash -lc 'if [ "__RUN_PIPELINE_AFTER_PROVISION__" = "1" ]; then systemctl start options-scanner.service; fi'
'@

$cloudInit = $cloudInitTemplate.
    Replace("__REPO_URL__", $repoBash).
    Replace("__BRANCH__", $branchBash).
    Replace("__ADMIN_USERNAME__", $adminBash).
    Replace("__PIPELINE_ARGS__", $pipelineArgsBash).
    Replace("__ADMIN_USERNAME_RAW__", $adminRaw).
    Replace("__TIMER_ON_CALENDAR__", $timerRaw).
    Replace("__RUN_PIPELINE_AFTER_PROVISION__", $runAfterProvision)

$customDataPath = Join-Path -Path $env:TEMP -ChildPath "$VmName-cloud-init.yaml"
Set-Content -Path $customDataPath -Value $cloudInit -Encoding utf8

try {
    Write-Host "Creating/updating resource group '$ResourceGroup' in '$Location'..."
    az group create `
        --name $ResourceGroup `
        --location $Location `
        --only-show-errors `
        --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create/update resource group '$ResourceGroup'."
    }

    Write-Host "Creating VM '$VmName'..."
    $vmCreateArgs = @(
        "vm", "create",
        "--resource-group", $ResourceGroup,
        "--name", $VmName,
        "--image", $Image,
        "--size", $VmSize,
        "--admin-username", $AdminUsername,
        "--public-ip-sku", "Standard",
        "--nsg-rule", "SSH",
        "--custom-data", $customDataPath,
        "--only-show-errors",
        "--output", "json"
    )

    if ($GenerateSshKeys) {
        $vmCreateArgs += "--generate-ssh-keys"
    } else {
        $vmCreateArgs += @("--ssh-key-values", $SshPublicKeyPath)
    }

    $vmCreateOutput = az @vmCreateArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create VM '$VmName'."
    }

    $result = $vmCreateOutput | ConvertFrom-Json
    $publicIp = $result.publicIpAddress
    $fqdn = $result.fqdns

    Write-Host ""
    Write-Host "VM created successfully."
    Write-Host "Public IP: $publicIp"
    if ($fqdn) {
        Write-Host "FQDN: $fqdn"
    }
    Write-Host "Cloud-init will continue setup for a few minutes after first boot."
    Write-Host "Timer schedule: $TimerOnCalendar"
    Write-Host "Pipeline args: $PipelineArgs"
    Write-Host "SSH command:"
    Write-Host "ssh $AdminUsername@$publicIp"
    Write-Host "Check timer:"
    Write-Host "ssh $AdminUsername@$publicIp 'systemctl status options-scanner.timer --no-pager'"
    Write-Host "Check recent logs:"
    Write-Host "ssh $AdminUsername@$publicIp 'journalctl -u options-scanner.service -n 200 --no-pager'"
}
finally {
    if (Test-Path -Path $customDataPath) {
        Remove-Item -Path $customDataPath -Force
    }
}
