# ============================================================================
# Retail Application Docker Management Script
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("start", "stop", "restart", "status", "logs", "build", "env", "urls")]
    [string]$Action
)

$ContainerName = "ai-product-comparator"
$ImageName = "retail-app"
$Port = "8005"
$AppUrl = "http://localhost:$Port/retail-agent"
$LoginUrl = "http://localhost:$Port/retail-agent?page=login"
$HomeUrl = "http://localhost:$Port/retail-agent?page=home"

function Show-URLs {
    Write-Host "Application URLs:" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Login Page:" -ForegroundColor Yellow
    Write-Host "  $LoginUrl" -ForegroundColor Green
    Write-Host ""
    Write-Host "Home/Dashboard Page:" -ForegroundColor Yellow  
    Write-Host "  $HomeUrl" -ForegroundColor Green
    Write-Host ""
    Write-Host "Base URL (redirects based on auth):" -ForegroundColor Yellow
    Write-Host "  $AppUrl" -ForegroundColor Green
    Write-Host ""
    Write-Host "Routing Behavior:" -ForegroundColor Cyan
    Write-Host "• Unauthenticated users → Login page (?page=login)" -ForegroundColor White
    Write-Host "• Authenticated users → Home page (?page=home)" -ForegroundColor White
    Write-Host "• Direct URL access supported with query parameters" -ForegroundColor White
}

function Show-Environment {
    Write-Host "Checking Microsoft Authentication Configuration..." -ForegroundColor Cyan
    
    try {
        $clientId = docker exec $ContainerName python -c "import os; print(os.getenv('MICROSOFT_CLIENT_ID', 'NOT SET'))" 2>$null
        $tenantId = docker exec $ContainerName python -c "import os; print(os.getenv('MICROSOFT_TENANT_ID', 'NOT SET'))" 2>$null
        $redirectUri = docker exec $ContainerName python -c "import os; print(os.getenv('MICROSOFT_REDIRECT_URI', 'NOT SET'))" 2>$null
        $clientSecret = docker exec $ContainerName python -c "import os; secret = os.getenv('MICROSOFT_CLIENT_SECRET'); print('CONFIGURED' if secret else 'NOT SET')" 2>$null
        
        Write-Host "Microsoft Client ID: $clientId" -ForegroundColor $(if($clientId -ne "NOT SET") {"Green"} else {"Red"})
        Write-Host "Microsoft Tenant ID: $tenantId" -ForegroundColor $(if($tenantId -ne "NOT SET") {"Green"} else {"Red"})
        Write-Host "Microsoft Redirect URI: $redirectUri" -ForegroundColor $(if($redirectUri -ne "NOT SET") {"Green"} else {"Red"})
        Write-Host "Microsoft Client Secret: $clientSecret" -ForegroundColor $(if($clientSecret -eq "CONFIGURED") {"Green"} else {"Red"})
        
        if ($clientId -eq "NOT SET" -or $tenantId -eq "NOT SET" -or $redirectUri -eq "NOT SET" -or $clientSecret -eq "NOT SET") {
            Write-Host ""
            Write-Host "WARNING: Microsoft authentication is not fully configured!" -ForegroundColor Red
            Write-Host "Check the docker-compose.yml file for missing environment variables." -ForegroundColor Yellow
        } else {
            Write-Host ""
            Write-Host "Microsoft authentication is properly configured!" -ForegroundColor Green
            Write-Host "Active redirect URI: $redirectUri" -ForegroundColor Cyan
        }
    } catch {
        Write-Host "Could not check environment - container may not be running" -ForegroundColor Red
    }
}

function Show-Status {
    Write-Host "Checking container status..." -ForegroundColor Cyan
    docker ps -a --filter "name=$ContainerName"
    Write-Host ""
    Show-Environment
}

function Start-Container {
    Write-Host "Starting Retail Application..." -ForegroundColor Green
    docker-compose up -d --build
    Start-Sleep -Seconds 5
    Show-Status
    
    try {
        $response = Invoke-WebRequest -Uri $AppUrl -UseBasicParsing -TimeoutSec 10
        if ($response.StatusCode -eq 200) {
            Write-Host "Application is running successfully!" -ForegroundColor Green
            Write-Host "Login URL: $LoginUrl" -ForegroundColor Cyan
            Write-Host "Home URL: $HomeUrl" -ForegroundColor Cyan
            Write-Host "Base URL: $AppUrl" -ForegroundColor Cyan
        }
    } catch {
        Write-Host "Container started but app may still be loading..." -ForegroundColor Yellow
        Write-Host "Try accessing:" -ForegroundColor Cyan
        Write-Host "  Login: $LoginUrl" -ForegroundColor Cyan
        Write-Host "  Home: $HomeUrl" -ForegroundColor Cyan
        Write-Host "  Base: $AppUrl" -ForegroundColor Cyan
    }
}

function Stop-Container {
    Write-Host "Stopping Retail Application..." -ForegroundColor Red
    docker-compose down
}

function Restart-Container {
    Write-Host "Restarting Retail Application..." -ForegroundColor Yellow
    Stop-Container
    Start-Sleep -Seconds 2
    Start-Container
}

function Show-Logs {
    Write-Host "Container logs:" -ForegroundColor Cyan
    docker logs $ContainerName
}

function Build-Image {
    Write-Host "Building Docker image..." -ForegroundColor Cyan
    docker-compose build
}

# Main execution
switch ($Action) {
    "start" { Start-Container }
    "stop" { Stop-Container }
    "restart" { Restart-Container }
    "status" { Show-Status }
    "logs" { Show-Logs }
    "build" { Build-Image }
    "env" { Show-Environment }
    "urls" { Show-URLs }
}

Write-Host ""
Write-Host "Available commands:"
Write-Host "  .\manage_retail_app.ps1 start   - Start the application"
Write-Host "  .\manage_retail_app.ps1 stop    - Stop the application"
Write-Host "  .\manage_retail_app.ps1 restart - Restart the application"
Write-Host "  .\manage_retail_app.ps1 status  - Check container status"
Write-Host "  .\manage_retail_app.ps1 logs    - View container logs"
Write-Host "  .\manage_retail_app.ps1 build   - Rebuild the Docker image"
Write-Host "  .\manage_retail_app.ps1 env     - Check Microsoft auth configuration"
Write-Host "  .\manage_retail_app.ps1 urls    - Show application URLs and routing info"