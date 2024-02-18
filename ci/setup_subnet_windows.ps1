# This script adds or removes a range of IP addresses to the loopback interface on Windows.

param (
    [string]$action = "up"
)

# Function to check if an IP is already assigned
function Test-IPAddress {
    param (
        [string]$IPAddress
    )
    $ipExists = Get-NetIPAddress -AddressFamily IPv4 -IPAddress $IPAddress -ErrorAction SilentlyContinue
    return $ipExists -ne $null
}

# Get the loopback interface
$loopbackInterface = Get-NetIPInterface -AddressFamily IPv4 | Where-Object { $_.InterfaceDescription -like "*Loopback*" }

if ($action -eq "up") {
    # Add IP addresses to the loopback interface
    0..2 | ForEach-Object {
        $subnet = $_
        2..255 | ForEach-Object {
            $ipAddress = "127.0.$subnet.$_"
            if (-not (Test-IPAddress -IPAddress $ipAddress)) {
                Write-Host "Adding IP address $ipAddress"
                New-NetIPAddress -IPAddress $ipAddress -PrefixLength 8 -InterfaceIndex $loopbackInterface.InterfaceIndex -ErrorAction SilentlyContinue
            }
        }
    }
} else {
    # Remove IP addresses from the loopback interface
    0..2 | ForEach-Object {
        $subnet = $_
        2..255 | ForEach-Object {
            $ipAddress = "127.0.$subnet.$_"
            if (Test-IPAddress -IPAddress $ipAddress) {
                Write-Host "Removing IP address $ipAddress"
                Remove-NetIPAddress -IPAddress $ipAddress -Confirm:$false -ErrorAction SilentlyContinue
            }
        }
    }
}
