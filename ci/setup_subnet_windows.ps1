# This script adds or removes a range of IP addresses to the loopback interface on Windows.
param (
    [string]$action = "up"
)

# Function to check if an IP is already assigned
function Test-IPAddress {
    param (
        [string]$IPAddress,
        [string]$AddressFamily
    )
    $ipExists = Get-NetIPAddress -AddressFamily $AddressFamily -IPAddress $IPAddress -ErrorAction SilentlyContinue
    return $ipExists -ne $null
}

# Get the loopback interface for IPv4 and IPv6
$loopbackInterfaceIPv4 = Get-NetIPInterface -AddressFamily IPv4 | Where-Object { $_.InterfaceDescription -like "*Loopback*" }
$loopbackInterfaceIPv6 = Get-NetIPInterface -AddressFamily IPv6 | Where-Object { $_.InterfaceDescription -like "*Loopback*" }

if ($action -eq "up") {
    # Add IPv4 addresses to the loopback interface
    0..2 | ForEach-Object {
        $subnet = $_
        2..255 | ForEach-Object {
            $ipAddress = "127.0.$subnet.$_"
            if (-not (Test-IPAddress -IPAddress $ipAddress -AddressFamily IPv4)) {
                Write-Host "Adding IPv4 address $ipAddress"
                New-NetIPAddress -IPAddress $ipAddress -PrefixLength 8 -InterfaceIndex $loopbackInterfaceIPv4.InterfaceIndex -ErrorAction SilentlyContinue
            }
        }
    }

    # Add IPv6 addresses to the loopback interface
    # Example: Adding ::1:x addresses, adjust according to your needs
    2..255 | ForEach-Object {
        $ipAddress = "::1:$_"
        if (-not (Test-IPAddress -IPAddress $ipAddress -AddressFamily IPv6)) {
            Write-Host "Adding IPv6 address $ipAddress"
            New-NetIPAddress -IPAddress $ipAddress -PrefixLength 128 -InterfaceIndex $loopbackInterfaceIPv6.InterfaceIndex -ErrorAction SilentlyContinue
        }
    }
} else {
    # Remove IPv4 addresses from the loopback interface
    0..2 | ForEach-Object {
        $subnet = $_
        2..255 | ForEach-Object {
            $ipAddress = "127.0.$subnet.$_"
            if (Test-IPAddress -IPAddress $ipAddress -AddressFamily IPv4) {
                Write-Host "Removing IPv4 address $ipAddress"
                Remove-NetIPAddress -IPAddress $ipAddress -Confirm:$false -ErrorAction SilentlyContinue
            }
        }
    }

    # Remove IPv6 addresses from the loopback interface
    2..255 | ForEach-Object {
        $ipAddress = "::1:$_"
        if (Test-IPAddress -IPAddress $ipAddress -AddressFamily IPv6) {
            Write-Host "Removing IPv6 address $ipAddress"
            Remove-NetIPAddress -IPAddress $ipAddress -Confirm:$false -ErrorAction SilentlyContinue
        }
    }
}
