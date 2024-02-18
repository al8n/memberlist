param (
    [string]$action = "up"
)

function Test-IPAddress {
    param (
        [string]$IPAddress,
        [string]$AddressFamily
    )
    $ipExists = Get-NetIPAddress -AddressFamily $AddressFamily -IPAddress $IPAddress -ErrorAction SilentlyContinue
    return $ipExists -ne $null
}

$loopbackInterfaceIPv4 = Get-NetIPInterface -AddressFamily IPv4 | Where-Object { $_.InterfaceDescription -like "*Loopback*" }
$loopbackInterfaceIPv6 = Get-NetIPInterface -AddressFamily IPv6 | Where-Object { $_.InterfaceDescription -like "*Loopback*" }

if ($action -eq "up") {
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

    # Adjusting the IPv6 address addition logic for clarity and correctness
    2..255 | ForEach-Object {
        # Constructing a proper IPv6 address string
        $ipAddress = "fc00::1:$_"
        if (-not (Test-IPAddress -IPAddress $ipAddress -AddressFamily IPv6)) {
            Write-Host "Adding IPv6 address $ipAddress"
            # Note: Specifying the correct prefix length for IPv6 ULA addresses, which is commonly 64, but here it is kept as 128 for individual addresses
            New-NetIPAddress -IPAddress $ipAddress -PrefixLength 128 -InterfaceIndex $loopbackInterfaceIPv6.InterfaceIndex -ErrorAction SilentlyContinue
        }
    }
} else {
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

    2..255 | ForEach-Object {
        $ipAddress = "fc00::1:$_"
        if (Test-IPAddress -IPAddress $ipAddress -AddressFamily IPv6) {
            Write-Host "Removing IPv6 address $ipAddress"
            Remove-NetIPAddress -IPAddress $ipAddress -Confirm:$false -ErrorAction SilentlyContinue
        }
    }
}
