//! Host+port address type for memberlist-compio.

use hostaddr::HostAddr;
use smol_str::SmolStr;

/// Host+port address — `SmolStr`-backed for cheap-clone semantics.
/// Short hostnames live inline (< 24 bytes on 64-bit); longer use Arc
/// backing. Parses from `"host:port"` via `FromStr`.
pub type Address = HostAddr<SmolStr>;
