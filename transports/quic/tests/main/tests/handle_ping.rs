/// No label, no compression
#[path = "handle_ping/no_label_no_compression.rs"]
pub mod no_label_no_compression;

/// No label, with compression
#[cfg(feature = "compression")]
#[path = "handle_ping/no_label_with_compression.rs"]
pub mod no_label_with_compression;

/// With label, with compression, no encryption
#[cfg(feature = "compression")]
#[path = "handle_ping/with_label_with_compression.rs"]
pub mod with_label_with_compression;

#[macro_export]
macro_rules! handle_ping_test_suites {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    $crate::__handle_ping_no_label_no_compression!($($prefix: )? $rt::$run({ $s }));

    // $crate::__handle_ping_label_only!($($prefix: )? $rt::$run({ $s }));

    // #[cfg(feature = "encryption")]
    // $crate::__handle_ping_no_label_no_compression_with_encryption!($($prefix: )? $rt::$run({ $s }));

    // #[cfg(feature = "compression")]
    // $crate::__handle_ping_no_label_compression_only!($($prefix: )? $rt::$run({ $s }));

    // #[cfg(feature = "compression")]
    // $crate::__handle_ping_with_label_and_compression!($($prefix: )? $rt::$run({ $s }));

    // #[cfg(all(feature = "compression", feature = "encryption"))]
    // $crate::__handle_ping_no_label_compression_and_encryption!($($prefix: )? $rt::$run({ $s }));
  };
}