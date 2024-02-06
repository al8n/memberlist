#[macro_use]
#[path = "handle_ping/no_label_no_compression_no_encryption.rs"]
mod no_label_no_compression_no_encryption;

#[macro_use]
#[path = "handle_ping/no_label_no_compression_with_encryption.rs"]
#[cfg(feature = "encryption")]
mod no_label_no_compression_with_encryption;

#[macro_use]
#[path = "handle_ping/no_label_with_compression_no_encryption.rs"]
#[cfg(feature = "compression")]
mod no_label_with_compression_no_encryption;

#[macro_use]
#[path = "handle_ping/no_label_with_compression_with_encryption.rs"]
#[cfg(all(feature = "compression", feature = "encryption"))]
mod no_label_with_compression_with_encryption;

#[macro_use]
#[path = "handle_ping/with_label_no_compression_no_encryption.rs"]
mod with_label_no_compression_no_encryption;

#[path = "handle_ping/with_label_with_compression_no_encryption.rs"]
#[cfg(feature = "compression")]
mod with_label_with_compression_no_encryption;

#[macro_export]
macro_rules! handle_ping_test_suites {
  ($($prefix:literal: )? $rt:ident::$run:ident({ $s: expr })) => {
    $crate::__handle_ping_no_label_no_compression_no_encryption!($($prefix: )? $rt::$run({ $s }));

    $crate::__handle_ping_label_only!($($prefix: )? $rt::$run({ $s }));

    #[cfg(feature = "encryption")]
    $crate::__handle_ping_no_label_no_compression_with_encryption!($($prefix: )? $rt::$run({ $s }));

    #[cfg(feature = "compression")]
    $crate::__handle_ping_no_label_compression_only!($($prefix: )? $rt::$run({ $s }));

    #[cfg(feature = "compression")]
    $crate::__handle_ping_with_label_and_compression!($($prefix: )? $rt::$run({ $s }));

    #[cfg(all(feature = "compression", feature = "encryption"))]
    $crate::__handle_ping_no_label_compression_and_encryption!($($prefix: )? $rt::$run({ $s }));
  };
}
