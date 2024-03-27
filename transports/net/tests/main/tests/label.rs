#[path = "label/with_label.rs"]
mod with_label;

#[path = "label/with_label_and_encryption.rs"]
#[cfg(feature = "encryption")]
mod with_label_and_encryption;

#[macro_export]
macro_rules! label_test_suites {
  ($($prefix:literal: )? $layer:ident<$rt:ident>::$run:ident({ $s: expr })) => {
    $crate::__label_only!($($prefix: )? $layer<$rt>::$run({ $s }));


    #[cfg(feature = "encryption")]
    $crate::__label_and_encryption!($($prefix: )? $layer<$rt>::$run({ $s }));
  };
}
