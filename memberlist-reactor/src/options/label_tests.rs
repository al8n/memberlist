use super::*;
use crate::error::Error;

#[test]
fn with_label_validates_too_long() {
  let long = vec![b'x'; 254]; // one over the 253-byte max
  let result = MemberlistOptions::new().with_label(Some(long));
  assert!(
    matches!(result, Err(Error::InvalidLabel(_))),
    "a label exceeding 253 bytes must be rejected"
  );
}

#[test]
fn with_label_validates_non_utf8() {
  let bad = vec![0xff, 0xfe];
  let result = MemberlistOptions::new().with_label(Some(bad));
  assert!(
    matches!(result, Err(Error::InvalidLabel(_))),
    "a non-UTF-8 label must be rejected"
  );
}

#[test]
fn with_label_accepts_valid() {
  let opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-x".to_vec()))
    .expect("valid ASCII label must be accepted");
  assert_eq!(opts.label(), Some(b"cluster-x".as_slice()));
  assert!(!opts.skip_inbound_label_check());
}

#[test]
fn with_skip_inbound_label_check_round_trips() {
  let opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-x".to_vec()))
    .expect("valid label")
    .with_skip_inbound_label_check(true);
  assert_eq!(opts.label(), Some(b"cluster-x".as_slice()));
  assert!(opts.skip_inbound_label_check());
}

#[test]
fn empty_label_normalizes_to_none() {
  let opts = MemberlistOptions::new()
    .with_label(Some(Vec::new()))
    .expect("empty label is valid");
  assert_eq!(opts.label(), None);
}

#[cfg(feature = "serde")]
#[test]
fn label_serde_round_trips_and_sets() {
  // A configured label round-trips as a string.
  let opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-x".to_vec()))
    .expect("valid label");
  let json = serde_json::to_string(&opts).unwrap();
  let back: MemberlistOptions = serde_json::from_str(&json).unwrap();
  assert_eq!(back.label(), Some(b"cluster-x".as_slice()));

  // A config carrying a label actually sets it (the accessor is `Some`).
  let from_cfg: MemberlistOptions = serde_json::from_str(r#"{"label": "prod"}"#).unwrap();
  assert_eq!(from_cfg.label(), Some(b"prod".as_slice()));

  // Absent / empty normalize to no label.
  assert_eq!(
    serde_json::from_str::<MemberlistOptions>("{}")
      .unwrap()
      .label(),
    None
  );
  assert_eq!(
    serde_json::from_str::<MemberlistOptions>(r#"{"label": ""}"#)
      .unwrap()
      .label(),
    None
  );
}

#[cfg(feature = "serde")]
#[test]
fn label_serde_rejects_too_long() {
  let long = "x".repeat(254);
  let json = format!(r#"{{"label": "{long}"}}"#);
  assert!(
    serde_json::from_str::<MemberlistOptions>(&json).is_err(),
    "a label exceeding 253 bytes must be rejected by serde"
  );
}

#[cfg(feature = "clap")]
#[test]
fn label_clap_parses_and_rejects_too_long() {
  use clap::{CommandFactory, Parser};

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    memberlist: MemberlistOptions,
  }

  let cli = Cli::try_parse_from(["app", "--memberlist-label", "cluster-x"]).unwrap();
  assert_eq!(cli.memberlist.label(), Some(b"cluster-x".as_slice()));

  let long = "x".repeat(254);
  assert!(
    Cli::try_parse_from(["app", "--memberlist-label", &long]).is_err(),
    "a label exceeding 253 bytes must be rejected by clap"
  );

  let cmd = Cli::command();
  let arg = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "memberlist-label")
    .expect("memberlist-label arg is registered");
  assert_eq!(
    arg.get_env().and_then(|e| e.to_str()),
    Some("MEMBERLIST_LABEL")
  );
}
