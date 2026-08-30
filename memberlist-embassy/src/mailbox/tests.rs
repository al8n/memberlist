use super::{Command, Mailbox};
use core::net::SocketAddr;

fn sa(last: u8) -> SocketAddr {
  SocketAddr::from(([169, 254, 0, last], 7946))
}

/// `consume_command` clears ONLY the directive the worker arm acted on. With no
/// concurrently-posted directive, each handshake/close arm's command is consumed
/// to `Idle`.
#[test]
fn consume_command_clears_the_acted_directive() {
  let mut mb = Mailbox::new(64, 64);

  // Accept arm: acted on `Listen(port)`, nothing posted since ⇒ cleared.
  mb.command = Command::Listen(7946);
  mb.consume_command(Command::Listen(7946));
  assert_eq!(mb.command, Command::Idle);

  // Dial-success arm: acted on `Dial(remote)` ⇒ cleared.
  mb.command = Command::Dial(sa(2));
  mb.consume_command(Command::Dial(sa(2)));
  assert_eq!(mb.command, Command::Idle);

  // Established Close-consume: acted on `Close` ⇒ cleared.
  mb.command = Command::Close;
  mb.consume_command(Command::Close);
  assert_eq!(mb.command, Command::Idle);
}

/// A directive the engine posted AFTER the worker read the one it acted on (an
/// `Abort` retiring the slot, or a `Close`→`Abort` escalation) must SURVIVE the
/// consume: `command` no longer equals the acted directive, so the next command
/// match still tears the slot down instead of the arm erasing it into an orphan.
#[test]
fn consume_command_preserves_a_concurrently_posted_directive() {
  // Accept-success racing an abort: the abort must not be erased.
  let mut mb = Mailbox::new(64, 64);
  mb.command = Command::Abort;
  mb.consume_command(Command::Listen(7946));
  assert_eq!(
    mb.command,
    Command::Abort,
    "an abort posted while the accept completed must survive the consume"
  );

  // Dial-success racing an abort.
  mb.command = Command::Abort;
  mb.consume_command(Command::Dial(sa(2)));
  assert_eq!(
    mb.command,
    Command::Abort,
    "an abort posted while the connect completed must survive the consume"
  );

  // Close-consume racing an abort escalation (Draining→Aborting).
  mb.command = Command::Abort;
  mb.consume_command(Command::Close);
  assert_eq!(
    mb.command,
    Command::Abort,
    "an abort escalation while closing must survive the consume"
  );

  // A dial that is being re-targeted: the acted `Dial` differs from a freshly
  // posted `Dial` to another remote, so the new one survives.
  mb.command = Command::Dial(sa(3));
  mb.consume_command(Command::Dial(sa(2)));
  assert_eq!(mb.command, Command::Dial(sa(3)));
}
