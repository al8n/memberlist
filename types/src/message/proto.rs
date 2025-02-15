pub use decoder::*;
pub use encoder::*;

mod decoder;
mod encoder;

const PAYLOAD_LEN_SIZE: usize = 4;
const MAX_MESSAGES_PER_BATCH: usize = 255;
const BATCH_OVERHEAD: usize = 2; // 1 byte for the batch message tag, 1 byte for the number of messages
