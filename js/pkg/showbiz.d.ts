/* tslint:disable */
/* eslint-disable */
/**
* A `StdDuration` type to represent a span of time, typically used for system
* timeouts.
*/
export class Duration {
  free(): void;
/**
* @returns {Duration}
*/
  static zero(): Duration;
/**
* @returns {Duration}
*/
  static millisecond(): Duration;
/**
* @returns {Duration}
*/
  static second(): Duration;
/**
* @returns {Duration}
*/
  static minute(): Duration;
/**
* @returns {Duration}
*/
  static hour(): Duration;
/**
* @returns {Duration}
*/
  static day(): Duration;
/**
* @returns {Duration}
*/
  static week(): Duration;
/**
* @param {bigint} millis
* @returns {Duration}
*/
  static from_millis(millis: bigint): Duration;
/**
* @param {bigint} secs
* @returns {Duration}
*/
  static from_secs(secs: bigint): Duration;
/**
* @returns {bigint}
*/
  as_millis(): bigint;
/**
* @returns {bigint}
*/
  as_secs(): bigint;
}
/**
*/
export class Options {
  free(): void;
/**
* @returns {string}
*/
  name(): string;
/**
* @returns {string}
*/
  label(): string;
/**
* @returns {boolean}
*/
  skip_inbound_label_check(): boolean;
/**
* @returns {string}
*/
  bind_addr(): string;
/**
* @returns {string | undefined}
*/
  advertise_addr(): string | undefined;
/**
* @returns {number}
*/
  protocol_version(): number;
/**
* @returns {Duration}
*/
  tcp_timeout(): Duration;
/**
* @returns {number}
*/
  indirect_checks(): number;
/**
* @returns {number}
*/
  retransmit_mult(): number;
/**
* @returns {number}
*/
  suspicion_mult(): number;
/**
* @returns {number}
*/
  suspicion_max_timeout_mult(): number;
/**
* @returns {Duration}
*/
  push_pull_interval(): Duration;
/**
* @returns {Duration}
*/
  probe_interval(): Duration;
/**
* @returns {Duration}
*/
  probe_timeout(): Duration;
/**
* @returns {boolean}
*/
  disable_tcp_pings(): boolean;
/**
* @returns {number}
*/
  awareness_max_multiplier(): number;
/**
* @returns {Duration}
*/
  gossip_interval(): Duration;
/**
* @returns {number}
*/
  gossip_nodes(): number;
/**
* @returns {Duration}
*/
  gossip_to_the_dead_time(): Duration;
/**
* @returns {boolean}
*/
  gossip_verify_incoming(): boolean;
/**
* @returns {boolean}
*/
  gossip_verify_outgoing(): boolean;
/**
* @returns {boolean}
*/
  enable_compression(): boolean;
/**
* @returns {Uint8Array | undefined}
*/
  secret_key(): Uint8Array | undefined;
/**
* @returns {number}
*/
  delegate_protocol_version(): number;
/**
* @returns {number}
*/
  delegate_protocol_min(): number;
/**
* @returns {number}
*/
  delegate_protocol_max(): number;
/**
* @returns {string}
*/
  dns_config_path(): string;
/**
* @returns {number}
*/
  handoff_queue_depth(): number;
/**
* @returns {number}
*/
  packet_buffer_size(): number;
/**
* @returns {Duration}
*/
  dead_node_reclaim_time(): Duration;
/**
* @returns {boolean}
*/
  require_node_names(): boolean;
/**
* @returns {any}
*/
  cidrs_allowed(): any;
/**
* @returns {Duration}
*/
  queue_check_interval(): Duration;
/**
* @param {string} val
* @returns {Options}
*/
  with_name(val: string): Options;
/**
* @param {string} val
* @returns {Options}
*/
  with_label(val: string): Options;
/**
* @param {boolean} val
* @returns {Options}
*/
  with_skip_inbound_label_check(val: boolean): Options;
/**
* @param {string} val
* @returns {Options}
*/
  with_bind_addr(val: string): Options;
/**
* @param {string | undefined} val
* @returns {Options}
*/
  with_advertise_addr(val?: string): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_protocol_version(val: number): Options;
/**
* @param {Duration} val
* @returns {Options}
*/
  with_tcp_timeout(val: Duration): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_indirect_checks(val: number): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_retransmit_mult(val: number): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_suspicion_mult(val: number): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_suspicion_max_timeout_mult(val: number): Options;
/**
* @param {Duration} val
* @returns {Options}
*/
  with_push_pull_interval(val: Duration): Options;
/**
* @param {Duration} val
* @returns {Options}
*/
  with_probe_interval(val: Duration): Options;
/**
* @param {Duration} val
* @returns {Options}
*/
  with_probe_timeout(val: Duration): Options;
/**
* @param {boolean} val
* @returns {Options}
*/
  with_disable_tcp_pings(val: boolean): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_awareness_max_multiplier(val: number): Options;
/**
* @param {Duration} val
* @returns {Options}
*/
  with_gossip_interval(val: Duration): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_gossip_nodes(val: number): Options;
/**
* @param {Duration} val
* @returns {Options}
*/
  with_gossip_to_the_dead_time(val: Duration): Options;
/**
* @param {boolean} val
* @returns {Options}
*/
  with_gossip_verify_incoming(val: boolean): Options;
/**
* @param {boolean} val
* @returns {Options}
*/
  with_gossip_verify_outgoing(val: boolean): Options;
/**
* @param {boolean} val
* @returns {Options}
*/
  with_enable_compression(val: boolean): Options;
/**
* @param {Uint8Array | undefined} val
* @returns {Options}
*/
  with_secret_key(val?: Uint8Array): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_delegate_protocol_version(val: number): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_delegate_protocol_min(val: number): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_delegate_protocol_max(val: number): Options;
/**
* @param {string} val
* @returns {Options}
*/
  with_dns_config_path(val: string): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_handoff_queue_depth(val: number): Options;
/**
* @param {number} val
* @returns {Options}
*/
  with_packet_buffer_size(val: number): Options;
/**
* @param {Duration} val
* @returns {Options}
*/
  with_dead_node_reclaim_time(val: Duration): Options;
/**
* @param {boolean} val
* @returns {Options}
*/
  with_require_node_names(val: boolean): Options;
/**
* @param {string} val
* @returns {Options}
*/
  add_allowed_cidr(val: string): Options;
/**
* @returns {Options}
*/
  clear_allowed_cidrs(): Options;
/**
* @param {Duration} val
* @returns {Options}
*/
  with_queue_check_interval(val: Duration): Options;
/**
* Returns a sane set of configurations for Memberlist.
* It uses the hostname as the node name, and otherwise sets very conservative
* values that are sane for most LAN environments. The default configuration
* errs on the side of caution, choosing values that are optimized
* for higher convergence at the cost of higher bandwidth usage. Regardless,
* these values are a good starting point when getting started with memberlist.
* @returns {Options}
*/
  static lan(): Options;
/**
* Returns a configuration
* that is optimized for most WAN environments. The default configuration is
* still very conservative and errs on the side of caution.
* @returns {Options}
*/
  static wan(): Options;
/**
* Returns a configuration
* that is optimized for a local loopback environments. The default configuration is
* still very conservative and errs on the side of caution.
* @returns {Options}
*/
  static local(): Options;
}
