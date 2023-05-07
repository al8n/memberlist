let wasm;
export function __wbg_set_wasm(val) {
    wasm = val;
}


const lTextDecoder = typeof TextDecoder === 'undefined' ? (0, module.require)('util').TextDecoder : TextDecoder;

let cachedTextDecoder = new lTextDecoder('utf-8', { ignoreBOM: true, fatal: true });

cachedTextDecoder.decode();

let cachedUint8Memory0 = null;

function getUint8Memory0() {
    if (cachedUint8Memory0 === null || cachedUint8Memory0.byteLength === 0) {
        cachedUint8Memory0 = new Uint8Array(wasm.memory.buffer);
    }
    return cachedUint8Memory0;
}

function getStringFromWasm0(ptr, len) {
    return cachedTextDecoder.decode(getUint8Memory0().subarray(ptr, ptr + len));
}

const heap = new Array(128).fill(undefined);

heap.push(undefined, null, true, false);

let heap_next = heap.length;

function addHeapObject(obj) {
    if (heap_next === heap.length) heap.push(heap.length + 1);
    const idx = heap_next;
    heap_next = heap[idx];

    heap[idx] = obj;
    return idx;
}

function getObject(idx) { return heap[idx]; }

function dropObject(idx) {
    if (idx < 132) return;
    heap[idx] = heap_next;
    heap_next = idx;
}

function takeObject(idx) {
    const ret = getObject(idx);
    dropObject(idx);
    return ret;
}

let cachedInt32Memory0 = null;

function getInt32Memory0() {
    if (cachedInt32Memory0 === null || cachedInt32Memory0.byteLength === 0) {
        cachedInt32Memory0 = new Int32Array(wasm.memory.buffer);
    }
    return cachedInt32Memory0;
}

function getArrayU8FromWasm0(ptr, len) {
    return getUint8Memory0().subarray(ptr / 1, ptr / 1 + len);
}

let WASM_VECTOR_LEN = 0;

const lTextEncoder = typeof TextEncoder === 'undefined' ? (0, module.require)('util').TextEncoder : TextEncoder;

let cachedTextEncoder = new lTextEncoder('utf-8');

const encodeString = (typeof cachedTextEncoder.encodeInto === 'function'
    ? function (arg, view) {
    return cachedTextEncoder.encodeInto(arg, view);
}
    : function (arg, view) {
    const buf = cachedTextEncoder.encode(arg);
    view.set(buf);
    return {
        read: arg.length,
        written: buf.length
    };
});

function passStringToWasm0(arg, malloc, realloc) {

    if (realloc === undefined) {
        const buf = cachedTextEncoder.encode(arg);
        const ptr = malloc(buf.length);
        getUint8Memory0().subarray(ptr, ptr + buf.length).set(buf);
        WASM_VECTOR_LEN = buf.length;
        return ptr;
    }

    let len = arg.length;
    let ptr = malloc(len);

    const mem = getUint8Memory0();

    let offset = 0;

    for (; offset < len; offset++) {
        const code = arg.charCodeAt(offset);
        if (code > 0x7F) break;
        mem[ptr + offset] = code;
    }

    if (offset !== len) {
        if (offset !== 0) {
            arg = arg.slice(offset);
        }
        ptr = realloc(ptr, len, len = offset + arg.length * 3);
        const view = getUint8Memory0().subarray(ptr + offset, ptr + len);
        const ret = encodeString(arg, view);

        offset += ret.written;
    }

    WASM_VECTOR_LEN = offset;
    return ptr;
}

function isLikeNone(x) {
    return x === undefined || x === null;
}

function _assertClass(instance, klass) {
    if (!(instance instanceof klass)) {
        throw new Error(`expected instance of ${klass.name}`);
    }
    return instance.ptr;
}

function passArray8ToWasm0(arg, malloc) {
    const ptr = malloc(arg.length * 1);
    getUint8Memory0().set(arg, ptr / 1);
    WASM_VECTOR_LEN = arg.length;
    return ptr;
}
/**
* A `StdDuration` type to represent a span of time, typically used for system
* timeouts.
*/
export class Duration {

    static __wrap(ptr) {
        const obj = Object.create(Duration.prototype);
        obj.ptr = ptr;

        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.ptr;
        this.ptr = 0;

        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_duration_free(ptr);
    }
    /**
    * @returns {Duration}
    */
    static zero() {
        const ret = wasm.duration_zero();
        return Duration.__wrap(ret);
    }
    /**
    * @returns {Duration}
    */
    static millisecond() {
        const ret = wasm.duration_millisecond();
        return Duration.__wrap(ret);
    }
    /**
    * @returns {Duration}
    */
    static second() {
        const ret = wasm.duration_second();
        return Duration.__wrap(ret);
    }
    /**
    * @returns {Duration}
    */
    static minute() {
        const ret = wasm.duration_minute();
        return Duration.__wrap(ret);
    }
    /**
    * @returns {Duration}
    */
    static hour() {
        const ret = wasm.duration_hour();
        return Duration.__wrap(ret);
    }
    /**
    * @returns {Duration}
    */
    static day() {
        const ret = wasm.duration_day();
        return Duration.__wrap(ret);
    }
    /**
    * @returns {Duration}
    */
    static week() {
        const ret = wasm.duration_week();
        return Duration.__wrap(ret);
    }
    /**
    * @param {bigint} millis
    * @returns {Duration}
    */
    static from_millis(millis) {
        const ret = wasm.duration_from_millis(millis);
        return Duration.__wrap(ret);
    }
    /**
    * @param {bigint} secs
    * @returns {Duration}
    */
    static from_secs(secs) {
        const ret = wasm.duration_from_secs(secs);
        return Duration.__wrap(ret);
    }
    /**
    * @returns {bigint}
    */
    as_millis() {
        const ret = wasm.duration_as_millis(this.ptr);
        return BigInt.asUintN(64, ret);
    }
    /**
    * @returns {bigint}
    */
    as_secs() {
        const ret = wasm.duration_as_secs(this.ptr);
        return BigInt.asUintN(64, ret);
    }
}
/**
*/
export class Options {

    static __wrap(ptr) {
        const obj = Object.create(Options.prototype);
        obj.ptr = ptr;

        return obj;
    }

    __destroy_into_raw() {
        const ptr = this.ptr;
        this.ptr = 0;

        return ptr;
    }

    free() {
        const ptr = this.__destroy_into_raw();
        wasm.__wbg_options_free(ptr);
    }
    /**
    * @returns {string}
    */
    name() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.options_name(retptr, this.ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_free(r0, r1);
        }
    }
    /**
    * @returns {string}
    */
    label() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.options_label(retptr, this.ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_free(r0, r1);
        }
    }
    /**
    * @returns {boolean}
    */
    skip_inbound_label_check() {
        const ret = wasm.options_skip_inbound_label_check(this.ptr);
        return ret !== 0;
    }
    /**
    * @returns {string}
    */
    bind_addr() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.options_bind_addr(retptr, this.ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_free(r0, r1);
        }
    }
    /**
    * @returns {string | undefined}
    */
    advertise_addr() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.options_advertise_addr(retptr, this.ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            let v0;
            if (r0 !== 0) {
                v0 = getStringFromWasm0(r0, r1).slice();
                wasm.__wbindgen_free(r0, r1 * 1);
            }
            return v0;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {number}
    */
    protocol_version() {
        const ret = wasm.options_protocol_version(this.ptr);
        return ret;
    }
    /**
    * @returns {Duration}
    */
    tcp_timeout() {
        const ret = wasm.options_tcp_timeout(this.ptr);
        return Duration.__wrap(ret);
    }
    /**
    * @returns {number}
    */
    indirect_checks() {
        const ret = wasm.options_indirect_checks(this.ptr);
        return ret >>> 0;
    }
    /**
    * @returns {number}
    */
    retransmit_mult() {
        const ret = wasm.options_retransmit_mult(this.ptr);
        return ret >>> 0;
    }
    /**
    * @returns {number}
    */
    suspicion_mult() {
        const ret = wasm.options_suspicion_mult(this.ptr);
        return ret >>> 0;
    }
    /**
    * @returns {number}
    */
    suspicion_max_timeout_mult() {
        const ret = wasm.options_suspicion_max_timeout_mult(this.ptr);
        return ret >>> 0;
    }
    /**
    * @returns {Duration}
    */
    push_pull_interval() {
        const ret = wasm.options_push_pull_interval(this.ptr);
        return Duration.__wrap(ret);
    }
    /**
    * @returns {Duration}
    */
    probe_interval() {
        const ret = wasm.options_probe_interval(this.ptr);
        return Duration.__wrap(ret);
    }
    /**
    * @returns {Duration}
    */
    probe_timeout() {
        const ret = wasm.options_probe_timeout(this.ptr);
        return Duration.__wrap(ret);
    }
    /**
    * @returns {boolean}
    */
    disable_tcp_pings() {
        const ret = wasm.options_disable_tcp_pings(this.ptr);
        return ret !== 0;
    }
    /**
    * @returns {number}
    */
    awareness_max_multiplier() {
        const ret = wasm.options_awareness_max_multiplier(this.ptr);
        return ret >>> 0;
    }
    /**
    * @returns {Duration}
    */
    gossip_interval() {
        const ret = wasm.options_gossip_interval(this.ptr);
        return Duration.__wrap(ret);
    }
    /**
    * @returns {number}
    */
    gossip_nodes() {
        const ret = wasm.options_gossip_nodes(this.ptr);
        return ret >>> 0;
    }
    /**
    * @returns {Duration}
    */
    gossip_to_the_dead_time() {
        const ret = wasm.options_gossip_to_the_dead_time(this.ptr);
        return Duration.__wrap(ret);
    }
    /**
    * @returns {boolean}
    */
    gossip_verify_incoming() {
        const ret = wasm.options_gossip_verify_incoming(this.ptr);
        return ret !== 0;
    }
    /**
    * @returns {boolean}
    */
    gossip_verify_outgoing() {
        const ret = wasm.options_gossip_verify_outgoing(this.ptr);
        return ret !== 0;
    }
    /**
    * @returns {boolean}
    */
    enable_compression() {
        const ret = wasm.options_enable_compression(this.ptr);
        return ret !== 0;
    }
    /**
    * @returns {Uint8Array | undefined}
    */
    secret_key() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.options_secret_key(retptr, this.ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            let v0;
            if (r0 !== 0) {
                v0 = getArrayU8FromWasm0(r0, r1).slice();
                wasm.__wbindgen_free(r0, r1 * 1);
            }
            return v0;
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
        }
    }
    /**
    * @returns {number}
    */
    delegate_protocol_version() {
        const ret = wasm.options_delegate_protocol_version(this.ptr);
        return ret;
    }
    /**
    * @returns {number}
    */
    delegate_protocol_min() {
        const ret = wasm.options_delegate_protocol_min(this.ptr);
        return ret;
    }
    /**
    * @returns {number}
    */
    delegate_protocol_max() {
        const ret = wasm.options_delegate_protocol_max(this.ptr);
        return ret;
    }
    /**
    * @returns {string}
    */
    dns_config_path() {
        try {
            const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
            wasm.options_dns_config_path(retptr, this.ptr);
            var r0 = getInt32Memory0()[retptr / 4 + 0];
            var r1 = getInt32Memory0()[retptr / 4 + 1];
            return getStringFromWasm0(r0, r1);
        } finally {
            wasm.__wbindgen_add_to_stack_pointer(16);
            wasm.__wbindgen_free(r0, r1);
        }
    }
    /**
    * @returns {number}
    */
    handoff_queue_depth() {
        const ret = wasm.options_handoff_queue_depth(this.ptr);
        return ret >>> 0;
    }
    /**
    * @returns {number}
    */
    packet_buffer_size() {
        const ret = wasm.options_packet_buffer_size(this.ptr);
        return ret >>> 0;
    }
    /**
    * @returns {Duration}
    */
    dead_node_reclaim_time() {
        const ret = wasm.options_dead_node_reclaim_time(this.ptr);
        return Duration.__wrap(ret);
    }
    /**
    * @returns {boolean}
    */
    require_node_names() {
        const ret = wasm.options_require_node_names(this.ptr);
        return ret !== 0;
    }
    /**
    * @returns {any}
    */
    cidrs_allowed() {
        const ret = wasm.options_cidrs_allowed(this.ptr);
        return takeObject(ret);
    }
    /**
    * @returns {Duration}
    */
    queue_check_interval() {
        const ret = wasm.options_queue_check_interval(this.ptr);
        return Duration.__wrap(ret);
    }
    /**
    * @param {string} val
    * @returns {Options}
    */
    with_name(val) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(val, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.options_with_name(ptr, ptr0, len0);
        return Options.__wrap(ret);
    }
    /**
    * @param {string} val
    * @returns {Options}
    */
    with_label(val) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(val, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.options_with_label(ptr, ptr0, len0);
        return Options.__wrap(ret);
    }
    /**
    * @param {boolean} val
    * @returns {Options}
    */
    with_skip_inbound_label_check(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_skip_inbound_label_check(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {string} val
    * @returns {Options}
    */
    with_bind_addr(val) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(val, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.options_with_bind_addr(ptr, ptr0, len0);
        return Options.__wrap(ret);
    }
    /**
    * @param {string | undefined} val
    * @returns {Options}
    */
    with_advertise_addr(val) {
        const ptr = this.__destroy_into_raw();
        var ptr0 = isLikeNone(val) ? 0 : passStringToWasm0(val, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.options_with_advertise_addr(ptr, ptr0, len0);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_protocol_version(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_protocol_version(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {Duration} val
    * @returns {Options}
    */
    with_tcp_timeout(val) {
        const ptr = this.__destroy_into_raw();
        _assertClass(val, Duration);
        var ptr0 = val.__destroy_into_raw();
        const ret = wasm.options_with_tcp_timeout(ptr, ptr0);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_indirect_checks(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_indirect_checks(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_retransmit_mult(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_retransmit_mult(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_suspicion_mult(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_suspicion_mult(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_suspicion_max_timeout_mult(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_suspicion_max_timeout_mult(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {Duration} val
    * @returns {Options}
    */
    with_push_pull_interval(val) {
        const ptr = this.__destroy_into_raw();
        _assertClass(val, Duration);
        var ptr0 = val.__destroy_into_raw();
        const ret = wasm.options_with_push_pull_interval(ptr, ptr0);
        return Options.__wrap(ret);
    }
    /**
    * @param {Duration} val
    * @returns {Options}
    */
    with_probe_interval(val) {
        const ptr = this.__destroy_into_raw();
        _assertClass(val, Duration);
        var ptr0 = val.__destroy_into_raw();
        const ret = wasm.options_with_probe_interval(ptr, ptr0);
        return Options.__wrap(ret);
    }
    /**
    * @param {Duration} val
    * @returns {Options}
    */
    with_probe_timeout(val) {
        const ptr = this.__destroy_into_raw();
        _assertClass(val, Duration);
        var ptr0 = val.__destroy_into_raw();
        const ret = wasm.options_with_probe_timeout(ptr, ptr0);
        return Options.__wrap(ret);
    }
    /**
    * @param {boolean} val
    * @returns {Options}
    */
    with_disable_tcp_pings(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_disable_tcp_pings(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_awareness_max_multiplier(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_awareness_max_multiplier(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {Duration} val
    * @returns {Options}
    */
    with_gossip_interval(val) {
        const ptr = this.__destroy_into_raw();
        _assertClass(val, Duration);
        var ptr0 = val.__destroy_into_raw();
        const ret = wasm.options_with_gossip_interval(ptr, ptr0);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_gossip_nodes(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_gossip_nodes(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {Duration} val
    * @returns {Options}
    */
    with_gossip_to_the_dead_time(val) {
        const ptr = this.__destroy_into_raw();
        _assertClass(val, Duration);
        var ptr0 = val.__destroy_into_raw();
        const ret = wasm.options_with_gossip_to_the_dead_time(ptr, ptr0);
        return Options.__wrap(ret);
    }
    /**
    * @param {boolean} val
    * @returns {Options}
    */
    with_gossip_verify_incoming(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_gossip_verify_incoming(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {boolean} val
    * @returns {Options}
    */
    with_gossip_verify_outgoing(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_gossip_verify_outgoing(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {boolean} val
    * @returns {Options}
    */
    with_enable_compression(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_enable_compression(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {Uint8Array | undefined} val
    * @returns {Options}
    */
    with_secret_key(val) {
        const ptr = this.__destroy_into_raw();
        var ptr0 = isLikeNone(val) ? 0 : passArray8ToWasm0(val, wasm.__wbindgen_malloc);
        var len0 = WASM_VECTOR_LEN;
        const ret = wasm.options_with_secret_key(ptr, ptr0, len0);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_delegate_protocol_version(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_delegate_protocol_version(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_delegate_protocol_min(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_delegate_protocol_min(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_delegate_protocol_max(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_delegate_protocol_max(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {string} val
    * @returns {Options}
    */
    with_dns_config_path(val) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(val, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.options_with_dns_config_path(ptr, ptr0, len0);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_handoff_queue_depth(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_handoff_queue_depth(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {number} val
    * @returns {Options}
    */
    with_packet_buffer_size(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_packet_buffer_size(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {Duration} val
    * @returns {Options}
    */
    with_dead_node_reclaim_time(val) {
        const ptr = this.__destroy_into_raw();
        _assertClass(val, Duration);
        var ptr0 = val.__destroy_into_raw();
        const ret = wasm.options_with_dead_node_reclaim_time(ptr, ptr0);
        return Options.__wrap(ret);
    }
    /**
    * @param {boolean} val
    * @returns {Options}
    */
    with_require_node_names(val) {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_with_require_node_names(ptr, val);
        return Options.__wrap(ret);
    }
    /**
    * @param {string} val
    * @returns {Options}
    */
    add_allowed_cidr(val) {
        const ptr = this.__destroy_into_raw();
        const ptr0 = passStringToWasm0(val, wasm.__wbindgen_malloc, wasm.__wbindgen_realloc);
        const len0 = WASM_VECTOR_LEN;
        const ret = wasm.options_add_allowed_cidr(ptr, ptr0, len0);
        return Options.__wrap(ret);
    }
    /**
    * @returns {Options}
    */
    clear_allowed_cidrs() {
        const ptr = this.__destroy_into_raw();
        const ret = wasm.options_clear_allowed_cidrs(ptr);
        return Options.__wrap(ret);
    }
    /**
    * @param {Duration} val
    * @returns {Options}
    */
    with_queue_check_interval(val) {
        const ptr = this.__destroy_into_raw();
        _assertClass(val, Duration);
        var ptr0 = val.__destroy_into_raw();
        const ret = wasm.options_with_queue_check_interval(ptr, ptr0);
        return Options.__wrap(ret);
    }
    /**
    * Returns a sane set of configurations for Memberlist.
    * It uses the hostname as the node name, and otherwise sets very conservative
    * values that are sane for most LAN environments. The default configuration
    * errs on the side of caution, choosing values that are optimized
    * for higher convergence at the cost of higher bandwidth usage. Regardless,
    * these values are a good starting point when getting started with memberlist.
    * @returns {Options}
    */
    static lan() {
        const ret = wasm.options_lan();
        return Options.__wrap(ret);
    }
    /**
    * Returns a configuration
    * that is optimized for most WAN environments. The default configuration is
    * still very conservative and errs on the side of caution.
    * @returns {Options}
    */
    static wan() {
        const ret = wasm.options_wan();
        return Options.__wrap(ret);
    }
    /**
    * Returns a configuration
    * that is optimized for a local loopback environments. The default configuration is
    * still very conservative and errs on the side of caution.
    * @returns {Options}
    */
    static local() {
        const ret = wasm.options_local();
        return Options.__wrap(ret);
    }
}

export function __wbindgen_string_new(arg0, arg1) {
    const ret = getStringFromWasm0(arg0, arg1);
    return addHeapObject(ret);
};

export function __wbindgen_object_drop_ref(arg0) {
    takeObject(arg0);
};

export function __wbg_new_b525de17f44a8943() {
    const ret = new Array();
    return addHeapObject(ret);
};

export function __wbg_push_49c286f04dd3bf59(arg0, arg1) {
    const ret = getObject(arg0).push(getObject(arg1));
    return ret;
};

export function __wbindgen_throw(arg0, arg1) {
    throw new Error(getStringFromWasm0(arg0, arg1));
};

