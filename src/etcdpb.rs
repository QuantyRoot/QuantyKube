//! The etcd v3 API messages (etcd v3.6, packages `etcdserverpb` and
//! `mvccpb`) and the gRPC health check, encoded and decoded by hand on top
//! of `quantycore::proto`.

use quantycore::proto::{
    decode_varint, encode_key, encode_varint, put_bool, put_bytes, put_int, put_message, put_uint,
    DecodeError, Reader, Value, WireType,
};

pub trait Message: Default {
    fn write(&self, out: &mut Vec<u8>);

    /// Fields already set are overwritten by scalars and merged into by
    /// embedded messages, the way protobuf treats a message sent in pieces.
    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError>;

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.write(&mut out);
        out
    }

    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let mut m = Self::default();
        m.merge(buf)?;
        Ok(m)
    }
}

// A Txn can contain a Txn. Without a bound, a small message nested deeply
// enough would overflow the stack while decoding.
const MAX_TXN_DEPTH: usize = 64;

fn each_field<'a>(
    buf: &'a [u8],
    mut f: impl FnMut(u32, Value<'a>) -> Result<(), DecodeError>,
) -> Result<(), DecodeError> {
    let mut r = Reader::new(buf);
    while let Some((n, v)) = r.next_field()? {
        f(n, v)?;
    }
    Ok(())
}

fn bytes(v: Value, n: u32) -> Result<Vec<u8>, DecodeError> {
    v.bytes(n).map(<[u8]>::to_vec)
}

// proto3 requires strings to be UTF-8. DecodeError has no variant for
// that, so a bad string is reported as a field of the wrong type.
fn string(v: Value, n: u32) -> Result<String, DecodeError> {
    String::from_utf8(bytes(v, n)?).map_err(|_| DecodeError::WrongWireType(n))
}

// Enums are int32 on the wire; protobuf keeps the low 32 bits of the varint.
fn enum_value(v: Value, n: u32) -> Result<i32, DecodeError> {
    v.uint(n).map(|x| x as i32)
}

fn put_enum(out: &mut Vec<u8>, field: u32, v: i32) {
    put_int(out, field, i64::from(v));
}

fn put_string(out: &mut Vec<u8>, field: u32, s: &str) {
    put_bytes(out, field, s.as_bytes());
}

fn put_msg<M: Message>(out: &mut Vec<u8>, field: u32, m: &M) {
    put_message(out, field, &m.encode());
}

fn put_opt<M: Message>(out: &mut Vec<u8>, field: u32, m: &Option<M>) {
    if let Some(m) = m {
        put_msg(out, field, m);
    }
}

fn put_msgs<M: Message>(out: &mut Vec<u8>, field: u32, ms: &[M]) {
    for m in ms {
        put_msg(out, field, m);
    }
}

// Elements of a repeated bytes or string field are written even when
// empty, which put_bytes would skip.
fn put_each_bytes<T: AsRef<[u8]>>(out: &mut Vec<u8>, field: u32, items: &[T]) {
    for item in items {
        put_message(out, field, item.as_ref());
    }
}

// A oneof member carries presence, so it is written even when it holds
// zero: a Compare on mod_revision == 0 must keep its target.
fn put_oneof_int(out: &mut Vec<u8>, field: u32, v: i64) {
    encode_key(field, WireType::Varint, out);
    encode_varint(v as u64, out);
}

fn merge_opt<M: Message>(slot: &mut Option<M>, buf: &[u8]) -> Result<(), DecodeError> {
    slot.get_or_insert_with(M::default).merge(buf)
}

fn push_msg<M: Message>(list: &mut Vec<M>, buf: &[u8]) -> Result<(), DecodeError> {
    list.push(M::decode(buf)?);
    Ok(())
}

// Writers pack repeated enums into one length-delimited field, but a
// parser must accept each element as its own varint field too, and both
// forms may be mixed in one message.
fn push_enums<E: From<i32>>(list: &mut Vec<E>, v: Value, n: u32) -> Result<(), DecodeError> {
    match v {
        Value::Varint(x) => list.push(E::from(x as i32)),
        Value::Len(mut packed) => {
            while !packed.is_empty() {
                let (x, used) = decode_varint(packed)?;
                list.push(E::from(x as i32));
                packed = &packed[used..];
            }
        }
        _ => return Err(DecodeError::WrongWireType(n)),
    }
    Ok(())
}

fn put_packed_enums(out: &mut Vec<u8>, field: u32, values: impl ExactSizeIterator<Item = i32>) {
    if values.len() == 0 {
        return;
    }
    let mut packed = Vec::new();
    for v in values {
        encode_varint(i64::from(v) as u64, &mut packed);
    }
    put_message(out, field, &packed);
}

// Values outside the schema are kept in Other so that decoding never
// fails on an enum and re-encoding gives back the same number.
macro_rules! proto_enum {
    ($name:ident { $($variant:ident = $value:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($variant,)+
            Other(i32),
        }

        impl From<i32> for $name {
            fn from(v: i32) -> Self {
                match v {
                    $($value => $name::$variant,)+
                    other => $name::Other(other),
                }
            }
        }

        impl From<$name> for i32 {
            fn from(v: $name) -> i32 {
                match v {
                    $($name::$variant => $value,)+
                    $name::Other(other) => other,
                }
            }
        }

        impl Default for $name {
            fn default() -> Self {
                $name::from(0)
            }
        }
    };
}

proto_enum!(EventType { Put = 0, Delete = 1 });
proto_enum!(SortOrder {
    None = 0,
    Ascend = 1,
    Descend = 2
});
proto_enum!(SortTarget {
    Key = 0,
    Version = 1,
    Create = 2,
    Mod = 3,
    Value = 4
});
proto_enum!(CompareResult {
    Equal = 0,
    Greater = 1,
    Less = 2,
    NotEqual = 3
});
proto_enum!(CompareTarget {
    Version = 0,
    Create = 1,
    Mod = 2,
    Value = 3,
    Lease = 4
});
proto_enum!(FilterType {
    NoPut = 0,
    NoDelete = 1
});
proto_enum!(ServingStatus {
    Unknown = 0,
    Serving = 1,
    NotServing = 2,
    ServiceUnknown = 3
});

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub value: Vec<u8>,
    pub lease: i64,
}

impl Message for KeyValue {
    fn write(&self, out: &mut Vec<u8>) {
        put_bytes(out, 1, &self.key);
        put_int(out, 2, self.create_revision);
        put_int(out, 3, self.mod_revision);
        put_int(out, 4, self.version);
        put_bytes(out, 5, &self.value);
        put_int(out, 6, self.lease);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.key = bytes(v, n)?,
                2 => self.create_revision = v.int(n)?,
                3 => self.mod_revision = v.int(n)?,
                4 => self.version = v.int(n)?,
                5 => self.value = bytes(v, n)?,
                6 => self.lease = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Event {
    pub kind: EventType,
    pub kv: Option<KeyValue>,
    pub prev_kv: Option<KeyValue>,
}

impl Message for Event {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.kind.into());
        put_opt(out, 2, &self.kv);
        put_opt(out, 3, &self.prev_kv);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.kind = enum_value(v, n)?.into(),
                2 => merge_opt(&mut self.kv, v.bytes(n)?)?,
                3 => merge_opt(&mut self.prev_kv, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResponseHeader {
    pub cluster_id: u64,
    pub member_id: u64,
    pub revision: i64,
    pub raft_term: u64,
}

impl Message for ResponseHeader {
    fn write(&self, out: &mut Vec<u8>) {
        put_uint(out, 1, self.cluster_id);
        put_uint(out, 2, self.member_id);
        put_int(out, 3, self.revision);
        put_uint(out, 4, self.raft_term);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.cluster_id = v.uint(n)?,
                2 => self.member_id = v.uint(n)?,
                3 => self.revision = v.int(n)?,
                4 => self.raft_term = v.uint(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeRequest {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub limit: i64,
    pub revision: i64,
    pub sort_order: SortOrder,
    pub sort_target: SortTarget,
    pub serializable: bool,
    pub keys_only: bool,
    pub count_only: bool,
    pub min_mod_revision: i64,
    pub max_mod_revision: i64,
    pub min_create_revision: i64,
    pub max_create_revision: i64,
}

impl Message for RangeRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_bytes(out, 1, &self.key);
        put_bytes(out, 2, &self.range_end);
        put_int(out, 3, self.limit);
        put_int(out, 4, self.revision);
        put_enum(out, 5, self.sort_order.into());
        put_enum(out, 6, self.sort_target.into());
        put_bool(out, 7, self.serializable);
        put_bool(out, 8, self.keys_only);
        put_bool(out, 9, self.count_only);
        put_int(out, 10, self.min_mod_revision);
        put_int(out, 11, self.max_mod_revision);
        put_int(out, 12, self.min_create_revision);
        put_int(out, 13, self.max_create_revision);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.key = bytes(v, n)?,
                2 => self.range_end = bytes(v, n)?,
                3 => self.limit = v.int(n)?,
                4 => self.revision = v.int(n)?,
                5 => self.sort_order = enum_value(v, n)?.into(),
                6 => self.sort_target = enum_value(v, n)?.into(),
                7 => self.serializable = v.bool(n)?,
                8 => self.keys_only = v.bool(n)?,
                9 => self.count_only = v.bool(n)?,
                10 => self.min_mod_revision = v.int(n)?,
                11 => self.max_mod_revision = v.int(n)?,
                12 => self.min_create_revision = v.int(n)?,
                13 => self.max_create_revision = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeResponse {
    pub header: Option<ResponseHeader>,
    pub kvs: Vec<KeyValue>,
    pub more: bool,
    pub count: i64,
}

impl Message for RangeResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_msgs(out, 2, &self.kvs);
        put_bool(out, 3, self.more);
        put_int(out, 4, self.count);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => push_msg(&mut self.kvs, v.bytes(n)?)?,
                3 => self.more = v.bool(n)?,
                4 => self.count = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutRequest {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub lease: i64,
    pub prev_kv: bool,
    pub ignore_value: bool,
    pub ignore_lease: bool,
}

impl Message for PutRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_bytes(out, 1, &self.key);
        put_bytes(out, 2, &self.value);
        put_int(out, 3, self.lease);
        put_bool(out, 4, self.prev_kv);
        put_bool(out, 5, self.ignore_value);
        put_bool(out, 6, self.ignore_lease);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.key = bytes(v, n)?,
                2 => self.value = bytes(v, n)?,
                3 => self.lease = v.int(n)?,
                4 => self.prev_kv = v.bool(n)?,
                5 => self.ignore_value = v.bool(n)?,
                6 => self.ignore_lease = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutResponse {
    pub header: Option<ResponseHeader>,
    pub prev_kv: Option<KeyValue>,
}

impl Message for PutResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_opt(out, 2, &self.prev_kv);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => merge_opt(&mut self.prev_kv, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteRangeRequest {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub prev_kv: bool,
}

impl Message for DeleteRangeRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_bytes(out, 1, &self.key);
        put_bytes(out, 2, &self.range_end);
        put_bool(out, 3, self.prev_kv);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.key = bytes(v, n)?,
                2 => self.range_end = bytes(v, n)?,
                3 => self.prev_kv = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteRangeResponse {
    pub header: Option<ResponseHeader>,
    pub deleted: i64,
    pub prev_kvs: Vec<KeyValue>,
}

impl Message for DeleteRangeResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_int(out, 2, self.deleted);
        put_msgs(out, 3, &self.prev_kvs);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => self.deleted = v.int(n)?,
                3 => push_msg(&mut self.prev_kvs, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpRequest {
    Range(RangeRequest),
    Put(PutRequest),
    DeleteRange(DeleteRangeRequest),
    Txn(TxnRequest),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestOp {
    pub request: Option<OpRequest>,
}

impl RequestOp {
    fn merge_at(&mut self, buf: &[u8], depth: usize) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match (n, &mut self.request) {
                (1, Some(OpRequest::Range(m))) => m.merge(v.bytes(n)?)?,
                (1, slot) => *slot = Some(OpRequest::Range(Message::decode(v.bytes(n)?)?)),
                (2, Some(OpRequest::Put(m))) => m.merge(v.bytes(n)?)?,
                (2, slot) => *slot = Some(OpRequest::Put(Message::decode(v.bytes(n)?)?)),
                (3, Some(OpRequest::DeleteRange(m))) => m.merge(v.bytes(n)?)?,
                (3, slot) => *slot = Some(OpRequest::DeleteRange(Message::decode(v.bytes(n)?)?)),
                (4, slot) => {
                    if depth >= MAX_TXN_DEPTH {
                        return Err(DecodeError::Overflow);
                    }
                    if !matches!(slot, Some(OpRequest::Txn(_))) {
                        *slot = Some(OpRequest::Txn(TxnRequest::default()));
                    }
                    if let Some(OpRequest::Txn(m)) = slot {
                        m.merge_at(v.bytes(n)?, depth + 1)?;
                    }
                }
                _ => {}
            }
            Ok(())
        })
    }
}

impl Message for RequestOp {
    fn write(&self, out: &mut Vec<u8>) {
        match &self.request {
            Some(OpRequest::Range(m)) => put_msg(out, 1, m),
            Some(OpRequest::Put(m)) => put_msg(out, 2, m),
            Some(OpRequest::DeleteRange(m)) => put_msg(out, 3, m),
            Some(OpRequest::Txn(m)) => put_msg(out, 4, m),
            None => {}
        }
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        self.merge_at(buf, 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResponse {
    Range(RangeResponse),
    Put(PutResponse),
    DeleteRange(DeleteRangeResponse),
    Txn(TxnResponse),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResponseOp {
    pub response: Option<OpResponse>,
}

impl ResponseOp {
    fn merge_at(&mut self, buf: &[u8], depth: usize) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match (n, &mut self.response) {
                (1, Some(OpResponse::Range(m))) => m.merge(v.bytes(n)?)?,
                (1, slot) => *slot = Some(OpResponse::Range(Message::decode(v.bytes(n)?)?)),
                (2, Some(OpResponse::Put(m))) => m.merge(v.bytes(n)?)?,
                (2, slot) => *slot = Some(OpResponse::Put(Message::decode(v.bytes(n)?)?)),
                (3, Some(OpResponse::DeleteRange(m))) => m.merge(v.bytes(n)?)?,
                (3, slot) => *slot = Some(OpResponse::DeleteRange(Message::decode(v.bytes(n)?)?)),
                (4, slot) => {
                    if depth >= MAX_TXN_DEPTH {
                        return Err(DecodeError::Overflow);
                    }
                    if !matches!(slot, Some(OpResponse::Txn(_))) {
                        *slot = Some(OpResponse::Txn(TxnResponse::default()));
                    }
                    if let Some(OpResponse::Txn(m)) = slot {
                        m.merge_at(v.bytes(n)?, depth + 1)?;
                    }
                }
                _ => {}
            }
            Ok(())
        })
    }
}

impl Message for ResponseOp {
    fn write(&self, out: &mut Vec<u8>) {
        match &self.response {
            Some(OpResponse::Range(m)) => put_msg(out, 1, m),
            Some(OpResponse::Put(m)) => put_msg(out, 2, m),
            Some(OpResponse::DeleteRange(m)) => put_msg(out, 3, m),
            Some(OpResponse::Txn(m)) => put_msg(out, 4, m),
            None => {}
        }
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        self.merge_at(buf, 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetUnion {
    Version(i64),
    CreateRevision(i64),
    ModRevision(i64),
    Value(Vec<u8>),
    Lease(i64),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Compare {
    pub result: CompareResult,
    pub target: CompareTarget,
    pub key: Vec<u8>,
    pub target_union: Option<TargetUnion>,
    pub range_end: Vec<u8>,
}

impl Message for Compare {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.result.into());
        put_enum(out, 2, self.target.into());
        put_bytes(out, 3, &self.key);
        match &self.target_union {
            Some(TargetUnion::Version(x)) => put_oneof_int(out, 4, *x),
            Some(TargetUnion::CreateRevision(x)) => put_oneof_int(out, 5, *x),
            Some(TargetUnion::ModRevision(x)) => put_oneof_int(out, 6, *x),
            Some(TargetUnion::Value(x)) => put_message(out, 7, x),
            Some(TargetUnion::Lease(x)) => put_oneof_int(out, 8, *x),
            None => {}
        }
        put_bytes(out, 64, &self.range_end);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.result = enum_value(v, n)?.into(),
                2 => self.target = enum_value(v, n)?.into(),
                3 => self.key = bytes(v, n)?,
                4 => self.target_union = Some(TargetUnion::Version(v.int(n)?)),
                5 => self.target_union = Some(TargetUnion::CreateRevision(v.int(n)?)),
                6 => self.target_union = Some(TargetUnion::ModRevision(v.int(n)?)),
                7 => self.target_union = Some(TargetUnion::Value(bytes(v, n)?)),
                8 => self.target_union = Some(TargetUnion::Lease(v.int(n)?)),
                64 => self.range_end = bytes(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxnRequest {
    pub compare: Vec<Compare>,
    pub success: Vec<RequestOp>,
    pub failure: Vec<RequestOp>,
}

impl TxnRequest {
    fn merge_at(&mut self, buf: &[u8], depth: usize) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => push_msg(&mut self.compare, v.bytes(n)?)?,
                2 | 3 => {
                    let mut op = RequestOp::default();
                    op.merge_at(v.bytes(n)?, depth)?;
                    if n == 2 {
                        self.success.push(op);
                    } else {
                        self.failure.push(op);
                    }
                }
                _ => {}
            }
            Ok(())
        })
    }
}

impl Message for TxnRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_msgs(out, 1, &self.compare);
        put_msgs(out, 2, &self.success);
        put_msgs(out, 3, &self.failure);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        self.merge_at(buf, 0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxnResponse {
    pub header: Option<ResponseHeader>,
    pub succeeded: bool,
    pub responses: Vec<ResponseOp>,
}

impl TxnResponse {
    fn merge_at(&mut self, buf: &[u8], depth: usize) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => self.succeeded = v.bool(n)?,
                3 => {
                    let mut op = ResponseOp::default();
                    op.merge_at(v.bytes(n)?, depth)?;
                    self.responses.push(op);
                }
                _ => {}
            }
            Ok(())
        })
    }
}

impl Message for TxnResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_bool(out, 2, self.succeeded);
        put_msgs(out, 3, &self.responses);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        self.merge_at(buf, 0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionRequest {
    pub revision: i64,
    pub physical: bool,
}

impl Message for CompactionRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.revision);
        put_bool(out, 2, self.physical);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.revision = v.int(n)?,
                2 => self.physical = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionResponse {
    pub header: Option<ResponseHeader>,
}

impl Message for CompactionResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.header, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchRequestUnion {
    Create(WatchCreateRequest),
    Cancel(WatchCancelRequest),
    Progress(WatchProgressRequest),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchRequest {
    pub request_union: Option<WatchRequestUnion>,
}

impl Message for WatchRequest {
    fn write(&self, out: &mut Vec<u8>) {
        match &self.request_union {
            Some(WatchRequestUnion::Create(m)) => put_msg(out, 1, m),
            Some(WatchRequestUnion::Cancel(m)) => put_msg(out, 2, m),
            Some(WatchRequestUnion::Progress(m)) => put_msg(out, 3, m),
            None => {}
        }
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match (n, &mut self.request_union) {
                (1, Some(WatchRequestUnion::Create(m))) => m.merge(v.bytes(n)?)?,
                (1, slot) => *slot = Some(WatchRequestUnion::Create(Message::decode(v.bytes(n)?)?)),
                (2, Some(WatchRequestUnion::Cancel(m))) => m.merge(v.bytes(n)?)?,
                (2, slot) => *slot = Some(WatchRequestUnion::Cancel(Message::decode(v.bytes(n)?)?)),
                (3, slot) => {
                    *slot = Some(WatchRequestUnion::Progress(Message::decode(v.bytes(n)?)?))
                }
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchCreateRequest {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub start_revision: i64,
    pub progress_notify: bool,
    pub filters: Vec<FilterType>,
    pub prev_kv: bool,
    pub watch_id: i64,
    pub fragment: bool,
}

impl Message for WatchCreateRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_bytes(out, 1, &self.key);
        put_bytes(out, 2, &self.range_end);
        put_int(out, 3, self.start_revision);
        put_bool(out, 4, self.progress_notify);
        put_packed_enums(out, 5, self.filters.iter().map(|&f| f.into()));
        put_bool(out, 6, self.prev_kv);
        put_int(out, 7, self.watch_id);
        put_bool(out, 8, self.fragment);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.key = bytes(v, n)?,
                2 => self.range_end = bytes(v, n)?,
                3 => self.start_revision = v.int(n)?,
                4 => self.progress_notify = v.bool(n)?,
                5 => push_enums(&mut self.filters, v, n)?,
                6 => self.prev_kv = v.bool(n)?,
                7 => self.watch_id = v.int(n)?,
                8 => self.fragment = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchCancelRequest {
    pub watch_id: i64,
}

impl Message for WatchCancelRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.watch_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.watch_id = v.int(n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchProgressRequest {}

impl Message for WatchProgressRequest {
    fn write(&self, _out: &mut Vec<u8>) {}

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |_, _| Ok(()))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WatchResponse {
    pub header: Option<ResponseHeader>,
    pub watch_id: i64,
    pub created: bool,
    pub canceled: bool,
    pub compact_revision: i64,
    pub cancel_reason: String,
    pub fragment: bool,
    pub events: Vec<Event>,
}

impl Message for WatchResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_int(out, 2, self.watch_id);
        put_bool(out, 3, self.created);
        put_bool(out, 4, self.canceled);
        put_int(out, 5, self.compact_revision);
        put_string(out, 6, &self.cancel_reason);
        put_bool(out, 7, self.fragment);
        put_msgs(out, 11, &self.events);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => self.watch_id = v.int(n)?,
                3 => self.created = v.bool(n)?,
                4 => self.canceled = v.bool(n)?,
                5 => self.compact_revision = v.int(n)?,
                6 => self.cancel_reason = string(v, n)?,
                7 => self.fragment = v.bool(n)?,
                11 => push_msg(&mut self.events, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseGrantRequest {
    pub ttl: i64,
    pub id: i64,
}

impl Message for LeaseGrantRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.ttl);
        put_int(out, 2, self.id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.ttl = v.int(n)?,
                2 => self.id = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseGrantResponse {
    pub header: Option<ResponseHeader>,
    pub id: i64,
    pub ttl: i64,
    pub error: String,
}

impl Message for LeaseGrantResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_int(out, 2, self.id);
        put_int(out, 3, self.ttl);
        put_string(out, 4, &self.error);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => self.id = v.int(n)?,
                3 => self.ttl = v.int(n)?,
                4 => self.error = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseRevokeRequest {
    pub id: i64,
}

impl Message for LeaseRevokeRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.id = v.int(n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseRevokeResponse {
    pub header: Option<ResponseHeader>,
}

impl Message for LeaseRevokeResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.header, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseKeepAliveRequest {
    pub id: i64,
}

impl Message for LeaseKeepAliveRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.id = v.int(n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseKeepAliveResponse {
    pub header: Option<ResponseHeader>,
    pub id: i64,
    pub ttl: i64,
}

impl Message for LeaseKeepAliveResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_int(out, 2, self.id);
        put_int(out, 3, self.ttl);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => self.id = v.int(n)?,
                3 => self.ttl = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseTimeToLiveRequest {
    pub id: i64,
    pub keys: bool,
}

impl Message for LeaseTimeToLiveRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.id);
        put_bool(out, 2, self.keys);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = v.int(n)?,
                2 => self.keys = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseTimeToLiveResponse {
    pub header: Option<ResponseHeader>,
    pub id: i64,
    pub ttl: i64,
    pub granted_ttl: i64,
    pub keys: Vec<Vec<u8>>,
}

impl Message for LeaseTimeToLiveResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_int(out, 2, self.id);
        put_int(out, 3, self.ttl);
        put_int(out, 4, self.granted_ttl);
        put_each_bytes(out, 5, &self.keys);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => self.id = v.int(n)?,
                3 => self.ttl = v.int(n)?,
                4 => self.granted_ttl = v.int(n)?,
                5 => self.keys.push(bytes(v, n)?),
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemberListRequest {
    pub linearizable: bool,
}

impl Message for MemberListRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_bool(out, 1, self.linearizable);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.linearizable = v.bool(n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Member {
    pub id: u64,
    pub name: String,
    pub peer_urls: Vec<String>,
    pub client_urls: Vec<String>,
    pub is_learner: bool,
}

impl Message for Member {
    fn write(&self, out: &mut Vec<u8>) {
        put_uint(out, 1, self.id);
        put_string(out, 2, &self.name);
        put_each_bytes(out, 3, &self.peer_urls);
        put_each_bytes(out, 4, &self.client_urls);
        put_bool(out, 5, self.is_learner);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = v.uint(n)?,
                2 => self.name = string(v, n)?,
                3 => self.peer_urls.push(string(v, n)?),
                4 => self.client_urls.push(string(v, n)?),
                5 => self.is_learner = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemberListResponse {
    pub header: Option<ResponseHeader>,
    pub members: Vec<Member>,
}

impl Message for MemberListResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_msgs(out, 2, &self.members);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => push_msg(&mut self.members, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusRequest {}

impl Message for StatusRequest {
    fn write(&self, _out: &mut Vec<u8>) {}

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |_, _| Ok(()))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DowngradeInfo {
    pub enabled: bool,
    pub target_version: String,
}

impl Message for DowngradeInfo {
    fn write(&self, out: &mut Vec<u8>) {
        put_bool(out, 1, self.enabled);
        put_string(out, 2, &self.target_version);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.enabled = v.bool(n)?,
                2 => self.target_version = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusResponse {
    pub header: Option<ResponseHeader>,
    pub version: String,
    pub db_size: i64,
    pub leader: u64,
    pub raft_index: u64,
    pub raft_term: u64,
    pub raft_applied_index: u64,
    pub errors: Vec<String>,
    pub db_size_in_use: i64,
    pub is_learner: bool,
    pub storage_version: String,
    pub db_size_quota: i64,
    pub downgrade_info: Option<DowngradeInfo>,
}

impl Message for StatusResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.header);
        put_string(out, 2, &self.version);
        put_int(out, 3, self.db_size);
        put_uint(out, 4, self.leader);
        put_uint(out, 5, self.raft_index);
        put_uint(out, 6, self.raft_term);
        put_uint(out, 7, self.raft_applied_index);
        put_each_bytes(out, 8, &self.errors);
        put_int(out, 9, self.db_size_in_use);
        put_bool(out, 10, self.is_learner);
        put_string(out, 11, &self.storage_version);
        put_int(out, 12, self.db_size_quota);
        put_opt(out, 13, &self.downgrade_info);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.header, v.bytes(n)?)?,
                2 => self.version = string(v, n)?,
                3 => self.db_size = v.int(n)?,
                4 => self.leader = v.uint(n)?,
                5 => self.raft_index = v.uint(n)?,
                6 => self.raft_term = v.uint(n)?,
                7 => self.raft_applied_index = v.uint(n)?,
                8 => self.errors.push(string(v, n)?),
                9 => self.db_size_in_use = v.int(n)?,
                10 => self.is_learner = v.bool(n)?,
                11 => self.storage_version = string(v, n)?,
                12 => self.db_size_quota = v.int(n)?,
                13 => merge_opt(&mut self.downgrade_info, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HealthCheckRequest {
    pub service: String,
}

impl Message for HealthCheckRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.service);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.service = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HealthCheckResponse {
    pub status: ServingStatus,
}

impl Message for HealthCheckResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.status.into());
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.status = enum_value(v, n)?.into();
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    fn round_trip<M: Message + PartialEq + Debug>(m: M) {
        let bytes = m.encode();
        assert_eq!(M::decode(&bytes).as_ref(), Ok(&m), "{bytes:02x?}");
    }

    fn header() -> Option<ResponseHeader> {
        Some(ResponseHeader {
            cluster_id: 0xdead_beef,
            member_id: 1,
            revision: 1234,
            raft_term: 2,
        })
    }

    fn kv(key: &[u8], value: &[u8], create: i64, modified: i64, version: i64) -> KeyValue {
        KeyValue {
            key: key.to_vec(),
            create_revision: create,
            mod_revision: modified,
            version,
            value: value.to_vec(),
            lease: 0,
        }
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    const POD: &[u8] = b"/registry/pods/default/web-0";

    fn create_txn() -> TxnRequest {
        TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal,
                target: CompareTarget::Mod,
                key: POD.to_vec(),
                target_union: Some(TargetUnion::ModRevision(0)),
                range_end: vec![],
            }],
            success: vec![RequestOp {
                request: Some(OpRequest::Put(PutRequest {
                    key: POD.to_vec(),
                    value: b"k8s\x00pod".to_vec(),
                    lease: 7,
                    ..Default::default()
                })),
            }],
            failure: vec![RequestOp {
                request: Some(OpRequest::Range(RangeRequest {
                    key: POD.to_vec(),
                    ..Default::default()
                })),
            }],
        }
    }

    fn watch_create() -> WatchCreateRequest {
        WatchCreateRequest {
            key: b"/registry/".to_vec(),
            range_end: b"/registry0".to_vec(),
            start_revision: 100,
            progress_notify: true,
            filters: vec![FilterType::NoPut],
            prev_kv: true,
            ..Default::default()
        }
    }

    fn range_response() -> RangeResponse {
        let mut b = kv(b"/b", b"two", 6, 6, 1);
        b.lease = 300;
        RangeResponse {
            header: header(),
            kvs: vec![kv(b"/a", b"one", 5, 9, 3), b],
            more: true,
            count: 17,
        }
    }

    fn watch_delete() -> WatchResponse {
        WatchResponse {
            header: header(),
            watch_id: 3,
            events: vec![Event {
                kind: EventType::Delete,
                kv: Some(KeyValue {
                    key: b"/a".to_vec(),
                    mod_revision: 1235,
                    ..Default::default()
                }),
                prev_kv: Some(kv(b"/a", b"one", 5, 9, 3)),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn every_message_round_trips() {
        round_trip(watch_delete());
        round_trip(RangeRequest {
            key: b"/registry/".to_vec(),
            range_end: b"/registry0".to_vec(),
            limit: 500,
            revision: -1,
            sort_order: SortOrder::Descend,
            sort_target: SortTarget::Other(9),
            serializable: true,
            keys_only: true,
            count_only: true,
            min_mod_revision: 1,
            max_mod_revision: 2,
            min_create_revision: 3,
            max_create_revision: 4,
        });
        round_trip(PutResponse {
            header: header(),
            prev_kv: Some(KeyValue::default()),
        });
        round_trip(PutRequest {
            key: b"k".to_vec(),
            value: vec![0xff, 0],
            lease: i64::MAX,
            prev_kv: true,
            ignore_value: true,
            ignore_lease: true,
        });
        round_trip(DeleteRangeResponse {
            header: header(),
            deleted: 2,
            prev_kvs: vec![kv(b"/a", b"", 1, 1, 1), kv(b"/b", b"x", 2, 3, 2)],
        });
        let compares = [
            TargetUnion::Version(0),
            TargetUnion::CreateRevision(-5),
            TargetUnion::ModRevision(9),
            TargetUnion::Value(vec![]),
            TargetUnion::Lease(4),
        ];
        let nested = TxnRequest {
            compare: compares
                .into_iter()
                .map(|t| Compare {
                    result: CompareResult::NotEqual,
                    target: CompareTarget::Lease,
                    key: b"k".to_vec(),
                    target_union: Some(t),
                    range_end: b"l".to_vec(),
                })
                .collect(),
            success: vec![
                RequestOp {
                    request: Some(OpRequest::DeleteRange(DeleteRangeRequest {
                        key: b"/a".to_vec(),
                        range_end: b"/b".to_vec(),
                        prev_kv: true,
                    })),
                },
                RequestOp {
                    request: Some(OpRequest::Txn(create_txn())),
                },
                RequestOp::default(),
            ],
            failure: vec![],
        };
        round_trip(nested);
        round_trip(TxnResponse {
            header: header(),
            succeeded: true,
            responses: vec![
                ResponseOp {
                    response: Some(OpResponse::Range(range_response())),
                },
                ResponseOp {
                    response: Some(OpResponse::Put(PutResponse::default())),
                },
                ResponseOp {
                    response: Some(OpResponse::DeleteRange(DeleteRangeResponse {
                        deleted: 1,
                        ..Default::default()
                    })),
                },
                ResponseOp {
                    response: Some(OpResponse::Txn(TxnResponse {
                        succeeded: true,
                        ..Default::default()
                    })),
                },
            ],
        });
        round_trip(CompactionRequest {
            revision: 77,
            physical: true,
        });
        round_trip(CompactionResponse { header: header() });
        let mut multi_filter = watch_create();
        multi_filter.filters = vec![
            FilterType::NoDelete,
            FilterType::NoPut,
            FilterType::Other(-1),
        ];
        multi_filter.watch_id = 12;
        multi_filter.fragment = true;
        for union in [
            WatchRequestUnion::Create(multi_filter),
            WatchRequestUnion::Cancel(WatchCancelRequest { watch_id: 12 }),
            WatchRequestUnion::Progress(WatchProgressRequest {}),
        ] {
            round_trip(WatchRequest {
                request_union: Some(union),
            });
        }
        round_trip(WatchResponse {
            header: header(),
            watch_id: 4,
            created: true,
            canceled: true,
            compact_revision: 50,
            cancel_reason: "mvcc: required revision has been compacted".into(),
            fragment: true,
            events: vec![Event {
                kind: EventType::Put,
                kv: Some(kv(b"/c", b"v", 10, 10, 1)),
                prev_kv: None,
            }],
        });
        round_trip(LeaseGrantRequest { ttl: 60, id: 0 });
        round_trip(LeaseGrantResponse {
            header: header(),
            id: 60,
            ttl: 60,
            error: "none".into(),
        });
        round_trip(LeaseRevokeRequest { id: 5 });
        round_trip(LeaseRevokeResponse { header: header() });
        round_trip(LeaseKeepAliveRequest { id: 5 });
        round_trip(LeaseKeepAliveResponse {
            header: header(),
            id: 5,
            ttl: 30,
        });
        round_trip(LeaseTimeToLiveRequest { id: 5, keys: true });
        round_trip(LeaseTimeToLiveResponse {
            header: header(),
            id: 5,
            ttl: 29,
            granted_ttl: 30,
            keys: vec![b"/a".to_vec(), vec![]],
        });
        round_trip(MemberListRequest { linearizable: true });
        round_trip(MemberListResponse {
            header: header(),
            members: vec![Member {
                id: u64::MAX,
                name: "quantykube".into(),
                peer_urls: vec![],
                client_urls: vec!["https://127.0.0.1:2379".into(), String::new()],
                is_learner: true,
            }],
        });
        round_trip(StatusRequest {});
        round_trip(StatusResponse {
            header: header(),
            version: "3.6.5".into(),
            db_size: 1 << 20,
            leader: 1,
            raft_index: 2,
            raft_term: 3,
            raft_applied_index: 4,
            errors: vec!["NOSPACE".into()],
            db_size_in_use: 1 << 19,
            is_learner: true,
            storage_version: "3.6.0".into(),
            db_size_quota: 1 << 31,
            downgrade_info: Some(DowngradeInfo {
                enabled: true,
                target_version: "3.5.0".into(),
            }),
        });
        round_trip(HealthCheckRequest {
            service: "etcdserverpb.KV".into(),
        });
        round_trip(HealthCheckResponse {
            status: ServingStatus::Serving,
        });
    }

    // Produced by the Python protobuf runtime (protobuf 7.36) from the etcd
    // v3.6.5 .proto files.
    const TXN: &str = "0a2210021a1c2f72656769737472792f706f64732f64656661756c742f7765622d30\
        3000122b12290a1c2f72656769737472792f706f64732f64656661756c742f7765622d30\
        12076b387300706f6418071a200a1e0a1c2f72656769737472792f706f64732f64656661\
        756c742f7765622d30";
    const WATCH_CREATE: &str =
        "0a210a0a2f72656769737472792f120a2f726567697374727930186420012a01003001";
    const RANGE_RESPONSE: &str = "0a0d08effdb6f50d100118d2092002120f0a022f611005180920032a036f\
        6e6512120a022f621006180620012a0374776f30ac0218012011";
    const WATCH_DELETE: &str = "0a0d08effdb6f50d100118d209200210035a1c080112070a022f6118d309\
        1a0f0a022f611005180920032a036f6e65";

    fn golden<M: Message + PartialEq + Debug>(m: &M, hex: &str) {
        let want = unhex(hex);
        assert_eq!(m.encode(), want);
        assert_eq!(M::decode(&want).as_ref(), Ok(m));
    }

    #[test]
    fn bytes_match_the_reference_implementation() {
        golden(&create_txn(), TXN);
        let watch = WatchRequest {
            request_union: Some(WatchRequestUnion::Create(watch_create())),
        };
        golden(&watch, WATCH_CREATE);
        golden(&range_response(), RANGE_RESPONSE);
        golden(&watch_delete(), WATCH_DELETE);
    }

    #[test]
    fn truncated_input_is_an_error() {
        for hex in [TXN, WATCH_CREATE, RANGE_RESPONSE, WATCH_DELETE] {
            let full = unhex(hex);
            for cut in 0..full.len() {
                // A cut at a field boundary is a valid shorter message, so
                // only the absence of a panic is checked here.
                let _ = TxnRequest::decode(&full[..cut]);
                let _ = WatchRequest::decode(&full[..cut]);
                let _ = RangeResponse::decode(&full[..cut]);
                let _ = WatchResponse::decode(&full[..cut]);
            }
        }
        let txn = unhex(TXN);
        assert_eq!(
            TxnRequest::decode(&txn[..txn.len() - 1]),
            Err(DecodeError::Truncated)
        );
        // The outer length is intact but the Compare inside ends early.
        let mut inner_cut = vec![0x0a, 0x03];
        inner_cut.extend([0x1a, 0x05, b'k']);
        assert_eq!(TxnRequest::decode(&inner_cut), Err(DecodeError::Truncated));
        let mut deep = Vec::new();
        for _ in 0..1000 {
            let mut op = Vec::new();
            put_message(&mut op, 4, &deep);
            deep.clear();
            put_message(&mut deep, 2, &op);
        }
        assert!(TxnRequest::decode(&deep).is_err());
    }

    #[test]
    fn decoding_is_lenient_where_protobuf_requires_it() {
        let mut bytes = Vec::new();
        put_bytes(&mut bytes, 1, b"/a");
        // Unpacked filters, then a packed run, as an older writer might mix.
        put_uint(&mut bytes, 5, 1);
        put_message(&mut bytes, 5, &[0x00, 0x07]);
        put_uint(&mut bytes, 99, 5);
        put_bytes(&mut bytes, 100, b"future");
        bytes.extend([0xad, 0x06, 1, 2, 3, 4]);
        let got = WatchCreateRequest::decode(&bytes).unwrap();
        assert_eq!(got.key, b"/a");
        assert_eq!(
            got.filters,
            [
                FilterType::NoDelete,
                FilterType::NoPut,
                FilterType::Other(7)
            ]
        );
    }
}
