//! Raft messages on the wire between store nodes, as protobuf fields so
//! that a field can be added later without breaking older nodes.

use super::{Body, Entry, Message};
use quantycore::proto::{put_bool, put_bytes, put_message, put_uint, DecodeError, Reader};

const VOTE: u64 = 1;
const VOTE_REPLY: u64 = 2;
const APPEND: u64 = 3;
const APPEND_REPLY: u64 = 4;
const READ_INDEX: u64 = 5;
const READ_INDEX_REPLY: u64 = 6;
const READ_CHECK: u64 = 7;
const READ_CHECK_REPLY: u64 = 8;
const SNAPSHOT: u64 = 9;
const PROPOSE: u64 = 10;

fn encode_entry(e: &Entry) -> Vec<u8> {
    let mut out = Vec::new();
    put_uint(&mut out, 1, e.term);
    put_uint(&mut out, 2, e.index);
    put_bytes(&mut out, 3, &e.data);
    out
}

fn decode_entry(buf: &[u8]) -> Result<Entry, DecodeError> {
    let mut e = Entry {
        term: 0,
        index: 0,
        data: Vec::new(),
    };
    let mut r = Reader::new(buf);
    while let Some((n, v)) = r.next_field()? {
        match n {
            1 => e.term = v.uint(n)?,
            2 => e.index = v.uint(n)?,
            3 => e.data = v.bytes(n)?.to_vec(),
            _ => {}
        }
    }
    Ok(e)
}

pub fn encode(m: &Message) -> Vec<u8> {
    let mut out = Vec::new();
    put_uint(&mut out, 1, m.from);
    put_uint(&mut out, 2, m.to);
    put_uint(&mut out, 3, m.term);
    match &m.body {
        Body::Vote {
            pre,
            last_index,
            last_term,
        } => {
            put_uint(&mut out, 4, VOTE);
            put_bool(&mut out, 10, *pre);
            put_uint(&mut out, 11, *last_index);
            put_uint(&mut out, 12, *last_term);
        }
        Body::VoteReply { pre, granted } => {
            put_uint(&mut out, 4, VOTE_REPLY);
            put_bool(&mut out, 10, *pre);
            put_bool(&mut out, 13, *granted);
        }
        Body::Append {
            prev_index,
            prev_term,
            entries,
            commit,
        } => {
            put_uint(&mut out, 4, APPEND);
            put_uint(&mut out, 14, *prev_index);
            put_uint(&mut out, 15, *prev_term);
            for e in entries {
                put_message(&mut out, 16, &encode_entry(e));
            }
            put_uint(&mut out, 17, *commit);
        }
        Body::AppendReply { index, success } => {
            put_uint(&mut out, 4, APPEND_REPLY);
            put_uint(&mut out, 18, *index);
            put_bool(&mut out, 19, *success);
        }
        Body::ReadIndex { req } => {
            put_uint(&mut out, 4, READ_INDEX);
            put_uint(&mut out, 20, *req);
        }
        Body::ReadIndexReply { req, index } => {
            put_uint(&mut out, 4, READ_INDEX_REPLY);
            put_uint(&mut out, 20, *req);
            put_uint(&mut out, 18, *index);
        }
        Body::ReadCheck { seq } => {
            put_uint(&mut out, 4, READ_CHECK);
            put_uint(&mut out, 21, *seq);
        }
        Body::ReadCheckReply { seq } => {
            put_uint(&mut out, 4, READ_CHECK_REPLY);
            put_uint(&mut out, 21, *seq);
        }
        Body::Snapshot { index, term, data } => {
            put_uint(&mut out, 4, SNAPSHOT);
            put_uint(&mut out, 18, *index);
            put_uint(&mut out, 12, *term);
            put_bytes(&mut out, 22, data);
        }
        Body::Propose { data } => {
            put_uint(&mut out, 4, PROPOSE);
            put_bytes(&mut out, 22, data);
        }
    }
    out
}

pub fn decode(buf: &[u8]) -> Result<Message, DecodeError> {
    let (mut from, mut to, mut term, mut kind) = (0, 0, 0, 0);
    let (mut pre, mut granted, mut success) = (false, false, false);
    let (mut last_index, mut last_term, mut prev_index, mut prev_term) = (0, 0, 0, 0);
    let (mut commit, mut index, mut req, mut seq) = (0, 0, 0, 0);
    let mut entries = Vec::new();
    let mut data = Vec::new();
    let mut r = Reader::new(buf);
    while let Some((n, v)) = r.next_field()? {
        match n {
            1 => from = v.uint(n)?,
            2 => to = v.uint(n)?,
            3 => term = v.uint(n)?,
            4 => kind = v.uint(n)?,
            10 => pre = v.bool(n)?,
            11 => last_index = v.uint(n)?,
            12 => last_term = v.uint(n)?,
            13 => granted = v.bool(n)?,
            14 => prev_index = v.uint(n)?,
            15 => prev_term = v.uint(n)?,
            16 => entries.push(decode_entry(v.bytes(n)?)?),
            17 => commit = v.uint(n)?,
            18 => index = v.uint(n)?,
            19 => success = v.bool(n)?,
            20 => req = v.uint(n)?,
            21 => seq = v.uint(n)?,
            22 => data = v.bytes(n)?.to_vec(),
            _ => {}
        }
    }
    let body = match kind {
        VOTE => Body::Vote {
            pre,
            last_index,
            last_term,
        },
        VOTE_REPLY => Body::VoteReply { pre, granted },
        APPEND => Body::Append {
            prev_index,
            prev_term,
            entries,
            commit,
        },
        APPEND_REPLY => Body::AppendReply { index, success },
        READ_INDEX => Body::ReadIndex { req },
        READ_INDEX_REPLY => Body::ReadIndexReply { req, index },
        READ_CHECK => Body::ReadCheck { seq },
        READ_CHECK_REPLY => Body::ReadCheckReply { seq },
        SNAPSHOT => Body::Snapshot {
            index,
            term: last_term,
            data: std::mem::take(&mut data),
        },
        PROPOSE => Body::Propose { data },
        _ => return Err(DecodeError::Truncated),
    };
    Ok(Message {
        from,
        to,
        term,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_message_comes_back_as_it_went() {
        let e = |i| Entry {
            term: 7,
            index: i,
            data: vec![i as u8; 3],
        };
        let bodies = [
            Body::Vote {
                pre: true,
                last_index: 9,
                last_term: 4,
            },
            Body::VoteReply {
                pre: false,
                granted: true,
            },
            Body::Append {
                prev_index: 3,
                prev_term: 2,
                entries: vec![e(4), e(5)],
                commit: 3,
            },
            Body::Append {
                prev_index: 0,
                prev_term: 0,
                entries: vec![Entry {
                    term: 1,
                    index: 1,
                    data: Vec::new(),
                }],
                commit: 0,
            },
            Body::AppendReply {
                index: 12,
                success: false,
            },
            Body::ReadIndex { req: 5 },
            Body::ReadIndexReply { req: 5, index: 40 },
            Body::ReadCheck { seq: 3 },
            Body::ReadCheckReply { seq: 3 },
            Body::Snapshot {
                index: 99,
                term: 8,
                data: b"state".to_vec(),
            },
            Body::Propose {
                data: b"put".to_vec(),
            },
        ];
        for body in bodies {
            let m = Message {
                from: 1,
                to: 3,
                term: 8,
                body,
            };
            assert_eq!(decode(&encode(&m)).unwrap(), m);
        }
        assert!(decode(&[]).is_err());
    }
}
