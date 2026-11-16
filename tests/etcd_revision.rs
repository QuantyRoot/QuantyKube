//! Against a real etcd, when QK_ETCD names one as `host:port pki-dir`
//! (ca.crt, s.crt, s.key there): replace_all moves etcd's revision past
//! the copy's quickly, on one connection. CI's etcd job sets it.

use quantykube::etcdclient::{Client, Copy};
use quantykube::store::Record;
use std::path::Path;
use std::time::Instant;

#[test]
fn etcd_s_revision_is_moved_past_the_copy_s() {
    let Ok(spec) = std::env::var("QK_ETCD") else {
        eprintln!("QK_ETCD is not set; skipped");
        return;
    };
    let (addr, dir) = spec.split_once(' ').expect("host:port pki-dir");
    let dir = Path::new(dir);
    let c = Client::new(addr, &dir.join("ca.crt"), &dir.join("s.crt"), &dir.join("s.key")).unwrap();
    let before = c.copy().unwrap().revision;
    let copy = Copy {
        revision: before + 50_000,
        records: vec![Record {
            key: b"/registry/configmaps/default/x".to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        }],
        leases: Default::default(),
    };
    let t = Instant::now();
    c.replace_all(&copy).unwrap();
    let took = t.elapsed();
    let after = c.copy().unwrap();
    eprintln!("50000 revisions in {took:?}");
    assert!(after.revision > copy.revision, "{} <= {}", after.revision, copy.revision);
    assert_eq!(after.records.len(), 1);
    assert_eq!(after.records[0].value, b"v");
    assert!(took.as_secs() < 60, "too slow: {took:?}");
}
