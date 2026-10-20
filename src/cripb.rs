//! The Container Runtime Interface messages (package `runtime.v1`, cri-api
//! a8dc9a1) behind the RuntimeService and ImageService calls the kubelet
//! makes, encoded and decoded by hand on top of `quantycore::proto`.
//!
//! Windows-specific fields are not modeled and are skipped like unknown
//! fields. No message here contains itself, directly or indirectly, so the
//! schema bounds how deeply a decoder can recurse.

use std::collections::BTreeMap;

use quantycore::proto::{
    decode_varint, encode_key, encode_varint, put_bool, put_bytes, put_int, put_message, put_uint,
    DecodeError, Reader, Value, WireType,
};

// Proto maps are kept sorted so encoding is deterministic, in key order.
pub type Map = BTreeMap<String, String>;

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

// int32, uint32 and enums keep the low 32 bits of the varint, as protobuf
// does.
fn int32(v: Value, n: u32) -> Result<i32, DecodeError> {
    v.uint(n).map(|x| x as i32)
}

fn uint32(v: Value, n: u32) -> Result<u32, DecodeError> {
    v.uint(n).map(|x| x as u32)
}

fn double(v: Value, n: u32) -> Result<f64, DecodeError> {
    match v {
        Value::I64(bits) => Ok(f64::from_bits(bits)),
        _ => Err(DecodeError::WrongWireType(n)),
    }
}

fn put_enum(out: &mut Vec<u8>, field: u32, v: i32) {
    put_int(out, field, i64::from(v));
}

fn put_string(out: &mut Vec<u8>, field: u32, s: &str) {
    put_bytes(out, field, s.as_bytes());
}

// Only +0.0 is the default: -0.0 has a sign bit set and is written.
fn put_double(out: &mut Vec<u8>, field: u32, v: f64) {
    if v.to_bits() != 0 {
        encode_key(field, WireType::I64, out);
        out.extend_from_slice(&v.to_le_bytes());
    }
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

// Elements of a repeated string field are written even when empty, which
// put_bytes would skip.
fn put_strings(out: &mut Vec<u8>, field: u32, items: &[String]) {
    for item in items {
        put_message(out, field, item.as_bytes());
    }
}

// A map is a repeated message of key = 1 and value = 2. Both are written
// even when empty, as the reference implementations do.
fn put_map(out: &mut Vec<u8>, field: u32, map: &Map) {
    for (k, v) in map {
        let mut entry = Vec::new();
        put_message(&mut entry, 1, k.as_bytes());
        put_message(&mut entry, 2, v.as_bytes());
        put_message(out, field, &entry);
    }
}

// A missing key or value is the empty string; a repeated key replaces the
// earlier entry.
fn merge_map(map: &mut Map, v: Value, n: u32) -> Result<(), DecodeError> {
    let mut key = String::new();
    let mut value = String::new();
    each_field(v.bytes(n)?, |f, x| {
        match f {
            1 => key = string(x, f)?,
            2 => value = string(x, f)?,
            _ => {}
        }
        Ok(())
    })?;
    map.insert(key, value);
    Ok(())
}

fn put_packed_ints(out: &mut Vec<u8>, field: u32, values: &[i64]) {
    if values.is_empty() {
        return;
    }
    let mut packed = Vec::new();
    for &v in values {
        encode_varint(v as u64, &mut packed);
    }
    put_message(out, field, &packed);
}

// Writers pack repeated scalars, but a parser must also accept them one
// varint field at a time, and both forms may be mixed.
fn push_ints(list: &mut Vec<i64>, v: Value, n: u32) -> Result<(), DecodeError> {
    match v {
        Value::Varint(x) => list.push(x as i64),
        Value::Len(mut packed) => {
            while !packed.is_empty() {
                let (x, used) = decode_varint(packed)?;
                list.push(x as i64);
                packed = &packed[used..];
            }
        }
        _ => return Err(DecodeError::WrongWireType(n)),
    }
    Ok(())
}

fn merge_opt<M: Message>(slot: &mut Option<M>, buf: &[u8]) -> Result<(), DecodeError> {
    slot.get_or_insert_with(M::default).merge(buf)
}

fn push_msg<M: Message>(list: &mut Vec<M>, buf: &[u8]) -> Result<(), DecodeError> {
    list.push(M::decode(buf)?);
    Ok(())
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

macro_rules! empty_message {
    ($($name:ident),+ $(,)?) => {
        $(
            #[derive(Debug, Clone, Default, PartialEq, Eq)]
            pub struct $name {}

            impl Message for $name {
                fn write(&self, _out: &mut Vec<u8>) {}

                fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
                    each_field(buf, |_, _| Ok(()))
                }
            }
        )+
    };
}

proto_enum!(Protocol {
    Tcp = 0,
    Udp = 1,
    Sctp = 2
});
proto_enum!(MountPropagation {
    Private = 0,
    HostToContainer = 1,
    Bidirectional = 2
});
proto_enum!(NamespaceMode {
    Pod = 0,
    Container = 1,
    Node = 2,
    Target = 3
});
proto_enum!(SupplementalGroupsPolicy {
    Merge = 0,
    Strict = 1
});
proto_enum!(ProfileType {
    RuntimeDefault = 0,
    Unconfined = 1,
    Localhost = 2
});
proto_enum!(PodSandboxState {
    Ready = 0,
    NotReady = 1
});
proto_enum!(ContainerState {
    Created = 0,
    Running = 1,
    Exited = 2,
    Unknown = 3
});
// The numbers are CRI's own, not Linux signal numbers.
proto_enum!(Signal {
    RuntimeDefault = 0,
    SigAbrt = 1,
    SigAlrm = 2,
    SigBus = 3,
    SigChld = 4,
    SigCld = 5,
    SigCont = 6,
    SigFpe = 7,
    SigHup = 8,
    SigIll = 9,
    SigInt = 10,
    SigIo = 11,
    SigIot = 12,
    SigKill = 13,
    SigPipe = 14,
    SigPoll = 15,
    SigProf = 16,
    SigPwr = 17,
    SigQuit = 18,
    SigSegv = 19,
    SigStkflt = 20,
    SigStop = 21,
    SigSys = 22,
    SigTerm = 23,
    SigTrap = 24,
    SigTstp = 25,
    SigTtin = 26,
    SigTtou = 27,
    SigUrg = 28,
    SigUsr1 = 29,
    SigUsr2 = 30,
    SigVtalrm = 31,
    SigWinch = 32,
    SigXcpu = 33,
    SigXfsz = 34,
    SigRtMin = 35,
    SigRtMinPlus1 = 36,
    SigRtMinPlus2 = 37,
    SigRtMinPlus3 = 38,
    SigRtMinPlus4 = 39,
    SigRtMinPlus5 = 40,
    SigRtMinPlus6 = 41,
    SigRtMinPlus7 = 42,
    SigRtMinPlus8 = 43,
    SigRtMinPlus9 = 44,
    SigRtMinPlus10 = 45,
    SigRtMinPlus11 = 46,
    SigRtMinPlus12 = 47,
    SigRtMinPlus13 = 48,
    SigRtMinPlus14 = 49,
    SigRtMinPlus15 = 50,
    SigRtMaxMinus14 = 51,
    SigRtMaxMinus13 = 52,
    SigRtMaxMinus12 = 53,
    SigRtMaxMinus11 = 54,
    SigRtMaxMinus10 = 55,
    SigRtMaxMinus9 = 56,
    SigRtMaxMinus8 = 57,
    SigRtMaxMinus7 = 58,
    SigRtMaxMinus6 = 59,
    SigRtMaxMinus5 = 60,
    SigRtMaxMinus4 = 61,
    SigRtMaxMinus3 = 62,
    SigRtMaxMinus2 = 63,
    SigRtMaxMinus1 = 64,
    SigRtMax = 65,
});

empty_message!(
    StopPodSandboxResponse,
    RemovePodSandboxResponse,
    StartContainerResponse,
    StopContainerResponse,
    RemoveContainerResponse,
    RemoveImageResponse,
    ImageFsInfoRequest,
);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionRequest {
    pub version: String,
}

impl Message for VersionRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.version);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.version = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionResponse {
    pub version: String,
    pub runtime_name: String,
    pub runtime_version: String,
    pub runtime_api_version: String,
}

impl Message for VersionResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.version);
        put_string(out, 2, &self.runtime_name);
        put_string(out, 3, &self.runtime_version);
        put_string(out, 4, &self.runtime_api_version);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.version = string(v, n)?,
                2 => self.runtime_name = string(v, n)?,
                3 => self.runtime_version = string(v, n)?,
                4 => self.runtime_api_version = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DnsConfig {
    pub servers: Vec<String>,
    pub searches: Vec<String>,
    pub options: Vec<String>,
}

impl Message for DnsConfig {
    fn write(&self, out: &mut Vec<u8>) {
        put_strings(out, 1, &self.servers);
        put_strings(out, 2, &self.searches);
        put_strings(out, 3, &self.options);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.servers.push(string(v, n)?),
                2 => self.searches.push(string(v, n)?),
                3 => self.options.push(string(v, n)?),
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PortMapping {
    pub protocol: Protocol,
    pub container_port: i32,
    pub host_port: i32,
    pub host_ip: String,
}

impl Message for PortMapping {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.protocol.into());
        put_int(out, 2, i64::from(self.container_port));
        put_int(out, 3, i64::from(self.host_port));
        put_string(out, 4, &self.host_ip);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.protocol = int32(v, n)?.into(),
                2 => self.container_port = int32(v, n)?,
                3 => self.host_port = int32(v, n)?,
                4 => self.host_ip = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IdMapping {
    pub host_id: u32,
    pub container_id: u32,
    pub length: u32,
}

impl Message for IdMapping {
    fn write(&self, out: &mut Vec<u8>) {
        put_uint(out, 1, u64::from(self.host_id));
        put_uint(out, 2, u64::from(self.container_id));
        put_uint(out, 3, u64::from(self.length));
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.host_id = uint32(v, n)?,
                2 => self.container_id = uint32(v, n)?,
                3 => self.length = uint32(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Mount {
    pub container_path: String,
    pub host_path: String,
    pub readonly: bool,
    pub selinux_relabel: bool,
    pub propagation: MountPropagation,
    pub uid_mappings: Vec<IdMapping>,
    pub gid_mappings: Vec<IdMapping>,
    /// Comes with readonly set and PRIVATE propagation.
    pub recursive_read_only: bool,
    /// Set for an image volume, which mounts an image instead of host_path;
    /// image.image is then the image_ref PullImage returned.
    pub image: Option<ImageSpec>,
    pub image_sub_path: String,
    pub mount_options: Vec<String>,
}

impl Message for Mount {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_path);
        put_string(out, 2, &self.host_path);
        put_bool(out, 3, self.readonly);
        put_bool(out, 4, self.selinux_relabel);
        put_enum(out, 5, self.propagation.into());
        put_msgs(out, 6, &self.uid_mappings);
        put_msgs(out, 7, &self.gid_mappings);
        put_bool(out, 8, self.recursive_read_only);
        put_opt(out, 9, &self.image);
        put_string(out, 10, &self.image_sub_path);
        put_strings(out, 11, &self.mount_options);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.container_path = string(v, n)?,
                2 => self.host_path = string(v, n)?,
                3 => self.readonly = v.bool(n)?,
                4 => self.selinux_relabel = v.bool(n)?,
                5 => self.propagation = int32(v, n)?.into(),
                6 => push_msg(&mut self.uid_mappings, v.bytes(n)?)?,
                7 => push_msg(&mut self.gid_mappings, v.bytes(n)?)?,
                8 => self.recursive_read_only = v.bool(n)?,
                9 => merge_opt(&mut self.image, v.bytes(n)?)?,
                10 => self.image_sub_path = string(v, n)?,
                11 => self.mount_options.push(string(v, n)?),
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserNamespace {
    pub mode: NamespaceMode,
    pub uids: Vec<IdMapping>,
    pub gids: Vec<IdMapping>,
}

impl Message for UserNamespace {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.mode.into());
        put_msgs(out, 2, &self.uids);
        put_msgs(out, 3, &self.gids);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.mode = int32(v, n)?.into(),
                2 => push_msg(&mut self.uids, v.bytes(n)?)?,
                3 => push_msg(&mut self.gids, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

// POD is 0, so it never appears on the wire: an absent mode means POD.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NamespaceOption {
    pub network: NamespaceMode,
    pub pid: NamespaceMode,
    pub ipc: NamespaceMode,
    /// The container whose PID namespace is joined when pid is TARGET.
    pub target_id: String,
    pub userns_options: Option<UserNamespace>,
}

impl Message for NamespaceOption {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.network.into());
        put_enum(out, 2, self.pid.into());
        put_enum(out, 3, self.ipc.into());
        put_string(out, 4, &self.target_id);
        put_opt(out, 5, &self.userns_options);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.network = int32(v, n)?.into(),
                2 => self.pid = int32(v, n)?.into(),
                3 => self.ipc = int32(v, n)?.into(),
                4 => self.target_id = string(v, n)?,
                5 => merge_opt(&mut self.userns_options, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Int64Value {
    pub value: i64,
}

impl Message for Int64Value {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.value);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.value = v.int(n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UInt64Value {
    pub value: u64,
}

impl Message for UInt64Value {
    fn write(&self, out: &mut Vec<u8>) {
        put_uint(out, 1, self.value);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.value = v.uint(n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeLinuxOption {
    pub user: String,
    pub role: String,
    pub kind: String,
    pub level: String,
}

impl Message for SeLinuxOption {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.user);
        put_string(out, 2, &self.role);
        put_string(out, 3, &self.kind);
        put_string(out, 4, &self.level);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.user = string(v, n)?,
                2 => self.role = string(v, n)?,
                3 => self.kind = string(v, n)?,
                4 => self.level = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SecurityProfile {
    pub profile_type: ProfileType,
    /// The profile's path or name when profile_type is Localhost.
    pub localhost_ref: String,
}

impl Message for SecurityProfile {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.profile_type.into());
        put_string(out, 2, &self.localhost_ref);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.profile_type = int32(v, n)?.into(),
                2 => self.localhost_ref = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

// run_as_user and run_as_group are wrapped so that an unset id (use the
// image's) differs from 0 (root).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinuxSandboxSecurityContext {
    pub namespace_options: Option<NamespaceOption>,
    pub selinux_options: Option<SeLinuxOption>,
    pub run_as_user: Option<Int64Value>,
    pub readonly_rootfs: bool,
    pub supplemental_groups: Vec<i64>,
    pub privileged: bool,
    /// Deprecated in favor of seccomp.
    pub seccomp_profile_path: String,
    pub run_as_group: Option<Int64Value>,
    pub seccomp: Option<SecurityProfile>,
    pub apparmor: Option<SecurityProfile>,
    pub supplemental_groups_policy: SupplementalGroupsPolicy,
}

impl Message for LinuxSandboxSecurityContext {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.namespace_options);
        put_opt(out, 2, &self.selinux_options);
        put_opt(out, 3, &self.run_as_user);
        put_bool(out, 4, self.readonly_rootfs);
        put_packed_ints(out, 5, &self.supplemental_groups);
        put_bool(out, 6, self.privileged);
        put_string(out, 7, &self.seccomp_profile_path);
        put_opt(out, 8, &self.run_as_group);
        put_opt(out, 9, &self.seccomp);
        put_opt(out, 10, &self.apparmor);
        put_enum(out, 11, self.supplemental_groups_policy.into());
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.namespace_options, v.bytes(n)?)?,
                2 => merge_opt(&mut self.selinux_options, v.bytes(n)?)?,
                3 => merge_opt(&mut self.run_as_user, v.bytes(n)?)?,
                4 => self.readonly_rootfs = v.bool(n)?,
                5 => push_ints(&mut self.supplemental_groups, v, n)?,
                6 => self.privileged = v.bool(n)?,
                7 => self.seccomp_profile_path = string(v, n)?,
                8 => merge_opt(&mut self.run_as_group, v.bytes(n)?)?,
                9 => merge_opt(&mut self.seccomp, v.bytes(n)?)?,
                10 => merge_opt(&mut self.apparmor, v.bytes(n)?)?,
                11 => self.supplemental_groups_policy = int32(v, n)?.into(),
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HugepageLimit {
    pub page_size: String,
    pub limit: u64,
}

impl Message for HugepageLimit {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.page_size);
        put_uint(out, 2, self.limit);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.page_size = string(v, n)?,
                2 => self.limit = v.uint(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinuxContainerResources {
    pub cpu_period: i64,
    pub cpu_quota: i64,
    pub cpu_shares: i64,
    pub memory_limit_in_bytes: i64,
    pub oom_score_adj: i64,
    pub cpuset_cpus: String,
    pub cpuset_mems: String,
    pub hugepage_limits: Vec<HugepageLimit>,
    /// cgroup v2 files and their contents, written as is.
    pub unified: Map,
    pub memory_swap_limit_in_bytes: i64,
}

impl Message for LinuxContainerResources {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.cpu_period);
        put_int(out, 2, self.cpu_quota);
        put_int(out, 3, self.cpu_shares);
        put_int(out, 4, self.memory_limit_in_bytes);
        put_int(out, 5, self.oom_score_adj);
        put_string(out, 6, &self.cpuset_cpus);
        put_string(out, 7, &self.cpuset_mems);
        put_msgs(out, 8, &self.hugepage_limits);
        put_map(out, 9, &self.unified);
        put_int(out, 10, self.memory_swap_limit_in_bytes);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.cpu_period = v.int(n)?,
                2 => self.cpu_quota = v.int(n)?,
                3 => self.cpu_shares = v.int(n)?,
                4 => self.memory_limit_in_bytes = v.int(n)?,
                5 => self.oom_score_adj = v.int(n)?,
                6 => self.cpuset_cpus = string(v, n)?,
                7 => self.cpuset_mems = string(v, n)?,
                8 => push_msg(&mut self.hugepage_limits, v.bytes(n)?)?,
                9 => merge_map(&mut self.unified, v, n)?,
                10 => self.memory_swap_limit_in_bytes = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinuxPodSandboxConfig {
    pub cgroup_parent: String,
    pub security_context: Option<LinuxSandboxSecurityContext>,
    pub sysctls: Map,
    pub overhead: Option<LinuxContainerResources>,
    pub resources: Option<LinuxContainerResources>,
}

impl Message for LinuxPodSandboxConfig {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.cgroup_parent);
        put_opt(out, 2, &self.security_context);
        put_map(out, 3, &self.sysctls);
        put_opt(out, 4, &self.overhead);
        put_opt(out, 5, &self.resources);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.cgroup_parent = string(v, n)?,
                2 => merge_opt(&mut self.security_context, v.bytes(n)?)?,
                3 => merge_map(&mut self.sysctls, v, n)?,
                4 => merge_opt(&mut self.overhead, v.bytes(n)?)?,
                5 => merge_opt(&mut self.resources, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxMetadata {
    pub name: String,
    pub uid: String,
    pub namespace: String,
    pub attempt: u32,
}

impl Message for PodSandboxMetadata {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.name);
        put_string(out, 2, &self.uid);
        put_string(out, 3, &self.namespace);
        put_uint(out, 4, u64::from(self.attempt));
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.name = string(v, n)?,
                2 => self.uid = string(v, n)?,
                3 => self.namespace = string(v, n)?,
                4 => self.attempt = uint32(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxConfig {
    pub metadata: Option<PodSandboxMetadata>,
    pub hostname: String,
    /// Container log paths are relative to this directory.
    pub log_directory: String,
    pub dns_config: Option<DnsConfig>,
    pub port_mappings: Vec<PortMapping>,
    pub labels: Map,
    /// Must be returned unchanged in PodSandboxStatus.
    pub annotations: Map,
    pub linux: Option<LinuxPodSandboxConfig>,
}

impl Message for PodSandboxConfig {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.metadata);
        put_string(out, 2, &self.hostname);
        put_string(out, 3, &self.log_directory);
        put_opt(out, 4, &self.dns_config);
        put_msgs(out, 5, &self.port_mappings);
        put_map(out, 6, &self.labels);
        put_map(out, 7, &self.annotations);
        put_opt(out, 8, &self.linux);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.metadata, v.bytes(n)?)?,
                2 => self.hostname = string(v, n)?,
                3 => self.log_directory = string(v, n)?,
                4 => merge_opt(&mut self.dns_config, v.bytes(n)?)?,
                5 => push_msg(&mut self.port_mappings, v.bytes(n)?)?,
                6 => merge_map(&mut self.labels, v, n)?,
                7 => merge_map(&mut self.annotations, v, n)?,
                8 => merge_opt(&mut self.linux, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunPodSandboxRequest {
    pub config: Option<PodSandboxConfig>,
    /// Empty selects the default handler; an unknown one must be rejected.
    pub runtime_handler: String,
}

impl Message for RunPodSandboxRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.config);
        put_string(out, 2, &self.runtime_handler);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.config, v.bytes(n)?)?,
                2 => self.runtime_handler = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunPodSandboxResponse {
    pub pod_sandbox_id: String,
}

impl Message for RunPodSandboxResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.pod_sandbox_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.pod_sandbox_id = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StopPodSandboxRequest {
    pub pod_sandbox_id: String,
}

impl Message for StopPodSandboxRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.pod_sandbox_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.pod_sandbox_id = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemovePodSandboxRequest {
    pub pod_sandbox_id: String,
}

impl Message for RemovePodSandboxRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.pod_sandbox_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.pod_sandbox_id = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxStatusRequest {
    pub pod_sandbox_id: String,
    pub verbose: bool,
}

impl Message for PodSandboxStatusRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.pod_sandbox_id);
        put_bool(out, 2, self.verbose);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.pod_sandbox_id = string(v, n)?,
                2 => self.verbose = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodIp {
    pub ip: String,
}

impl Message for PodIp {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.ip);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.ip = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxNetworkStatus {
    pub ip: String,
    /// Further addresses of the pod, such as the IPv6 one of a dual-stack
    /// pod; ip is the primary.
    pub additional_ips: Vec<PodIp>,
}

impl Message for PodSandboxNetworkStatus {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.ip);
        put_msgs(out, 2, &self.additional_ips);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.ip = string(v, n)?,
                2 => push_msg(&mut self.additional_ips, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

// Namespace has no field 1; options is field 2.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Namespace {
    pub options: Option<NamespaceOption>,
}

impl Message for Namespace {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 2, &self.options);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 2 {
                merge_opt(&mut self.options, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinuxPodSandboxStatus {
    pub namespaces: Option<Namespace>,
}

impl Message for LinuxPodSandboxStatus {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.namespaces);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.namespaces, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

// Timestamps throughout CRI are nanoseconds since the Unix epoch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxStatus {
    pub id: String,
    pub metadata: Option<PodSandboxMetadata>,
    pub state: PodSandboxState,
    pub created_at: i64,
    pub network: Option<PodSandboxNetworkStatus>,
    pub linux: Option<LinuxPodSandboxStatus>,
    pub labels: Map,
    pub annotations: Map,
    pub runtime_handler: String,
}

impl Message for PodSandboxStatus {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_opt(out, 2, &self.metadata);
        put_enum(out, 3, self.state.into());
        put_int(out, 4, self.created_at);
        put_opt(out, 5, &self.network);
        put_opt(out, 6, &self.linux);
        put_map(out, 7, &self.labels);
        put_map(out, 8, &self.annotations);
        put_string(out, 9, &self.runtime_handler);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => merge_opt(&mut self.metadata, v.bytes(n)?)?,
                3 => self.state = int32(v, n)?.into(),
                4 => self.created_at = v.int(n)?,
                5 => merge_opt(&mut self.network, v.bytes(n)?)?,
                6 => merge_opt(&mut self.linux, v.bytes(n)?)?,
                7 => merge_map(&mut self.labels, v, n)?,
                8 => merge_map(&mut self.annotations, v, n)?,
                9 => self.runtime_handler = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxStatusResponse {
    pub status: Option<PodSandboxStatus>,
    /// Free-form, values in JSON; expected only when verbose was asked for.
    pub info: Map,
    pub containers_statuses: Vec<ContainerStatus>,
    pub timestamp: i64,
}

impl Message for PodSandboxStatusResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.status);
        put_map(out, 2, &self.info);
        put_msgs(out, 3, &self.containers_statuses);
        put_int(out, 4, self.timestamp);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.status, v.bytes(n)?)?,
                2 => merge_map(&mut self.info, v, n)?,
                3 => push_msg(&mut self.containers_statuses, v.bytes(n)?)?,
                4 => self.timestamp = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

// The state filters are wrapped because the zero state (READY, CREATED)
// is a real value to filter on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxStateValue {
    pub state: PodSandboxState,
}

impl Message for PodSandboxStateValue {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.state.into());
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.state = int32(v, n)?.into();
            }
            Ok(())
        })
    }
}

// Every set criterion must match.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxFilter {
    pub id: String,
    pub state: Option<PodSandboxStateValue>,
    pub label_selector: Map,
}

impl Message for PodSandboxFilter {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_opt(out, 2, &self.state);
        put_map(out, 3, &self.label_selector);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => merge_opt(&mut self.state, v.bytes(n)?)?,
                3 => merge_map(&mut self.label_selector, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPodSandboxRequest {
    pub filter: Option<PodSandboxFilter>,
}

impl Message for ListPodSandboxRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.filter);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.filter, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandbox {
    pub id: String,
    pub metadata: Option<PodSandboxMetadata>,
    pub state: PodSandboxState,
    pub created_at: i64,
    pub labels: Map,
    pub annotations: Map,
    pub runtime_handler: String,
}

impl Message for PodSandbox {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_opt(out, 2, &self.metadata);
        put_enum(out, 3, self.state.into());
        put_int(out, 4, self.created_at);
        put_map(out, 5, &self.labels);
        put_map(out, 6, &self.annotations);
        put_string(out, 7, &self.runtime_handler);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => merge_opt(&mut self.metadata, v.bytes(n)?)?,
                3 => self.state = int32(v, n)?.into(),
                4 => self.created_at = v.int(n)?,
                5 => merge_map(&mut self.labels, v, n)?,
                6 => merge_map(&mut self.annotations, v, n)?,
                7 => self.runtime_handler = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPodSandboxResponse {
    pub items: Vec<PodSandbox>,
}

impl Message for ListPodSandboxResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_msgs(out, 1, &self.items);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                push_msg(&mut self.items, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageSpec {
    /// An image ID or a digest reference.
    pub image: String,
    pub annotations: Map,
    /// The reference the user wrote (image[:tag] or digest), when known.
    pub user_specified_image: String,
    pub runtime_handler: String,
    pub image_ref: String,
}

impl Message for ImageSpec {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.image);
        put_map(out, 2, &self.annotations);
        put_string(out, 18, &self.user_specified_image);
        put_string(out, 19, &self.runtime_handler);
        put_string(out, 20, &self.image_ref);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.image = string(v, n)?,
                2 => merge_map(&mut self.annotations, v, n)?,
                18 => self.user_specified_image = string(v, n)?,
                19 => self.runtime_handler = string(v, n)?,
                20 => self.image_ref = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

// value is bytes, not string, in this version of the proto, so an
// environment variable need not be UTF-8.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyValue {
    pub key: String,
    pub value: Vec<u8>,
}

impl Message for KeyValue {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.key);
        put_bytes(out, 2, &self.value);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.key = string(v, n)?,
                2 => self.value = bytes(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Capability {
    pub add_capabilities: Vec<String>,
    pub drop_capabilities: Vec<String>,
    pub add_ambient_capabilities: Vec<String>,
}

impl Message for Capability {
    fn write(&self, out: &mut Vec<u8>) {
        put_strings(out, 1, &self.add_capabilities);
        put_strings(out, 2, &self.drop_capabilities);
        put_strings(out, 3, &self.add_ambient_capabilities);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.add_capabilities.push(string(v, n)?),
                2 => self.drop_capabilities.push(string(v, n)?),
                3 => self.add_ambient_capabilities.push(string(v, n)?),
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinuxContainerSecurityContext {
    pub capabilities: Option<Capability>,
    pub privileged: bool,
    pub namespace_options: Option<NamespaceOption>,
    pub selinux_options: Option<SeLinuxOption>,
    pub run_as_user: Option<Int64Value>,
    /// Resolved inside the image; only used when run_as_user is unset.
    pub run_as_username: String,
    pub readonly_rootfs: bool,
    pub supplemental_groups: Vec<i64>,
    /// Deprecated in favor of apparmor.
    pub apparmor_profile: String,
    /// Deprecated in favor of seccomp.
    pub seccomp_profile_path: String,
    pub no_new_privs: bool,
    pub run_as_group: Option<Int64Value>,
    /// Paths to hide, and paths to make read-only, in the container.
    pub masked_paths: Vec<String>,
    pub readonly_paths: Vec<String>,
    pub seccomp: Option<SecurityProfile>,
    pub apparmor: Option<SecurityProfile>,
    pub supplemental_groups_policy: SupplementalGroupsPolicy,
}

impl Message for LinuxContainerSecurityContext {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.capabilities);
        put_bool(out, 2, self.privileged);
        put_opt(out, 3, &self.namespace_options);
        put_opt(out, 4, &self.selinux_options);
        put_opt(out, 5, &self.run_as_user);
        put_string(out, 6, &self.run_as_username);
        put_bool(out, 7, self.readonly_rootfs);
        put_packed_ints(out, 8, &self.supplemental_groups);
        put_string(out, 9, &self.apparmor_profile);
        put_string(out, 10, &self.seccomp_profile_path);
        put_bool(out, 11, self.no_new_privs);
        put_opt(out, 12, &self.run_as_group);
        put_strings(out, 13, &self.masked_paths);
        put_strings(out, 14, &self.readonly_paths);
        put_opt(out, 15, &self.seccomp);
        put_opt(out, 16, &self.apparmor);
        put_enum(out, 17, self.supplemental_groups_policy.into());
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.capabilities, v.bytes(n)?)?,
                2 => self.privileged = v.bool(n)?,
                3 => merge_opt(&mut self.namespace_options, v.bytes(n)?)?,
                4 => merge_opt(&mut self.selinux_options, v.bytes(n)?)?,
                5 => merge_opt(&mut self.run_as_user, v.bytes(n)?)?,
                6 => self.run_as_username = string(v, n)?,
                7 => self.readonly_rootfs = v.bool(n)?,
                8 => push_ints(&mut self.supplemental_groups, v, n)?,
                9 => self.apparmor_profile = string(v, n)?,
                10 => self.seccomp_profile_path = string(v, n)?,
                11 => self.no_new_privs = v.bool(n)?,
                12 => merge_opt(&mut self.run_as_group, v.bytes(n)?)?,
                13 => self.masked_paths.push(string(v, n)?),
                14 => self.readonly_paths.push(string(v, n)?),
                15 => merge_opt(&mut self.seccomp, v.bytes(n)?)?,
                16 => merge_opt(&mut self.apparmor, v.bytes(n)?)?,
                17 => self.supplemental_groups_policy = int32(v, n)?.into(),
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinuxContainerConfig {
    pub resources: Option<LinuxContainerResources>,
    pub security_context: Option<LinuxContainerSecurityContext>,
}

impl Message for LinuxContainerConfig {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.resources);
        put_opt(out, 2, &self.security_context);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.resources, v.bytes(n)?)?,
                2 => merge_opt(&mut self.security_context, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinuxContainerUser {
    pub uid: i64,
    pub gid: i64,
    pub supplemental_groups: Vec<i64>,
}

impl Message for LinuxContainerUser {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.uid);
        put_int(out, 2, self.gid);
        put_packed_ints(out, 3, &self.supplemental_groups);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.uid = v.int(n)?,
                2 => self.gid = v.int(n)?,
                3 => push_ints(&mut self.supplemental_groups, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerMetadata {
    pub name: String,
    pub attempt: u32,
}

impl Message for ContainerMetadata {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.name);
        put_uint(out, 2, u64::from(self.attempt));
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.name = string(v, n)?,
                2 => self.attempt = uint32(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Device {
    pub container_path: String,
    pub host_path: String,
    /// cgroup device permissions: any of r, w and m.
    pub permissions: String,
}

impl Message for Device {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_path);
        put_string(out, 2, &self.host_path);
        put_string(out, 3, &self.permissions);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.container_path = string(v, n)?,
                2 => self.host_path = string(v, n)?,
                3 => self.permissions = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CdiDevice {
    pub name: String,
}

impl Message for CdiDevice {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.name);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.name = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerConfig {
    pub metadata: Option<ContainerMetadata>,
    pub image: Option<ImageSpec>,
    /// The entrypoint; empty means the image's.
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub working_dir: String,
    pub envs: Vec<KeyValue>,
    pub mounts: Vec<Mount>,
    pub devices: Vec<Device>,
    pub labels: Map,
    /// Must be returned unchanged in ContainerStatus.
    pub annotations: Map,
    /// Relative to PodSandboxConfig.log_directory.
    pub log_path: String,
    pub stdin: bool,
    pub stdin_once: bool,
    pub tty: bool,
    pub linux: Option<LinuxContainerConfig>,
    pub cdi_devices: Vec<CdiDevice>,
    pub stop_signal: Signal,
}

impl Message for ContainerConfig {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.metadata);
        put_opt(out, 2, &self.image);
        put_strings(out, 3, &self.command);
        put_strings(out, 4, &self.args);
        put_string(out, 5, &self.working_dir);
        put_msgs(out, 6, &self.envs);
        put_msgs(out, 7, &self.mounts);
        put_msgs(out, 8, &self.devices);
        put_map(out, 9, &self.labels);
        put_map(out, 10, &self.annotations);
        put_string(out, 11, &self.log_path);
        put_bool(out, 12, self.stdin);
        put_bool(out, 13, self.stdin_once);
        put_bool(out, 14, self.tty);
        put_opt(out, 15, &self.linux);
        put_msgs(out, 17, &self.cdi_devices);
        put_enum(out, 18, self.stop_signal.into());
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.metadata, v.bytes(n)?)?,
                2 => merge_opt(&mut self.image, v.bytes(n)?)?,
                3 => self.command.push(string(v, n)?),
                4 => self.args.push(string(v, n)?),
                5 => self.working_dir = string(v, n)?,
                6 => push_msg(&mut self.envs, v.bytes(n)?)?,
                7 => push_msg(&mut self.mounts, v.bytes(n)?)?,
                8 => push_msg(&mut self.devices, v.bytes(n)?)?,
                9 => merge_map(&mut self.labels, v, n)?,
                10 => merge_map(&mut self.annotations, v, n)?,
                11 => self.log_path = string(v, n)?,
                12 => self.stdin = v.bool(n)?,
                13 => self.stdin_once = v.bool(n)?,
                14 => self.tty = v.bool(n)?,
                15 => merge_opt(&mut self.linux, v.bytes(n)?)?,
                17 => push_msg(&mut self.cdi_devices, v.bytes(n)?)?,
                18 => self.stop_signal = int32(v, n)?.into(),
                _ => {}
            }
            Ok(())
        })
    }
}

// sandbox_config repeats the RunPodSandbox config so that a runtime need
// not keep it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateContainerRequest {
    pub pod_sandbox_id: String,
    pub config: Option<ContainerConfig>,
    pub sandbox_config: Option<PodSandboxConfig>,
}

impl Message for CreateContainerRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.pod_sandbox_id);
        put_opt(out, 2, &self.config);
        put_opt(out, 3, &self.sandbox_config);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.pod_sandbox_id = string(v, n)?,
                2 => merge_opt(&mut self.config, v.bytes(n)?)?,
                3 => merge_opt(&mut self.sandbox_config, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateContainerResponse {
    pub container_id: String,
}

impl Message for CreateContainerResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.container_id = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartContainerRequest {
    pub container_id: String,
}

impl Message for StartContainerRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.container_id = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StopContainerRequest {
    pub container_id: String,
    /// Seconds to wait after the stop signal before killing; 0 kills at
    /// once.
    pub timeout: i64,
}

impl Message for StopContainerRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_id);
        put_int(out, 2, self.timeout);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.container_id = string(v, n)?,
                2 => self.timeout = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoveContainerRequest {
    pub container_id: String,
}

impl Message for RemoveContainerRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.container_id = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerStateValue {
    pub state: ContainerState,
}

impl Message for ContainerStateValue {
    fn write(&self, out: &mut Vec<u8>) {
        put_enum(out, 1, self.state.into());
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.state = int32(v, n)?.into();
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerFilter {
    pub id: String,
    pub state: Option<ContainerStateValue>,
    pub pod_sandbox_id: String,
    pub label_selector: Map,
}

impl Message for ContainerFilter {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_opt(out, 2, &self.state);
        put_string(out, 3, &self.pod_sandbox_id);
        put_map(out, 4, &self.label_selector);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => merge_opt(&mut self.state, v.bytes(n)?)?,
                3 => self.pod_sandbox_id = string(v, n)?,
                4 => merge_map(&mut self.label_selector, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListContainersRequest {
    pub filter: Option<ContainerFilter>,
}

impl Message for ListContainersRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.filter);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.filter, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Container {
    pub id: String,
    pub pod_sandbox_id: String,
    pub metadata: Option<ContainerMetadata>,
    pub image: Option<ImageSpec>,
    pub image_ref: String,
    pub state: ContainerState,
    pub created_at: i64,
    pub labels: Map,
    pub annotations: Map,
    pub image_id: String,
}

impl Message for Container {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_string(out, 2, &self.pod_sandbox_id);
        put_opt(out, 3, &self.metadata);
        put_opt(out, 4, &self.image);
        put_string(out, 5, &self.image_ref);
        put_enum(out, 6, self.state.into());
        put_int(out, 7, self.created_at);
        put_map(out, 8, &self.labels);
        put_map(out, 9, &self.annotations);
        put_string(out, 10, &self.image_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => self.pod_sandbox_id = string(v, n)?,
                3 => merge_opt(&mut self.metadata, v.bytes(n)?)?,
                4 => merge_opt(&mut self.image, v.bytes(n)?)?,
                5 => self.image_ref = string(v, n)?,
                6 => self.state = int32(v, n)?.into(),
                7 => self.created_at = v.int(n)?,
                8 => merge_map(&mut self.labels, v, n)?,
                9 => merge_map(&mut self.annotations, v, n)?,
                10 => self.image_id = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListContainersResponse {
    pub containers: Vec<Container>,
}

impl Message for ListContainersResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_msgs(out, 1, &self.containers);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                push_msg(&mut self.containers, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerStatusRequest {
    pub container_id: String,
    pub verbose: bool,
}

impl Message for ContainerStatusRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_id);
        put_bool(out, 2, self.verbose);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.container_id = string(v, n)?,
                2 => self.verbose = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerResources {
    pub linux: Option<LinuxContainerResources>,
}

impl Message for ContainerResources {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.linux);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.linux, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerUser {
    pub linux: Option<LinuxContainerUser>,
}

impl Message for ContainerUser {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.linux);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.linux, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerStatus {
    pub id: String,
    pub metadata: Option<ContainerMetadata>,
    pub state: ContainerState,
    pub created_at: i64,
    /// 0 when not started or not finished.
    pub started_at: i64,
    pub finished_at: i64,
    pub exit_code: i32,
    pub image: Option<ImageSpec>,
    pub image_ref: String,
    /// A CamelCase word; the kubelet expects "OOMKilled" after a cgroup
    /// out-of-memory kill.
    pub reason: String,
    pub message: String,
    pub labels: Map,
    pub annotations: Map,
    pub mounts: Vec<Mount>,
    pub log_path: String,
    pub resources: Option<ContainerResources>,
    /// Must equal the image_ref PullImage returned for the same image.
    pub image_id: String,
    pub user: Option<ContainerUser>,
    pub stop_signal: Signal,
}

impl Message for ContainerStatus {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_opt(out, 2, &self.metadata);
        put_enum(out, 3, self.state.into());
        put_int(out, 4, self.created_at);
        put_int(out, 5, self.started_at);
        put_int(out, 6, self.finished_at);
        put_int(out, 7, i64::from(self.exit_code));
        put_opt(out, 8, &self.image);
        put_string(out, 9, &self.image_ref);
        put_string(out, 10, &self.reason);
        put_string(out, 11, &self.message);
        put_map(out, 12, &self.labels);
        put_map(out, 13, &self.annotations);
        put_msgs(out, 14, &self.mounts);
        put_string(out, 15, &self.log_path);
        put_opt(out, 16, &self.resources);
        put_string(out, 17, &self.image_id);
        put_opt(out, 18, &self.user);
        put_enum(out, 19, self.stop_signal.into());
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => merge_opt(&mut self.metadata, v.bytes(n)?)?,
                3 => self.state = int32(v, n)?.into(),
                4 => self.created_at = v.int(n)?,
                5 => self.started_at = v.int(n)?,
                6 => self.finished_at = v.int(n)?,
                7 => self.exit_code = int32(v, n)?,
                8 => merge_opt(&mut self.image, v.bytes(n)?)?,
                9 => self.image_ref = string(v, n)?,
                10 => self.reason = string(v, n)?,
                11 => self.message = string(v, n)?,
                12 => merge_map(&mut self.labels, v, n)?,
                13 => merge_map(&mut self.annotations, v, n)?,
                14 => push_msg(&mut self.mounts, v.bytes(n)?)?,
                15 => self.log_path = string(v, n)?,
                16 => merge_opt(&mut self.resources, v.bytes(n)?)?,
                17 => self.image_id = string(v, n)?,
                18 => merge_opt(&mut self.user, v.bytes(n)?)?,
                19 => self.stop_signal = int32(v, n)?.into(),
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerStatusResponse {
    pub status: Option<ContainerStatus>,
    pub info: Map,
}

impl Message for ContainerStatusResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.status);
        put_map(out, 2, &self.info);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.status, v.bytes(n)?)?,
                2 => merge_map(&mut self.info, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecSyncRequest {
    pub container_id: String,
    pub cmd: Vec<String>,
    /// Seconds; 0 means no timeout.
    pub timeout: i64,
}

impl Message for ExecSyncRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_id);
        put_strings(out, 2, &self.cmd);
        put_int(out, 3, self.timeout);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.container_id = string(v, n)?,
                2 => self.cmd.push(string(v, n)?),
                3 => self.timeout = v.int(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecSyncResponse {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
}

impl Message for ExecSyncResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_bytes(out, 1, &self.stdout);
        put_bytes(out, 2, &self.stderr);
        put_int(out, 3, i64::from(self.exit_code));
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.stdout = bytes(v, n)?,
                2 => self.stderr = bytes(v, n)?,
                3 => self.exit_code = int32(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageFilter {
    pub image: Option<ImageSpec>,
}

impl Message for ImageFilter {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.image);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.image, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListImagesRequest {
    pub filter: Option<ImageFilter>,
}

impl Message for ListImagesRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.filter);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.filter, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Image {
    pub id: String,
    pub repo_tags: Vec<String>,
    pub repo_digests: Vec<String>,
    pub size: u64,
    /// Mutually exclusive with username.
    pub uid: Option<Int64Value>,
    pub username: String,
    pub spec: Option<ImageSpec>,
    /// Only a recommendation to skip garbage collection; RemoveImage must
    /// still remove it.
    pub pinned: bool,
}

impl Message for Image {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_strings(out, 2, &self.repo_tags);
        put_strings(out, 3, &self.repo_digests);
        put_uint(out, 4, self.size);
        put_opt(out, 5, &self.uid);
        put_string(out, 6, &self.username);
        put_opt(out, 7, &self.spec);
        put_bool(out, 8, self.pinned);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => self.repo_tags.push(string(v, n)?),
                3 => self.repo_digests.push(string(v, n)?),
                4 => self.size = v.uint(n)?,
                5 => merge_opt(&mut self.uid, v.bytes(n)?)?,
                6 => self.username = string(v, n)?,
                7 => merge_opt(&mut self.spec, v.bytes(n)?)?,
                8 => self.pinned = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListImagesResponse {
    pub images: Vec<Image>,
}

impl Message for ListImagesResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_msgs(out, 1, &self.images);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                push_msg(&mut self.images, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageStatusRequest {
    pub image: Option<ImageSpec>,
    pub verbose: bool,
}

impl Message for ImageStatusRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.image);
        put_bool(out, 2, self.verbose);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.image, v.bytes(n)?)?,
                2 => self.verbose = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

// An image that is not present is reported with image unset, not with an
// error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageStatusResponse {
    pub image: Option<Image>,
    pub info: Map,
}

impl Message for ImageStatusResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.image);
        put_map(out, 2, &self.info);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.image, v.bytes(n)?)?,
                2 => merge_map(&mut self.info, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct AuthConfig {
    pub username: String,
    pub password: String,
    pub auth: String,
    pub server_address: String,
    pub identity_token: String,
    pub registry_token: String,
}

// The proto marks the secrets debug_redact.
impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |s: &str| if s.is_empty() { "" } else { "<redacted>" };
        f.debug_struct("AuthConfig")
            .field("username", &self.username)
            .field("password", &redact(&self.password))
            .field("auth", &redact(&self.auth))
            .field("server_address", &self.server_address)
            .field("identity_token", &redact(&self.identity_token))
            .field("registry_token", &redact(&self.registry_token))
            .finish()
    }
}

impl Message for AuthConfig {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.username);
        put_string(out, 2, &self.password);
        put_string(out, 3, &self.auth);
        put_string(out, 4, &self.server_address);
        put_string(out, 5, &self.identity_token);
        put_string(out, 6, &self.registry_token);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.username = string(v, n)?,
                2 => self.password = string(v, n)?,
                3 => self.auth = string(v, n)?,
                4 => self.server_address = string(v, n)?,
                5 => self.identity_token = string(v, n)?,
                6 => self.registry_token = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PullImageRequest {
    pub image: Option<ImageSpec>,
    pub auth: Option<AuthConfig>,
    pub sandbox_config: Option<PodSandboxConfig>,
}

impl Message for PullImageRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.image);
        put_opt(out, 2, &self.auth);
        put_opt(out, 3, &self.sandbox_config);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.image, v.bytes(n)?)?,
                2 => merge_opt(&mut self.auth, v.bytes(n)?)?,
                3 => merge_opt(&mut self.sandbox_config, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PullImageResponse {
    /// Must equal Image.id, Container.image_id and ContainerStatus.image_id
    /// for the same image (since v1.36; it used to allow a digest).
    pub image_ref: String,
}

impl Message for PullImageResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.image_ref);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.image_ref = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoveImageRequest {
    pub image: Option<ImageSpec>,
}

impl Message for RemoveImageRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.image);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.image, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

// The kubelet requires the conditions "RuntimeReady" and "NetworkReady";
// a node is not ready while either is missing or false. Others are only
// shown to users.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeCondition {
    pub kind: String,
    pub status: bool,
    pub reason: String,
    pub message: String,
}

impl Message for RuntimeCondition {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.kind);
        put_bool(out, 2, self.status);
        put_string(out, 3, &self.reason);
        put_string(out, 4, &self.message);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.kind = string(v, n)?,
                2 => self.status = v.bool(n)?,
                3 => self.reason = string(v, n)?,
                4 => self.message = string(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeStatus {
    pub conditions: Vec<RuntimeCondition>,
}

impl Message for RuntimeStatus {
    fn write(&self, out: &mut Vec<u8>) {
        put_msgs(out, 1, &self.conditions);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                push_msg(&mut self.conditions, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusRequest {
    pub verbose: bool,
}

impl Message for StatusRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_bool(out, 1, self.verbose);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.verbose = v.bool(n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeHandlerFeatures {
    pub recursive_read_only_mounts: bool,
    pub user_namespaces: bool,
}

impl Message for RuntimeHandlerFeatures {
    fn write(&self, out: &mut Vec<u8>) {
        put_bool(out, 1, self.recursive_read_only_mounts);
        put_bool(out, 2, self.user_namespaces);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.recursive_read_only_mounts = v.bool(n)?,
                2 => self.user_namespaces = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

// The handler with an empty name stands for the default one.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeHandler {
    pub name: String,
    pub features: Option<RuntimeHandlerFeatures>,
}

impl Message for RuntimeHandler {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.name);
        put_opt(out, 2, &self.features);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.name = string(v, n)?,
                2 => merge_opt(&mut self.features, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeFeatures {
    pub supplemental_groups_policy: bool,
    pub user_namespaces_host_network: bool,
    pub mount_options: bool,
}

impl Message for RuntimeFeatures {
    fn write(&self, out: &mut Vec<u8>) {
        put_bool(out, 1, self.supplemental_groups_policy);
        put_bool(out, 2, self.user_namespaces_host_network);
        put_bool(out, 3, self.mount_options);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.supplemental_groups_policy = v.bool(n)?,
                2 => self.user_namespaces_host_network = v.bool(n)?,
                3 => self.mount_options = v.bool(n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusResponse {
    pub status: Option<RuntimeStatus>,
    pub info: Map,
    pub runtime_handlers: Vec<RuntimeHandler>,
    pub features: Option<RuntimeFeatures>,
}

impl Message for StatusResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.status);
        put_map(out, 2, &self.info);
        put_msgs(out, 3, &self.runtime_handlers);
        put_opt(out, 4, &self.features);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.status, v.bytes(n)?)?,
                2 => merge_map(&mut self.info, v, n)?,
                3 => push_msg(&mut self.runtime_handlers, v.bytes(n)?)?,
                4 => merge_opt(&mut self.features, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilesystemIdentifier {
    pub mountpoint: String,
}

impl Message for FilesystemIdentifier {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.mountpoint);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.mountpoint = string(v, n)?;
            }
            Ok(())
        })
    }
}

// The UInt64Value wrappers here and in the stats tell "not measured"
// apart from 0.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilesystemUsage {
    pub timestamp: i64,
    pub fs_id: Option<FilesystemIdentifier>,
    pub used_bytes: Option<UInt64Value>,
    pub inodes_used: Option<UInt64Value>,
}

impl Message for FilesystemUsage {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.timestamp);
        put_opt(out, 2, &self.fs_id);
        put_opt(out, 3, &self.used_bytes);
        put_opt(out, 4, &self.inodes_used);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.timestamp = v.int(n)?,
                2 => merge_opt(&mut self.fs_id, v.bytes(n)?)?,
                3 => merge_opt(&mut self.used_bytes, v.bytes(n)?)?,
                4 => merge_opt(&mut self.inodes_used, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageFsInfoResponse {
    pub image_filesystems: Vec<FilesystemUsage>,
    /// Only for runtimes that keep containers on a filesystem apart from
    /// images.
    pub container_filesystems: Vec<FilesystemUsage>,
}

impl Message for ImageFsInfoResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_msgs(out, 1, &self.image_filesystems);
        put_msgs(out, 2, &self.container_filesystems);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => push_msg(&mut self.image_filesystems, v.bytes(n)?)?,
                2 => push_msg(&mut self.container_filesystems, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PsiData {
    pub total: u64,
    pub avg10: f64,
    pub avg60: f64,
    pub avg300: f64,
}

impl Message for PsiData {
    fn write(&self, out: &mut Vec<u8>) {
        put_uint(out, 1, self.total);
        put_double(out, 2, self.avg10);
        put_double(out, 3, self.avg60);
        put_double(out, 4, self.avg300);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.total = v.uint(n)?,
                2 => self.avg10 = double(v, n)?,
                3 => self.avg60 = double(v, n)?,
                4 => self.avg300 = double(v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PsiStats {
    pub full: Option<PsiData>,
    pub some: Option<PsiData>,
}

impl Message for PsiStats {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.full);
        put_opt(out, 2, &self.some);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.full, v.bytes(n)?)?,
                2 => merge_opt(&mut self.some, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct CpuUsage {
    pub timestamp: i64,
    /// Cumulative.
    pub usage_core_nano_seconds: Option<UInt64Value>,
    /// Averaged over a short window.
    pub usage_nano_cores: Option<UInt64Value>,
    pub psi: Option<PsiStats>,
}

impl Message for CpuUsage {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.timestamp);
        put_opt(out, 2, &self.usage_core_nano_seconds);
        put_opt(out, 3, &self.usage_nano_cores);
        put_opt(out, 4, &self.psi);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.timestamp = v.int(n)?,
                2 => merge_opt(&mut self.usage_core_nano_seconds, v.bytes(n)?)?,
                3 => merge_opt(&mut self.usage_nano_cores, v.bytes(n)?)?,
                4 => merge_opt(&mut self.psi, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MemoryUsage {
    pub timestamp: i64,
    pub working_set_bytes: Option<UInt64Value>,
    pub available_bytes: Option<UInt64Value>,
    pub usage_bytes: Option<UInt64Value>,
    pub rss_bytes: Option<UInt64Value>,
    pub page_faults: Option<UInt64Value>,
    pub major_page_faults: Option<UInt64Value>,
    pub psi: Option<PsiStats>,
}

impl Message for MemoryUsage {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.timestamp);
        put_opt(out, 2, &self.working_set_bytes);
        put_opt(out, 3, &self.available_bytes);
        put_opt(out, 4, &self.usage_bytes);
        put_opt(out, 5, &self.rss_bytes);
        put_opt(out, 6, &self.page_faults);
        put_opt(out, 7, &self.major_page_faults);
        put_opt(out, 8, &self.psi);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.timestamp = v.int(n)?,
                2 => merge_opt(&mut self.working_set_bytes, v.bytes(n)?)?,
                3 => merge_opt(&mut self.available_bytes, v.bytes(n)?)?,
                4 => merge_opt(&mut self.usage_bytes, v.bytes(n)?)?,
                5 => merge_opt(&mut self.rss_bytes, v.bytes(n)?)?,
                6 => merge_opt(&mut self.page_faults, v.bytes(n)?)?,
                7 => merge_opt(&mut self.major_page_faults, v.bytes(n)?)?,
                8 => merge_opt(&mut self.psi, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct IoUsage {
    pub timestamp: i64,
    pub psi: Option<PsiStats>,
}

impl Message for IoUsage {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.timestamp);
        put_opt(out, 2, &self.psi);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.timestamp = v.int(n)?,
                2 => merge_opt(&mut self.psi, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SwapUsage {
    pub timestamp: i64,
    pub swap_available_bytes: Option<UInt64Value>,
    pub swap_usage_bytes: Option<UInt64Value>,
}

impl Message for SwapUsage {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.timestamp);
        put_opt(out, 2, &self.swap_available_bytes);
        put_opt(out, 3, &self.swap_usage_bytes);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.timestamp = v.int(n)?,
                2 => merge_opt(&mut self.swap_available_bytes, v.bytes(n)?)?,
                3 => merge_opt(&mut self.swap_usage_bytes, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerAttributes {
    pub id: String,
    pub metadata: Option<ContainerMetadata>,
    pub labels: Map,
    pub annotations: Map,
}

impl Message for ContainerAttributes {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_opt(out, 2, &self.metadata);
        put_map(out, 3, &self.labels);
        put_map(out, 4, &self.annotations);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => merge_opt(&mut self.metadata, v.bytes(n)?)?,
                3 => merge_map(&mut self.labels, v, n)?,
                4 => merge_map(&mut self.annotations, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerStats {
    pub attributes: Option<ContainerAttributes>,
    pub cpu: Option<CpuUsage>,
    pub memory: Option<MemoryUsage>,
    pub writable_layer: Option<FilesystemUsage>,
    pub swap: Option<SwapUsage>,
    pub io: Option<IoUsage>,
}

impl Message for ContainerStats {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.attributes);
        put_opt(out, 2, &self.cpu);
        put_opt(out, 3, &self.memory);
        put_opt(out, 4, &self.writable_layer);
        put_opt(out, 5, &self.swap);
        put_opt(out, 6, &self.io);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.attributes, v.bytes(n)?)?,
                2 => merge_opt(&mut self.cpu, v.bytes(n)?)?,
                3 => merge_opt(&mut self.memory, v.bytes(n)?)?,
                4 => merge_opt(&mut self.writable_layer, v.bytes(n)?)?,
                5 => merge_opt(&mut self.swap, v.bytes(n)?)?,
                6 => merge_opt(&mut self.io, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerStatsRequest {
    pub container_id: String,
}

impl Message for ContainerStatsRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.container_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.container_id = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerStatsResponse {
    pub stats: Option<ContainerStats>,
}

impl Message for ContainerStatsResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.stats);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.stats, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContainerStatsFilter {
    pub id: String,
    pub pod_sandbox_id: String,
    pub label_selector: Map,
}

impl Message for ContainerStatsFilter {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_string(out, 2, &self.pod_sandbox_id);
        put_map(out, 3, &self.label_selector);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => self.pod_sandbox_id = string(v, n)?,
                3 => merge_map(&mut self.label_selector, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListContainerStatsRequest {
    pub filter: Option<ContainerStatsFilter>,
}

impl Message for ListContainerStatsRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.filter);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.filter, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListContainerStatsResponse {
    pub stats: Vec<ContainerStats>,
}

impl Message for ListContainerStatsResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_msgs(out, 1, &self.stats);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                push_msg(&mut self.stats, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetworkInterfaceUsage {
    pub name: String,
    pub rx_bytes: Option<UInt64Value>,
    pub rx_errors: Option<UInt64Value>,
    pub tx_bytes: Option<UInt64Value>,
    pub tx_errors: Option<UInt64Value>,
}

impl Message for NetworkInterfaceUsage {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.name);
        put_opt(out, 2, &self.rx_bytes);
        put_opt(out, 3, &self.rx_errors);
        put_opt(out, 4, &self.tx_bytes);
        put_opt(out, 5, &self.tx_errors);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.name = string(v, n)?,
                2 => merge_opt(&mut self.rx_bytes, v.bytes(n)?)?,
                3 => merge_opt(&mut self.rx_errors, v.bytes(n)?)?,
                4 => merge_opt(&mut self.tx_bytes, v.bytes(n)?)?,
                5 => merge_opt(&mut self.tx_errors, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetworkUsage {
    pub timestamp: i64,
    pub default_interface: Option<NetworkInterfaceUsage>,
    pub interfaces: Vec<NetworkInterfaceUsage>,
}

impl Message for NetworkUsage {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.timestamp);
        put_opt(out, 2, &self.default_interface);
        put_msgs(out, 3, &self.interfaces);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.timestamp = v.int(n)?,
                2 => merge_opt(&mut self.default_interface, v.bytes(n)?)?,
                3 => push_msg(&mut self.interfaces, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessUsage {
    pub timestamp: i64,
    pub process_count: Option<UInt64Value>,
}

impl Message for ProcessUsage {
    fn write(&self, out: &mut Vec<u8>) {
        put_int(out, 1, self.timestamp);
        put_opt(out, 2, &self.process_count);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.timestamp = v.int(n)?,
                2 => merge_opt(&mut self.process_count, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LinuxPodSandboxStats {
    pub cpu: Option<CpuUsage>,
    pub memory: Option<MemoryUsage>,
    pub network: Option<NetworkUsage>,
    pub process: Option<ProcessUsage>,
    pub containers: Vec<ContainerStats>,
    pub io: Option<IoUsage>,
}

impl Message for LinuxPodSandboxStats {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.cpu);
        put_opt(out, 2, &self.memory);
        put_opt(out, 3, &self.network);
        put_opt(out, 4, &self.process);
        put_msgs(out, 5, &self.containers);
        put_opt(out, 6, &self.io);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.cpu, v.bytes(n)?)?,
                2 => merge_opt(&mut self.memory, v.bytes(n)?)?,
                3 => merge_opt(&mut self.network, v.bytes(n)?)?,
                4 => merge_opt(&mut self.process, v.bytes(n)?)?,
                5 => push_msg(&mut self.containers, v.bytes(n)?)?,
                6 => merge_opt(&mut self.io, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxAttributes {
    pub id: String,
    pub metadata: Option<PodSandboxMetadata>,
    pub labels: Map,
    pub annotations: Map,
}

impl Message for PodSandboxAttributes {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_opt(out, 2, &self.metadata);
        put_map(out, 3, &self.labels);
        put_map(out, 4, &self.annotations);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => merge_opt(&mut self.metadata, v.bytes(n)?)?,
                3 => merge_map(&mut self.labels, v, n)?,
                4 => merge_map(&mut self.annotations, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PodSandboxStats {
    pub attributes: Option<PodSandboxAttributes>,
    pub linux: Option<LinuxPodSandboxStats>,
}

impl Message for PodSandboxStats {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.attributes);
        put_opt(out, 2, &self.linux);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => merge_opt(&mut self.attributes, v.bytes(n)?)?,
                2 => merge_opt(&mut self.linux, v.bytes(n)?)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxStatsRequest {
    pub pod_sandbox_id: String,
}

impl Message for PodSandboxStatsRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.pod_sandbox_id);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                self.pod_sandbox_id = string(v, n)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PodSandboxStatsResponse {
    pub stats: Option<PodSandboxStats>,
}

impl Message for PodSandboxStatsResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.stats);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.stats, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodSandboxStatsFilter {
    pub id: String,
    pub label_selector: Map,
}

impl Message for PodSandboxStatsFilter {
    fn write(&self, out: &mut Vec<u8>) {
        put_string(out, 1, &self.id);
        put_map(out, 2, &self.label_selector);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            match n {
                1 => self.id = string(v, n)?,
                2 => merge_map(&mut self.label_selector, v, n)?,
                _ => {}
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPodSandboxStatsRequest {
    pub filter: Option<PodSandboxStatsFilter>,
}

impl Message for ListPodSandboxStatsRequest {
    fn write(&self, out: &mut Vec<u8>) {
        put_opt(out, 1, &self.filter);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                merge_opt(&mut self.filter, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListPodSandboxStatsResponse {
    pub stats: Vec<PodSandboxStats>,
}

impl Message for ListPodSandboxStatsResponse {
    fn write(&self, out: &mut Vec<u8>) {
        put_msgs(out, 1, &self.stats);
    }

    fn merge(&mut self, buf: &[u8]) -> Result<(), DecodeError> {
        each_field(buf, |n, v| {
            if n == 1 {
                push_msg(&mut self.stats, v.bytes(n)?)?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::type_name;
    use std::fmt::Debug;

    const UID: &str = "6f1c2a4e-0d3b-4c5e-9a7f-1b2c3d4e5f60";
    const SANDBOX_ID: &str = "8e3b5c1d2f4a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5";

    fn map(pairs: &[(&str, &str)]) -> Map {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn sandbox_metadata() -> PodSandboxMetadata {
        PodSandboxMetadata {
            name: "web-0".into(),
            uid: UID.into(),
            namespace: "default".into(),
            attempt: 1,
        }
    }

    fn sandbox_config() -> PodSandboxConfig {
        PodSandboxConfig {
            metadata: Some(sandbox_metadata()),
            hostname: "web-0".into(),
            log_directory: format!("/var/log/pods/default_web-0_{UID}"),
            dns_config: Some(DnsConfig {
                servers: strings(&["10.96.0.10"]),
                searches: strings(&[
                    "default.svc.cluster.local",
                    "svc.cluster.local",
                    "cluster.local",
                ]),
                options: strings(&["ndots:5"]),
            }),
            port_mappings: vec![
                PortMapping {
                    protocol: Protocol::Tcp,
                    container_port: 8080,
                    ..Default::default()
                },
                PortMapping {
                    protocol: Protocol::Udp,
                    container_port: 53,
                    host_port: 5353,
                    host_ip: "127.0.0.1".into(),
                },
            ],
            labels: map(&[
                ("app", "web"),
                ("io.kubernetes.pod.name", "web-0"),
                ("io.kubernetes.pod.namespace", "default"),
                ("io.kubernetes.pod.uid", UID),
            ]),
            annotations: map(&[
                (
                    "kubernetes.io/config.seen",
                    "2026-10-20T10:00:00.000000000Z",
                ),
                ("kubernetes.io/config.source", "api"),
                ("example.com/empty", ""),
            ]),
            linux: Some(LinuxPodSandboxConfig {
                cgroup_parent: format!("/kubepods/burstable/pod{UID}"),
                security_context: Some(LinuxSandboxSecurityContext {
                    namespace_options: Some(NamespaceOption {
                        pid: NamespaceMode::Container,
                        ..Default::default()
                    }),
                    run_as_user: Some(Int64Value { value: 0 }),
                    supplemental_groups: vec![1000, 2000],
                    seccomp: Some(SecurityProfile::default()),
                    ..Default::default()
                }),
                sysctls: map(&[("net.ipv4.ip_unprivileged_port_start", "0")]),
                ..Default::default()
            }),
        }
    }

    fn run_pod_sandbox() -> RunPodSandboxRequest {
        RunPodSandboxRequest {
            config: Some(sandbox_config()),
            runtime_handler: String::new(),
        }
    }

    fn create_container() -> CreateContainerRequest {
        let pod_volumes = format!("/var/lib/kubelet/pods/{UID}");
        CreateContainerRequest {
            pod_sandbox_id: SANDBOX_ID.into(),
            config: Some(ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "nginx".into(),
                    attempt: 0,
                }),
                image: Some(ImageSpec {
                    image:
                        "sha256:3b25b682ea82b2db3cc4fd48db818be788ee3f902ac7378090cf2624ec2442df"
                            .into(),
                    user_specified_image: "nginx:1.27".into(),
                    ..Default::default()
                }),
                command: strings(&["/docker-entrypoint.sh"]),
                args: strings(&["nginx", "-g", "daemon off;"]),
                working_dir: "/".into(),
                envs: vec![
                    KeyValue {
                        key: "KUBERNETES_SERVICE_HOST".into(),
                        value: b"10.96.0.1".to_vec(),
                    },
                    KeyValue {
                        key: "EMPTY".into(),
                        value: vec![],
                    },
                ],
                mounts: vec![
                    Mount {
                        container_path: "/var/run/secrets/kubernetes.io/serviceaccount".into(),
                        host_path: format!(
                            "{pod_volumes}/volumes/kubernetes.io~projected/kube-api-access-x7k2p"
                        ),
                        readonly: true,
                        ..Default::default()
                    },
                    Mount {
                        container_path: "/etc/hosts".into(),
                        host_path: format!("{pod_volumes}/etc-hosts"),
                        ..Default::default()
                    },
                    Mount {
                        container_path: "/data".into(),
                        host_path: "/mnt/data".into(),
                        propagation: MountPropagation::HostToContainer,
                        ..Default::default()
                    },
                ],
                labels: map(&[
                    ("io.kubernetes.container.name", "nginx"),
                    ("io.kubernetes.pod.name", "web-0"),
                    ("io.kubernetes.pod.namespace", "default"),
                    ("io.kubernetes.pod.uid", UID),
                ]),
                annotations: map(&[
                    ("io.kubernetes.container.hash", "a1b2c3d4"),
                    ("io.kubernetes.container.restartCount", "0"),
                    (
                        "io.kubernetes.container.terminationMessagePath",
                        "/dev/termination-log",
                    ),
                ]),
                log_path: "nginx/0.log".into(),
                linux: Some(LinuxContainerConfig {
                    resources: Some(LinuxContainerResources {
                        cpu_period: 100_000,
                        cpu_quota: 50_000,
                        cpu_shares: 512,
                        memory_limit_in_bytes: 256 << 20,
                        oom_score_adj: -997,
                        hugepage_limits: vec![HugepageLimit {
                            page_size: "2MB".into(),
                            limit: 0,
                        }],
                        unified: map(&[("memory.oom.group", "1")]),
                        ..Default::default()
                    }),
                    security_context: Some(LinuxContainerSecurityContext {
                        capabilities: Some(Capability {
                            drop_capabilities: strings(&["ALL"]),
                            ..Default::default()
                        }),
                        namespace_options: Some(NamespaceOption {
                            pid: NamespaceMode::Container,
                            ..Default::default()
                        }),
                        run_as_user: Some(Int64Value { value: 1000 }),
                        run_as_group: Some(Int64Value { value: 3000 }),
                        no_new_privs: true,
                        masked_paths: strings(&["/proc/kcore", "/proc/keys"]),
                        readonly_paths: strings(&["/proc/bus", "/proc/sys"]),
                        seccomp: Some(SecurityProfile::default()),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            sandbox_config: Some(sandbox_config()),
        }
    }

    fn list_containers() -> ListContainersResponse {
        ListContainersResponse {
            containers: vec![
                Container {
                    id: "c1a2b3".into(),
                    pod_sandbox_id: SANDBOX_ID.into(),
                    metadata: Some(ContainerMetadata {
                        name: "nginx".into(),
                        attempt: 0,
                    }),
                    image: Some(ImageSpec {
                        image: "sha256:3b25b6".into(),
                        ..Default::default()
                    }),
                    image_ref: "sha256:3b25b6".into(),
                    state: ContainerState::Running,
                    created_at: 1_760_954_400_123_456_789,
                    labels: map(&[("io.kubernetes.container.name", "nginx")]),
                    annotations: map(&[("io.kubernetes.container.restartCount", "0")]),
                    image_id: "sha256:3b25b6".into(),
                },
                Container {
                    id: "d4e5f6".into(),
                    pod_sandbox_id: SANDBOX_ID.into(),
                    metadata: Some(ContainerMetadata {
                        name: "init-db".into(),
                        attempt: 2,
                    }),
                    image: Some(ImageSpec {
                        image: "sha256:9f8e7d".into(),
                        ..Default::default()
                    }),
                    image_ref: "sha256:9f8e7d".into(),
                    state: ContainerState::Exited,
                    created_at: 1_760_954_300_000_000_000,
                    labels: map(&[("io.kubernetes.container.name", "init-db")]),
                    annotations: Map::new(),
                    image_id: "sha256:9f8e7d".into(),
                },
            ],
        }
    }

    fn pod_sandbox_status() -> PodSandboxStatusResponse {
        PodSandboxStatusResponse {
            status: Some(PodSandboxStatus {
                id: SANDBOX_ID.into(),
                metadata: Some(sandbox_metadata()),
                state: PodSandboxState::Ready,
                created_at: 1_760_954_400_000_000_000,
                network: Some(PodSandboxNetworkStatus {
                    ip: "10.244.1.7".into(),
                    additional_ips: vec![PodIp {
                        ip: "fd00:10:244:1::7".into(),
                    }],
                }),
                linux: Some(LinuxPodSandboxStatus {
                    namespaces: Some(Namespace {
                        options: Some(NamespaceOption {
                            pid: NamespaceMode::Container,
                            ..Default::default()
                        }),
                    }),
                }),
                labels: map(&[("app", "web")]),
                annotations: map(&[("kubernetes.io/config.source", "api")]),
                runtime_handler: "runc".into(),
            }),
            info: map(&[("info", r#"{"pid":4242,"runtimeSpec":{}}"#)]),
            containers_statuses: vec![],
            timestamp: 1_760_954_460_000_000_000,
        }
    }

    fn status() -> StatusResponse {
        StatusResponse {
            status: Some(RuntimeStatus {
                conditions: vec![
                    RuntimeCondition {
                        kind: "RuntimeReady".into(),
                        status: true,
                        ..Default::default()
                    },
                    RuntimeCondition {
                        kind: "NetworkReady".into(),
                        status: false,
                        reason: "NetworkPluginNotReady".into(),
                        message: "Network plugin returns error: cni plugin not initialized".into(),
                    },
                ],
            }),
            info: Map::new(),
            runtime_handlers: vec![
                RuntimeHandler {
                    name: String::new(),
                    features: Some(RuntimeHandlerFeatures {
                        recursive_read_only_mounts: true,
                        user_namespaces: true,
                    }),
                },
                RuntimeHandler {
                    name: "runc".into(),
                    features: Some(RuntimeHandlerFeatures {
                        recursive_read_only_mounts: true,
                        user_namespaces: false,
                    }),
                },
            ],
            features: Some(RuntimeFeatures {
                supplemental_groups_policy: true,
                ..Default::default()
            }),
        }
    }

    // The builders below set every field, so that a field lost on either
    // side of the round trip shows up.

    fn ids() -> Vec<IdMapping> {
        vec![IdMapping {
            host_id: 100_000,
            container_id: 0,
            length: 65_536,
        }]
    }

    fn full_ns() -> NamespaceOption {
        NamespaceOption {
            network: NamespaceMode::Node,
            pid: NamespaceMode::Target,
            ipc: NamespaceMode::Other(9),
            target_id: "c1a2b3".into(),
            userns_options: Some(UserNamespace {
                mode: NamespaceMode::Pod,
                uids: ids(),
                gids: ids(),
            }),
        }
    }

    fn full_image_spec() -> ImageSpec {
        ImageSpec {
            image: "sha256:abc".into(),
            annotations: map(&[("a", "1"), ("b", "")]),
            user_specified_image: "busybox:1.36".into(),
            runtime_handler: "kata".into(),
            image_ref: "sha256:def".into(),
        }
    }

    fn full_mount() -> Mount {
        Mount {
            container_path: "/m".into(),
            host_path: String::new(),
            readonly: true,
            selinux_relabel: true,
            propagation: MountPropagation::Bidirectional,
            uid_mappings: ids(),
            gid_mappings: ids(),
            recursive_read_only: true,
            image: Some(full_image_spec()),
            image_sub_path: "sub".into(),
            mount_options: strings(&["noexec", ""]),
        }
    }

    fn full_selinux() -> SeLinuxOption {
        SeLinuxOption {
            user: "system_u".into(),
            role: "system_r".into(),
            kind: "container_t".into(),
            level: "s0:c1,c2".into(),
        }
    }

    fn localhost_profile() -> SecurityProfile {
        SecurityProfile {
            profile_type: ProfileType::Localhost,
            localhost_ref: "profiles/audit.json".into(),
        }
    }

    fn full_resources() -> LinuxContainerResources {
        LinuxContainerResources {
            cpu_period: 1,
            cpu_quota: -1,
            cpu_shares: 2,
            memory_limit_in_bytes: 3,
            oom_score_adj: -1000,
            cpuset_cpus: "0-3".into(),
            cpuset_mems: "0".into(),
            hugepage_limits: vec![HugepageLimit {
                page_size: "1GB".into(),
                limit: u64::MAX,
            }],
            unified: map(&[("memory.high", "max"), ("pids.max", "")]),
            memory_swap_limit_in_bytes: 4,
        }
    }

    fn full_sandbox_sc() -> LinuxSandboxSecurityContext {
        LinuxSandboxSecurityContext {
            namespace_options: Some(full_ns()),
            selinux_options: Some(full_selinux()),
            run_as_user: Some(Int64Value { value: 0 }),
            readonly_rootfs: true,
            supplemental_groups: vec![-1, 0, i64::MAX],
            privileged: true,
            seccomp_profile_path: "runtime/default".into(),
            run_as_group: Some(Int64Value { value: 5 }),
            seccomp: Some(SecurityProfile::default()),
            apparmor: Some(localhost_profile()),
            supplemental_groups_policy: SupplementalGroupsPolicy::Strict,
        }
    }

    fn full_container_sc() -> LinuxContainerSecurityContext {
        LinuxContainerSecurityContext {
            capabilities: Some(Capability {
                add_capabilities: strings(&["NET_ADMIN"]),
                drop_capabilities: strings(&["ALL"]),
                add_ambient_capabilities: strings(&["NET_BIND_SERVICE"]),
            }),
            privileged: true,
            namespace_options: Some(full_ns()),
            selinux_options: Some(full_selinux()),
            run_as_user: Some(Int64Value { value: 1 }),
            run_as_username: "nobody".into(),
            readonly_rootfs: true,
            supplemental_groups: vec![10],
            apparmor_profile: "unconfined".into(),
            seccomp_profile_path: "unconfined".into(),
            no_new_privs: true,
            run_as_group: Some(Int64Value { value: 2 }),
            masked_paths: strings(&["/proc/acpi"]),
            readonly_paths: strings(&["/proc/irq"]),
            seccomp: Some(localhost_profile()),
            apparmor: Some(SecurityProfile {
                profile_type: ProfileType::Unconfined,
                localhost_ref: String::new(),
            }),
            supplemental_groups_policy: SupplementalGroupsPolicy::Other(7),
        }
    }

    fn full_sandbox_config() -> PodSandboxConfig {
        let mut c = sandbox_config();
        c.linux = Some(LinuxPodSandboxConfig {
            cgroup_parent: "kubepods.slice".into(),
            security_context: Some(full_sandbox_sc()),
            sysctls: map(&[("kernel.shm_rmid_forced", "1")]),
            overhead: Some(full_resources()),
            resources: Some(LinuxContainerResources {
                cpu_shares: 1024,
                ..Default::default()
            }),
        });
        c.port_mappings.push(PortMapping {
            protocol: Protocol::Sctp,
            container_port: -1,
            host_port: i32::MAX,
            host_ip: "::1".into(),
        });
        c
    }

    fn full_container_config() -> ContainerConfig {
        ContainerConfig {
            metadata: Some(ContainerMetadata {
                name: "app".into(),
                attempt: u32::MAX,
            }),
            image: Some(full_image_spec()),
            command: strings(&["/bin/sh", ""]),
            args: strings(&["-c", "sleep 1"]),
            working_dir: "/work".into(),
            envs: vec![KeyValue {
                key: "BIN".into(),
                value: vec![0xff, 0x00],
            }],
            mounts: vec![full_mount(), Mount::default()],
            devices: vec![Device {
                container_path: "/dev/fuse".into(),
                host_path: "/dev/fuse".into(),
                permissions: "rwm".into(),
            }],
            labels: map(&[("l", "1")]),
            annotations: map(&[("a", "2")]),
            log_path: "app/3.log".into(),
            stdin: true,
            stdin_once: true,
            tty: true,
            linux: Some(LinuxContainerConfig {
                resources: Some(full_resources()),
                security_context: Some(full_container_sc()),
            }),
            cdi_devices: vec![CdiDevice {
                name: "nvidia.com/gpu=0".into(),
            }],
            stop_signal: Signal::SigRtMax,
        }
    }

    fn full_container_status() -> ContainerStatus {
        ContainerStatus {
            id: "c1".into(),
            metadata: Some(ContainerMetadata {
                name: "app".into(),
                attempt: 1,
            }),
            state: ContainerState::Unknown,
            created_at: 1,
            started_at: 2,
            finished_at: 3,
            exit_code: -1,
            image: Some(full_image_spec()),
            image_ref: "sha256:def".into(),
            reason: "OOMKilled".into(),
            message: "out of memory".into(),
            labels: map(&[("l", "1")]),
            annotations: map(&[("a", "2")]),
            mounts: vec![full_mount()],
            log_path: "/var/log/pods/x/app/1.log".into(),
            resources: Some(ContainerResources {
                linux: Some(full_resources()),
            }),
            image_id: "sha256:def".into(),
            user: Some(ContainerUser {
                linux: Some(LinuxContainerUser {
                    uid: 1000,
                    gid: 1000,
                    supplemental_groups: vec![4, 24],
                }),
            }),
            stop_signal: Signal::SigTerm,
        }
    }

    fn u64v(value: u64) -> Option<UInt64Value> {
        Some(UInt64Value { value })
    }

    fn psi() -> Option<PsiStats> {
        let data = PsiData {
            total: 12,
            avg10: 0.25,
            avg60: -0.0,
            avg300: f64::MAX,
        };
        Some(PsiStats {
            full: Some(data.clone()),
            some: Some(data),
        })
    }

    fn fs_usage() -> FilesystemUsage {
        FilesystemUsage {
            timestamp: 7,
            fs_id: Some(FilesystemIdentifier {
                mountpoint: "/var/lib/quantykube".into(),
            }),
            used_bytes: u64v(0),
            inodes_used: u64v(99),
        }
    }

    fn full_container_stats() -> ContainerStats {
        ContainerStats {
            attributes: Some(ContainerAttributes {
                id: "c1".into(),
                metadata: Some(ContainerMetadata {
                    name: "app".into(),
                    attempt: 0,
                }),
                labels: map(&[("l", "1")]),
                annotations: map(&[("a", "2")]),
            }),
            cpu: Some(CpuUsage {
                timestamp: 1,
                usage_core_nano_seconds: u64v(2),
                usage_nano_cores: u64v(3),
                psi: psi(),
            }),
            memory: Some(MemoryUsage {
                timestamp: 1,
                working_set_bytes: u64v(1),
                available_bytes: u64v(2),
                usage_bytes: u64v(3),
                rss_bytes: u64v(4),
                page_faults: u64v(5),
                major_page_faults: u64v(6),
                psi: psi(),
            }),
            writable_layer: Some(fs_usage()),
            swap: Some(SwapUsage {
                timestamp: 1,
                swap_available_bytes: u64v(0),
                swap_usage_bytes: u64v(8),
            }),
            io: Some(IoUsage {
                timestamp: 1,
                psi: psi(),
            }),
        }
    }

    fn full_sandbox_stats() -> PodSandboxStats {
        let eth0 = NetworkInterfaceUsage {
            name: "eth0".into(),
            rx_bytes: u64v(1),
            rx_errors: u64v(2),
            tx_bytes: u64v(3),
            tx_errors: u64v(4),
        };
        PodSandboxStats {
            attributes: Some(PodSandboxAttributes {
                id: SANDBOX_ID.into(),
                metadata: Some(sandbox_metadata()),
                labels: map(&[("app", "web")]),
                annotations: map(&[("a", "")]),
            }),
            linux: Some(LinuxPodSandboxStats {
                cpu: full_container_stats().cpu,
                memory: full_container_stats().memory,
                network: Some(NetworkUsage {
                    timestamp: 5,
                    default_interface: Some(eth0.clone()),
                    interfaces: vec![eth0, NetworkInterfaceUsage::default()],
                }),
                process: Some(ProcessUsage {
                    timestamp: 5,
                    process_count: u64v(17),
                }),
                containers: vec![full_container_stats()],
                io: full_container_stats().io,
            }),
        }
    }

    fn full_image() -> Image {
        Image {
            id: "sha256:def".into(),
            repo_tags: strings(&["docker.io/library/busybox:1.36"]),
            repo_digests: strings(&["docker.io/library/busybox@sha256:abc"]),
            size: 4_261_574,
            uid: Some(Int64Value { value: 0 }),
            username: String::new(),
            spec: Some(full_image_spec()),
            pinned: true,
        }
    }

    fn rt<M: Message + PartialEq + Debug>(m: M) -> Option<String> {
        let bytes = m.encode();
        match M::decode(&bytes) {
            Ok(back) if back == m => None,
            other => Some(format!("{}: {other:?} from {bytes:02x?}", type_name::<M>())),
        }
    }

    #[test]
    fn every_message_round_trips() {
        let label_selector = map(&[("app", "web"), ("tier", "")]);
        let failures: Vec<String> = [
            rt(VersionRequest {
                version: "v1".into(),
            }),
            rt(VersionResponse {
                version: "0.1.0".into(),
                runtime_name: "quantykube".into(),
                runtime_version: "0.1.0".into(),
                runtime_api_version: "v1".into(),
            }),
            rt(run_pod_sandbox()),
            rt(RunPodSandboxRequest {
                config: Some(full_sandbox_config()),
                runtime_handler: "kata".into(),
            }),
            rt(RunPodSandboxResponse {
                pod_sandbox_id: SANDBOX_ID.into(),
            }),
            rt(StopPodSandboxRequest {
                pod_sandbox_id: SANDBOX_ID.into(),
            }),
            rt(StopPodSandboxResponse {}),
            rt(RemovePodSandboxRequest {
                pod_sandbox_id: SANDBOX_ID.into(),
            }),
            rt(RemovePodSandboxResponse {}),
            rt(PodSandboxStatusRequest {
                pod_sandbox_id: SANDBOX_ID.into(),
                verbose: true,
            }),
            rt(pod_sandbox_status()),
            rt(PodSandboxStatusResponse {
                status: Some(PodSandboxStatus {
                    state: PodSandboxState::NotReady,
                    linux: Some(LinuxPodSandboxStatus {
                        namespaces: Some(Namespace {
                            options: Some(full_ns()),
                        }),
                    }),
                    ..pod_sandbox_status().status.unwrap()
                }),
                containers_statuses: vec![full_container_status(), ContainerStatus::default()],
                ..pod_sandbox_status()
            }),
            rt(ListPodSandboxRequest {
                filter: Some(PodSandboxFilter {
                    id: "8e3b".into(),
                    state: Some(PodSandboxStateValue {
                        state: PodSandboxState::Ready,
                    }),
                    label_selector: label_selector.clone(),
                }),
            }),
            rt(ListPodSandboxResponse {
                items: vec![
                    PodSandbox {
                        id: SANDBOX_ID.into(),
                        metadata: Some(sandbox_metadata()),
                        state: PodSandboxState::Other(5),
                        created_at: 1,
                        labels: map(&[("app", "web")]),
                        annotations: map(&[("a", "b")]),
                        runtime_handler: "runc".into(),
                    },
                    PodSandbox::default(),
                ],
            }),
            rt(create_container()),
            rt(CreateContainerRequest {
                pod_sandbox_id: SANDBOX_ID.into(),
                config: Some(full_container_config()),
                sandbox_config: Some(full_sandbox_config()),
            }),
            rt(CreateContainerResponse {
                container_id: "c1".into(),
            }),
            rt(StartContainerRequest {
                container_id: "c1".into(),
            }),
            rt(StartContainerResponse {}),
            rt(StopContainerRequest {
                container_id: "c1".into(),
                timeout: 30,
            }),
            rt(StopContainerResponse {}),
            rt(RemoveContainerRequest {
                container_id: "c1".into(),
            }),
            rt(RemoveContainerResponse {}),
            rt(ListContainersRequest {
                filter: Some(ContainerFilter {
                    id: "c1".into(),
                    state: Some(ContainerStateValue {
                        state: ContainerState::Created,
                    }),
                    pod_sandbox_id: SANDBOX_ID.into(),
                    label_selector: label_selector.clone(),
                }),
            }),
            rt(list_containers()),
            rt(ContainerStatusRequest {
                container_id: "c1".into(),
                verbose: true,
            }),
            rt(ContainerStatusResponse {
                status: Some(full_container_status()),
                info: map(&[("info", "{}")]),
            }),
            rt(ExecSyncRequest {
                container_id: "c1".into(),
                cmd: strings(&["cat", "/etc/hostname"]),
                timeout: 10,
            }),
            rt(ExecSyncResponse {
                stdout: b"web-0\n".to_vec(),
                stderr: vec![0xff],
                exit_code: -2,
            }),
            rt(ListImagesRequest {
                filter: Some(ImageFilter {
                    image: Some(full_image_spec()),
                }),
            }),
            rt(ListImagesResponse {
                images: vec![full_image(), Image::default()],
            }),
            rt(ImageStatusRequest {
                image: Some(full_image_spec()),
                verbose: true,
            }),
            rt(ImageStatusResponse {
                image: Some(full_image()),
                info: map(&[("info", "{}")]),
            }),
            rt(PullImageRequest {
                image: Some(full_image_spec()),
                auth: Some(AuthConfig {
                    username: "u".into(),
                    password: "p".into(),
                    auth: "dTpw".into(),
                    server_address: "registry.example.com".into(),
                    identity_token: "i".into(),
                    registry_token: "r".into(),
                }),
                sandbox_config: Some(full_sandbox_config()),
            }),
            rt(PullImageResponse {
                image_ref: "sha256:def".into(),
            }),
            rt(RemoveImageRequest {
                image: Some(full_image_spec()),
            }),
            rt(RemoveImageResponse {}),
            rt(StatusRequest { verbose: true }),
            rt(status()),
            rt(StatusResponse {
                info: map(&[("config", "{}")]),
                features: Some(RuntimeFeatures {
                    supplemental_groups_policy: true,
                    user_namespaces_host_network: true,
                    mount_options: true,
                }),
                ..status()
            }),
            rt(ImageFsInfoRequest {}),
            rt(ImageFsInfoResponse {
                image_filesystems: vec![fs_usage()],
                container_filesystems: vec![fs_usage(), FilesystemUsage::default()],
            }),
            rt(ContainerStatsRequest {
                container_id: "c1".into(),
            }),
            rt(ContainerStatsResponse {
                stats: Some(full_container_stats()),
            }),
            rt(ListContainerStatsRequest {
                filter: Some(ContainerStatsFilter {
                    id: "c1".into(),
                    pod_sandbox_id: SANDBOX_ID.into(),
                    label_selector: label_selector.clone(),
                }),
            }),
            rt(ListContainerStatsResponse {
                stats: vec![full_container_stats(), ContainerStats::default()],
            }),
            rt(PodSandboxStatsRequest {
                pod_sandbox_id: SANDBOX_ID.into(),
            }),
            rt(PodSandboxStatsResponse {
                stats: Some(full_sandbox_stats()),
            }),
            rt(ListPodSandboxStatsRequest {
                filter: Some(PodSandboxStatsFilter {
                    id: SANDBOX_ID.into(),
                    label_selector,
                }),
            }),
            rt(ListPodSandboxStatsResponse {
                stats: vec![full_sandbox_stats(), PodSandboxStats::default()],
            }),
        ]
        .into_iter()
        .flatten()
        .collect();
        assert!(failures.is_empty(), "{failures:#?}");
    }

    // Produced by the Python protobuf runtime (protobuf 7.36.2, pure-Python
    // backend, deterministic serialization) from cri-api a8dc9a1 api.proto.
    // The upb backend would differ in map order: it writes a key after a
    // longer key that it is a prefix of.
    const RUN_POD_SANDBOX: &str =
        "0af5040a380a057765622d30122436663163326134652d306433622d346335652d396137662d3162\
        326333643465356636301a0764656661756c74200112057765622d301a402f7661722f6c6f672f70\
        6f64732f64656661756c745f7765622d305f36663163326134652d306433622d346335652d396137\
        662d31623263336434653566363022520a0a31302e39362e302e3130121964656661756c742e7376\
        632e636c75737465722e6c6f63616c12117376632e636c75737465722e6c6f63616c120d636c7573\
        7465722e6c6f63616c1a076e646f74733a352a0310903f2a120801103518e92922093132372e302e\
        302e31320a0a036170701203776562321f0a16696f2e6b756265726e657465732e706f642e6e616d\
        6512057765622d3032260a1b696f2e6b756265726e657465732e706f642e6e616d65737061636512\
        0764656661756c74323d0a15696f2e6b756265726e657465732e706f642e75696412243666316332\
        6134652d306433622d346335652d396137662d3162326333643465356636303a150a116578616d70\
        6c652e636f6d2f656d70747912003a3b0a196b756265726e657465732e696f2f636f6e6669672e73\
        65656e121e323032362d31302d32305431303a30303a30302e3030303030303030305a3a220a1b6b\
        756265726e657465732e696f2f636f6e6669672e736f75726365120361706942770a3b2f6b756265\
        706f64732f627572737461626c652f706f6436663163326134652d306433622d346335652d396137\
        662d316232633364346535663630120e0a0210011a002a04e807d00f4a001a280a236e65742e6970\
        76342e69705f756e70726976696c656765645f706f72745f7374617274120130";
    const CREATE_CONTAINER: &str =
        "0a3f3865336235633164326634613662376338643965306631613262336334643565366637303831\
        3932613362346335643665376638303931613262336334643512b4070a070a056e67696e7812560a\
        477368613235363a3362323562363832656138326232646233636334666434386462383138626537\
        383865653366393032616337333738303930636632363234656332343432646692010a6e67696e78\
        3a312e32371a152f646f636b65722d656e747279706f696e742e736822056e67696e7822022d6722\
        0b6461656d6f6e206f66663b2a012f32240a174b554245524e455445535f534552564943455f484f\
        5354120931302e39362e302e3132070a05454d5054593aa3010a2d2f7661722f72756e2f73656372\
        6574732f6b756265726e657465732e696f2f736572766963656163636f756e7412702f7661722f6c\
        69622f6b7562656c65742f706f64732f36663163326134652d306433622d346335652d396137662d\
        3162326333643465356636302f766f6c756d65732f6b756265726e657465732e696f7e70726f6a65\
        637465642f6b7562652d6170692d6163636573732d78376b327018013a520a0a2f6574632f686f73\
        747312442f7661722f6c69622f6b7562656c65742f706f64732f36663163326134652d306433622d\
        346335652d396137662d3162326333643465356636302f6574632d686f7374733a140a052f646174\
        6112092f6d6e742f6461746128014a250a1c696f2e6b756265726e657465732e636f6e7461696e65\
        722e6e616d6512056e67696e784a1f0a16696f2e6b756265726e657465732e706f642e6e616d6512\
        057765622d304a260a1b696f2e6b756265726e657465732e706f642e6e616d657370616365120764\
        656661756c744a3d0a15696f2e6b756265726e657465732e706f642e756964122436663163326134\
        652d306433622d346335652d396137662d31623263336434653566363052280a1c696f2e6b756265\
        726e657465732e636f6e7461696e65722e686173681208613162326333643452290a24696f2e6b75\
        6265726e657465732e636f6e7461696e65722e72657374617274436f756e7412013052460a2e696f\
        2e6b756265726e657465732e636f6e7461696e65722e7465726d696e6174696f6e4d657373616765\
        5061746812142f6465762f7465726d696e6174696f6e2d6c6f675a0b6e67696e782f302e6c6f677a\
        86010a3a08a08d0610d08603188004208080808001289bf8ffffffffffffff0142050a03324d424a\
        150a106d656d6f72792e6f6f6d2e67726f757012013112480a051203414c4c1a0210012a0308e807\
        5801620308b8176a0b2f70726f632f6b636f72656a0a2f70726f632f6b65797372092f70726f632f\
        62757372092f70726f632f7379737a001af5040a380a057765622d30122436663163326134652d30\
        6433622d346335652d396137662d3162326333643465356636301a0764656661756c742001120577\
        65622d301a402f7661722f6c6f672f706f64732f64656661756c745f7765622d305f366631633261\
        34652d306433622d346335652d396137662d31623263336434653566363022520a0a31302e39362e\
        302e3130121964656661756c742e7376632e636c75737465722e6c6f63616c12117376632e636c75\
        737465722e6c6f63616c120d636c75737465722e6c6f63616c1a076e646f74733a352a0310903f2a\
        120801103518e92922093132372e302e302e31320a0a036170701203776562321f0a16696f2e6b75\
        6265726e657465732e706f642e6e616d6512057765622d3032260a1b696f2e6b756265726e657465\
        732e706f642e6e616d657370616365120764656661756c74323d0a15696f2e6b756265726e657465\
        732e706f642e756964122436663163326134652d306433622d346335652d396137662d3162326333\
        643465356636303a150a116578616d706c652e636f6d2f656d70747912003a3b0a196b756265726e\
        657465732e696f2f636f6e6669672e7365656e121e323032362d31302d32305431303a30303a3030\
        2e3030303030303030305a3a220a1b6b756265726e657465732e696f2f636f6e6669672e736f7572\
        6365120361706942770a3b2f6b756265706f64732f627572737461626c652f706f64366631633261\
        34652d306433622d346335652d396137662d316232633364346535663630120e0a0210011a002a04\
        e807d00f4a001a280a236e65742e697076342e69705f756e70726976696c656765645f706f72745f\
        7374617274120130";
    const LIST_CONTAINERS: &str =
        "0adf010a06633161326233123f386533623563316432663461366237633864396530663161326233\
        6334643565366637303831393261336234633564366537663830393161326233633464351a070a05\
        6e67696e78220f0a0d7368613235363a3362323562362a0d7368613235363a336232356236300138\
        959a94e8a6d68ab81842250a1c696f2e6b756265726e657465732e636f6e7461696e65722e6e616d\
        6512056e67696e784a290a24696f2e6b756265726e657465732e636f6e7461696e65722e72657374\
        617274436f756e74120130520d7368613235363a3362323562360aba010a06643465356636123f38\
        65336235633164326634613662376338643965306631613262336334643565366637303831393261\
        336234633564366537663830393161326233633464351a0b0a07696e69742d64621002220f0a0d73\
        68613235363a3966386537642a0d7368613235363a39663865376430023880b0c9e9b1d38ab81842\
        270a1c696f2e6b756265726e657465732e636f6e7461696e65722e6e616d651207696e69742d6462\
        520d7368613235363a396638653764";
    const POD_SANDBOX_STATUS: &str =
        "0ae5010a3f3865336235633164326634613662376338643965306631613262336334643565366637\
        3038313932613362346335643665376638303931613262336334643512380a057765622d30122436\
        663163326134652d306433622d346335652d396137662d3162326333643465356636301a07646566\
        61756c742001208080a5ada6d68ab8182a200a0a31302e3234342e312e3712120a10666430303a31\
        303a3234343a313a3a3732060a04120210013a0a0a03617070120377656242220a1b6b756265726e\
        657465732e696f2f636f6e6669672e736f7572636512036170694a0472756e6312250a04696e666f\
        121d7b22706964223a343234322c2272756e74696d6553706563223a7b7d7d2080b0c2ef85d88ab8\
        18";
    const STATUS: &str =
        "0a730a100a0c52756e74696d65526561647910010a5f0a0c4e6574776f726b52656164791a154e65\
        74776f726b506c7567696e4e6f74526561647922384e6574776f726b20706c7567696e2072657475\
        726e73206572726f723a20636e6920706c7567696e206e6f7420696e697469616c697a65641a0612\
        04080110011a0a0a0472756e631202080122020801";

    fn golden<M: Message + PartialEq + Debug>(m: &M, hex: &str) {
        let want = unhex(hex);
        assert_eq!(m.encode(), want, "{}", type_name::<M>());
        assert_eq!(M::decode(&want).as_ref(), Ok(m));
    }

    #[test]
    fn bytes_match_the_reference_implementation() {
        golden(&run_pod_sandbox(), RUN_POD_SANDBOX);
        golden(&create_container(), CREATE_CONTAINER);
        golden(&list_containers(), LIST_CONTAINERS);
        golden(&pod_sandbox_status(), POD_SANDBOX_STATUS);
        golden(&status(), STATUS);
    }

    fn parses_completely(buf: &[u8]) -> bool {
        let mut r = Reader::new(buf);
        loop {
            match r.next_field() {
                Ok(Some(_)) => {}
                Ok(None) => return true,
                Err(_) => return false,
            }
        }
    }

    // A prefix that ends between two fields is a valid shorter message;
    // any other prefix must be an error.
    fn every_prefix<M: Message>(full: &[u8]) {
        for cut in 0..full.len() {
            let prefix = &full[..cut];
            assert_eq!(
                M::decode(prefix).is_ok(),
                parses_completely(prefix),
                "{} cut at {cut}",
                type_name::<M>()
            );
        }
    }

    // The same, one level down: the embedded message in `field` is cut
    // while the outer length still matches it.
    fn every_inner_prefix<M: Message>(full: &[u8], field: u32) {
        let mut r = Reader::new(full);
        let inner = loop {
            match r.next_field().unwrap() {
                Some((n, Value::Len(b))) if n == field => break b,
                Some(_) => {}
                None => panic!("no field {field}"),
            }
        };
        for cut in 0..inner.len() {
            let mut outer = Vec::new();
            put_message(&mut outer, field, &inner[..cut]);
            assert_eq!(
                M::decode(&outer).is_ok(),
                parses_completely(&inner[..cut]),
                "{} field {field} cut at {cut}",
                type_name::<M>()
            );
        }
    }

    #[test]
    fn truncated_input_is_an_error() {
        let run = unhex(RUN_POD_SANDBOX);
        let create = unhex(CREATE_CONTAINER);
        let list = unhex(LIST_CONTAINERS);
        let pod_status = unhex(POD_SANDBOX_STATUS);
        let status = unhex(STATUS);
        every_prefix::<RunPodSandboxRequest>(&run);
        every_prefix::<CreateContainerRequest>(&create);
        every_prefix::<ListContainersResponse>(&list);
        every_prefix::<PodSandboxStatusResponse>(&pod_status);
        every_prefix::<StatusResponse>(&status);
        every_inner_prefix::<RunPodSandboxRequest>(&run, 1);
        every_inner_prefix::<CreateContainerRequest>(&create, 2);
        every_inner_prefix::<CreateContainerRequest>(&create, 3);
        every_inner_prefix::<ListContainersResponse>(&list, 1);
        every_inner_prefix::<PodSandboxStatusResponse>(&pod_status, 1);
        every_inner_prefix::<StatusResponse>(&status, 1);
        assert_eq!(
            RunPodSandboxRequest::decode(&run[..run.len() - 1]),
            Err(DecodeError::Truncated)
        );
    }

    #[test]
    fn decoding_is_lenient_where_protobuf_requires_it() {
        let mut groups = Vec::new();
        put_uint(&mut groups, 5, 7);
        put_message(&mut groups, 5, &[8, 9]);
        let mut sc = Vec::new();
        put_message(&mut sc, 2, &groups);
        let mut bytes = Vec::new();
        put_bytes(&mut bytes, 2, b"web-0");
        put_message(&mut bytes, 8, &sc);
        // Windows settings, which are not modeled, and a field from a
        // future version.
        put_message(&mut bytes, 9, &[0x0a, 0x00]);
        put_uint(&mut bytes, 99, 1);
        let mut no_value = Vec::new();
        put_bytes(&mut no_value, 1, b"k");
        put_message(&mut bytes, 6, &no_value);
        put_message(&mut bytes, 6, &[0x12, 0x01, b'x']);
        put_message(&mut bytes, 7, &no_value);
        put_message(&mut bytes, 7, &[0x0a, 0x01, b'k', 0x12, 0x01, b'v']);
        let got = PodSandboxConfig::decode(&bytes).unwrap();
        assert_eq!(got.hostname, "web-0");
        assert_eq!(got.labels, map(&[("k", ""), ("", "x")]));
        assert_eq!(got.annotations, map(&[("k", "v")]));
        let sc = got.linux.unwrap().security_context.unwrap();
        assert_eq!(sc.supplemental_groups, [7, 8, 9]);

        let mut bad = Vec::new();
        put_uint(&mut bad, 2, 1);
        assert_eq!(
            PodSandboxConfig::decode(&bad),
            Err(DecodeError::WrongWireType(2))
        );
        let mut bad = Vec::new();
        put_message(&mut bad, 6, &[0x0a, 0x01, 0xff]);
        assert_eq!(
            PodSandboxConfig::decode(&bad),
            Err(DecodeError::WrongWireType(1))
        );
    }

    #[test]
    fn registry_secrets_stay_out_of_debug_output() {
        let auth = AuthConfig {
            username: "robot".into(),
            password: "hunter2".into(),
            registry_token: "tok3n".into(),
            ..Default::default()
        };
        let shown = format!("{auth:?}");
        assert!(shown.contains("robot"));
        assert!(
            !shown.contains("hunter2") && !shown.contains("tok3n"),
            "{shown}"
        );
    }
}
