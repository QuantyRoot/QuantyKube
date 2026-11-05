//! A pod's network set up by the runtime itself, in place of the CNI
//! plugins flannel's configuration names (ADR-042): a veth pair, one end
//! in the pod as eth0, the other on the node's bridge, and an address
//! from the node's subnet, with rtnetlink messages and no programs run.
//!
//! What it sets up is what flannel's plugin and the `bridge` and
//! `host-local` plugins it hands over to set up: the bridge `cni0` with
//! the subnet's first address as the pods' gateway, hairpin mode on the
//! pod's port so a pod reaches itself through a service, flannel's MTU,
//! a default route. Masquerading stays with flanneld, as it does there.

use quantycore::json::{Object, Value};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const BRIDGE: &str = "cni0";
const SUBNET_ENV: &str = "/run/flannel/subnet.env";

const RTM_NEWLINK: u16 = 16;
const RTM_DELLINK: u16 = 17;
const RTM_GETLINK: u16 = 18;
const RTM_NEWADDR: u16 = 20;
const RTM_NEWROUTE: u16 = 24;
const NLMSG_ERROR: u16 = 2;
const NLMSG_DONE: u16 = 3;
const NLM_F_REQUEST: u16 = 1;
const NLM_F_ACK: u16 = 4;
const NLM_F_EXCL: u16 = 0x200;
const NLM_F_CREATE: u16 = 0x400;
const IFLA_IFNAME: u16 = 3;
const IFLA_MTU: u16 = 4;
const IFLA_MASTER: u16 = 10;
const IFLA_LINKINFO: u16 = 18;
const IFLA_NET_NS_FD: u16 = 28;
const IFLA_INFO_KIND: u16 = 1;
const IFLA_INFO_DATA: u16 = 2;
const VETH_INFO_PEER: u16 = 1;
const IFA_ADDRESS: u16 = 1;
const IFA_LOCAL: u16 = 2;
const RTA_GATEWAY: u16 = 5;
const RTA_OIF: u16 = 4;
const AF_INET: u8 = 2;
const IFF_UP: u32 = 1;
const RT_TABLE_MAIN: u8 = 254;
const RTPROT_BOOT: u8 = 3;
const RTN_UNICAST: u8 = 1;
const NESTED: u16 = 0x8000;

/// The node's share of the pod network, as flanneld writes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subnet {
    pub gateway: Ipv4Addr,
    pub prefix: u8,
    pub mtu: u32,
}

impl Subnet {
    /// `FLANNEL_SUBNET=10.244.1.1/24` and `FLANNEL_MTU=1450` from the
    /// text of subnet.env.
    pub fn parse(text: &str) -> Option<Subnet> {
        let value = |k: &str| {
            text.lines()
                .find_map(|l| l.trim().strip_prefix(k)?.strip_prefix('='))
                .map(str::trim)
        };
        let (addr, prefix) = value("FLANNEL_SUBNET")?.split_once('/')?;
        let prefix: u8 = prefix.parse().ok().filter(|p| (8..=30).contains(p))?;
        Some(Subnet {
            gateway: addr.parse().ok()?,
            prefix,
            mtu: value("FLANNEL_MTU")
                .and_then(|m| m.parse().ok())
                .unwrap_or(1500),
        })
    }

    fn mask(&self) -> u32 {
        u32::MAX << (32 - self.prefix)
    }

    /// Every address a pod may get: the subnet without its network and
    /// broadcast addresses and without the gateway.
    fn hosts(&self) -> impl Iterator<Item = Ipv4Addr> + '_ {
        let net = u32::from(self.gateway) & self.mask();
        let last = net | !self.mask();
        (net + 1..last)
            .map(Ipv4Addr::from)
            .filter(move |a| *a != self.gateway)
    }
}

/// Where a pod network's subnet comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// flanneld's subnet.env, read for every pod: flanneld writes it
    /// once the node has its lease, which may be after the runtime
    /// started
    Flannel(PathBuf),
    /// a bridge plugin configuration with a host-local subnet
    Fixed(Subnet),
}

/// The bridge and the subnet of a CNI configuration this module can
/// stand in for, or None: flannel's, or a bridge plugin with one
/// host-local subnet, the bridge as the gateway and no masquerading,
/// each followed by nothing but portmap, which pods with host ports
/// still get from the plugins.
pub fn stand_in(plugins: &[Object]) -> Option<(String, Source)> {
    let first = plugins.first()?;
    let rest_ok = plugins[1..]
        .iter()
        .all(|p| p.get("type").and_then(Value::as_str) == Some("portmap"));
    if !rest_ok {
        return None;
    }
    let str_at = |v: &Value, p: &str| v.pointer(p).and_then(Value::as_str).map(str::to_string);
    let first = Value::Object(first.clone());
    match str_at(&first, "/type")?.as_str() {
        "flannel" => {
            let bridge = str_at(&first, "/delegate/bridge").unwrap_or_else(|| BRIDGE.into());
            Some((bridge, Source::Flannel(SUBNET_ENV.into())))
        }
        "bridge" => {
            let yes = |p: &str| first.pointer(p).and_then(Value::as_bool) == Some(true);
            if yes("/ipMasq") || !yes("/isGateway") {
                return None;
            }
            if str_at(&first, "/ipam/type")? != "host-local" {
                return None;
            }
            let ranges = first.pointer("/ipam/ranges")?.as_array()?;
            let range = ranges.first()?.as_array()?;
            if ranges.len() != 1 || range.len() != 1 {
                return None;
            }
            let r = range[0].as_object()?;
            if r.get("rangeStart").is_some() || r.get("rangeEnd").is_some() {
                return None;
            }
            let (net, prefix) = r.get("subnet")?.as_str()?.split_once('/')?;
            let net: Ipv4Addr = net.parse().ok()?;
            let prefix: u8 = prefix.parse().ok().filter(|p| (8..=30).contains(p))?;
            let gateway = match r.get("gateway").and_then(Value::as_str) {
                Some(g) => g.parse().ok()?,
                None => Ipv4Addr::from(u32::from(net) + 1),
            };
            let mtu = first
                .pointer("/mtu")
                .and_then(Value::as_u64)
                .unwrap_or(1500) as u32;
            let bridge = str_at(&first, "/bridge").unwrap_or_else(|| "cni0".into());
            Some((
                bridge,
                Source::Fixed(Subnet {
                    gateway,
                    prefix,
                    mtu,
                }),
            ))
        }
        _ => None,
    }
}

/// Addresses handed out, one file per address under the runtime's
/// directory, as host-local keeps them, so a restarted runtime knows
/// them; a mutex instead of a file lock between pods starting at once.
pub struct Ipam {
    dir: PathBuf,
    /// where host-local keeps the addresses it handed out, skipped here
    cni_dir: PathBuf,
    lock: Mutex<()>,
}

impl Ipam {
    pub fn new(dir: &Path, cni_dir: &Path) -> io::Result<Ipam> {
        fs::create_dir_all(dir)?;
        Ok(Ipam {
            dir: dir.into(),
            cni_dir: cni_dir.into(),
            lock: Mutex::new(()),
        })
    }

    fn taken(&self) -> BTreeSet<String> {
        let mut taken = BTreeSet::new();
        for d in [&self.dir, &self.cni_dir] {
            if let Ok(rd) = fs::read_dir(d) {
                for e in rd.flatten() {
                    taken.insert(e.file_name().to_string_lossy().into_owned());
                }
            }
        }
        taken
    }

    pub fn claim(&self, subnet: &Subnet, pod: &str) -> io::Result<Ipv4Addr> {
        let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let taken = self.taken();
        for a in subnet.hosts() {
            if taken.contains(&a.to_string()) {
                continue;
            }
            match File::create_new(self.dir.join(a.to_string())) {
                Ok(mut f) => {
                    f.write_all(pod.as_bytes())?;
                    return Ok(a);
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(io::Error::other(format!(
            "no address left in {}/{}",
            subnet.gateway, subnet.prefix
        )))
    }

    /// Releases what `pod` holds; nothing if it holds nothing.
    pub fn release(&self, pod: &str) -> io::Result<()> {
        let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        for e in fs::read_dir(&self.dir)?.flatten() {
            if fs::read(e.path()).is_ok_and(|b| b == pod.as_bytes()) {
                fs::remove_file(e.path())?;
            }
        }
        Ok(())
    }
}

/// One rtnetlink message under construction.
struct Msg {
    buf: Vec<u8>,
    /// where open nested attributes start
    open: Vec<usize>,
}

fn pad(n: usize) -> usize {
    (n + 3) & !3
}

impl Msg {
    fn new(kind: u16, flags: u16, header: &[u8]) -> Msg {
        let mut buf = vec![0u8; 16];
        buf[4..6].copy_from_slice(&kind.to_ne_bytes());
        buf[6..8].copy_from_slice(&(flags | NLM_F_REQUEST | NLM_F_ACK).to_ne_bytes());
        buf.extend_from_slice(header);
        buf.resize(pad(buf.len()), 0);
        Msg {
            buf,
            open: Vec::new(),
        }
    }

    fn attr(&mut self, kind: u16, data: &[u8]) -> &mut Msg {
        let len = 4 + data.len();
        self.buf.extend_from_slice(&(len as u16).to_ne_bytes());
        self.buf.extend_from_slice(&kind.to_ne_bytes());
        self.buf.extend_from_slice(data);
        self.buf.resize(pad(self.buf.len()), 0);
        self
    }

    fn str(&mut self, kind: u16, s: &str) -> &mut Msg {
        let mut b = s.as_bytes().to_vec();
        b.push(0);
        self.attr(kind, &b)
    }

    fn u32(&mut self, kind: u16, v: u32) -> &mut Msg {
        self.attr(kind, &v.to_ne_bytes())
    }

    fn begin(&mut self, kind: u16) -> &mut Msg {
        self.open.push(self.buf.len());
        self.attr(kind | NESTED, &[])
    }

    /// Raw bytes inside the attribute open now, such as the header a
    /// veth peer carries.
    fn raw(&mut self, data: &[u8]) -> &mut Msg {
        self.buf.extend_from_slice(data);
        self.buf.resize(pad(self.buf.len()), 0);
        self
    }

    fn end(&mut self) -> &mut Msg {
        let at = self.open.pop().expect("an open attribute");
        let len = (self.buf.len() - at) as u16;
        self.buf[at..at + 2].copy_from_slice(&len.to_ne_bytes());
        self
    }

    fn bytes(&mut self, seq: u32) -> &[u8] {
        let len = self.buf.len() as u32;
        self.buf[0..4].copy_from_slice(&len.to_ne_bytes());
        self.buf[8..12].copy_from_slice(&seq.to_ne_bytes());
        &self.buf
    }
}

fn ifinfo(index: i32, flags: u32, change: u32) -> [u8; 16] {
    let mut h = [0u8; 16];
    h[4..8].copy_from_slice(&index.to_ne_bytes());
    h[8..12].copy_from_slice(&flags.to_ne_bytes());
    h[12..16].copy_from_slice(&change.to_ne_bytes());
    h
}

/// An rtnetlink socket in one network namespace.
struct Route {
    sock: File,
    seq: u32,
}

impl Route {
    fn open(netns: Option<&Path>) -> io::Result<Route> {
        Ok(Route {
            sock: quantykube_sys::route_socket(netns)?,
            seq: 0,
        })
    }

    /// Sends `msg` and returns the messages that answer it, the
    /// acknowledgement left out; a kernel error becomes an io::Error.
    fn ask(&mut self, msg: &mut Msg, what: &str) -> io::Result<Vec<(u16, Vec<u8>)>> {
        self.seq += 1;
        self.sock.write_all(msg.bytes(self.seq))?;
        let mut out = Vec::new();
        let mut buf = vec![0u8; 65536];
        loop {
            let n = self.sock.read(&mut buf)?;
            let mut at = 0;
            while at + 16 <= n {
                let len = u32::from_ne_bytes(buf[at..at + 4].try_into().unwrap()) as usize;
                let kind = u16::from_ne_bytes(buf[at + 4..at + 6].try_into().unwrap());
                let seq = u32::from_ne_bytes(buf[at + 8..at + 12].try_into().unwrap());
                if len < 16 || at + len > n {
                    return Err(io::Error::other(format!("{what}: a truncated reply")));
                }
                let body = buf[at + 16..at + len].to_vec();
                at += pad(len);
                if seq != self.seq {
                    continue;
                }
                match kind {
                    NLMSG_ERROR => {
                        let code = i32::from_ne_bytes(body[0..4].try_into().unwrap());
                        if code == 0 {
                            return Ok(out);
                        }
                        let e = io::Error::from_raw_os_error(-code);
                        // ENODEV is how the kernel says a link is not there
                        let kind = if -code == 19 {
                            io::ErrorKind::NotFound
                        } else {
                            e.kind()
                        };
                        return Err(io::Error::new(kind, format!("{what}: {e}")));
                    }
                    NLMSG_DONE => return Ok(out),
                    _ => out.push((kind, body)),
                }
            }
        }
    }

    /// The index of the link named `name`.
    fn index(&mut self, name: &str) -> io::Result<i32> {
        let mut m = Msg::new(RTM_GETLINK, 0, &ifinfo(0, 0, 0));
        m.str(IFLA_IFNAME, name);
        let replies = self.ask(&mut m, &format!("finding {name}"))?;
        replies
            .iter()
            .find(|(k, b)| *k == RTM_NEWLINK && b.len() >= 8)
            .map(|(_, b)| i32::from_ne_bytes(b[4..8].try_into().unwrap()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("no link {name}")))
    }

    fn up(&mut self, index: i32, master: Option<i32>, what: &str) -> io::Result<()> {
        let mut m = Msg::new(RTM_NEWLINK, 0, &ifinfo(index, IFF_UP, IFF_UP));
        if let Some(b) = master {
            m.u32(IFLA_MASTER, b as u32);
        }
        self.ask(&mut m, what).map(drop)
    }

    fn address(&mut self, index: i32, addr: Ipv4Addr, prefix: u8, what: &str) -> io::Result<()> {
        let mut h = [0u8; 8];
        h[0] = AF_INET;
        h[1] = prefix;
        h[4..8].copy_from_slice(&(index as u32).to_ne_bytes());
        let mut m = Msg::new(RTM_NEWADDR, NLM_F_CREATE | NLM_F_EXCL, &h);
        m.attr(IFA_LOCAL, &addr.octets())
            .attr(IFA_ADDRESS, &addr.octets());
        match self.ask(&mut m, what) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            r => r.map(drop),
        }
    }

    fn default_route(&mut self, index: i32, gateway: Ipv4Addr) -> io::Result<()> {
        let h = [
            AF_INET,
            0,
            0,
            0,
            RT_TABLE_MAIN,
            RTPROT_BOOT,
            0,
            RTN_UNICAST,
            0,
            0,
            0,
            0,
        ];
        let mut m = Msg::new(RTM_NEWROUTE, NLM_F_CREATE | NLM_F_EXCL, &h);
        m.attr(RTA_GATEWAY, &gateway.octets())
            .u32(RTA_OIF, index as u32);
        self.ask(&mut m, "the pod's default route").map(drop)
    }
}

fn host_name(pod: &str) -> String {
    // links have at most 15 characters, and the pod id is hex
    format!("qk{}", &pod[..pod.len().min(12)])
}

/// The node's pod network without plugins.
pub struct PodNet {
    source: Source,
    bridge: String,
    pub ipam: Ipam,
}

impl PodNet {
    /// The network a CNI configuration named `cni_name` describes,
    /// addresses kept under `runtime_dir`.
    pub fn new(
        runtime_dir: &Path,
        cni_name: &str,
        bridge: &str,
        source: Source,
    ) -> io::Result<PodNet> {
        Ok(PodNet {
            source,
            bridge: bridge.into(),
            ipam: Ipam::new(
                &runtime_dir.join("ipam"),
                &Path::new("/var/lib/cni/networks").join(cni_name),
            )?,
        })
    }

    #[cfg(test)]
    fn with(subnet_env: &Path, bridge: &str, ipam: Ipam) -> PodNet {
        PodNet {
            source: Source::Flannel(subnet_env.into()),
            bridge: bridge.into(),
            ipam,
        }
    }

    fn subnet(&self) -> io::Result<Subnet> {
        match &self.source {
            Source::Fixed(s) => Ok(s.clone()),
            Source::Flannel(env) => {
                let text = fs::read_to_string(env)?;
                Subnet::parse(&text).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("{}: no FLANNEL_SUBNET", env.display()),
                    )
                })
            }
        }
    }

    /// The bridge, made with the gateway's address if flannel's bridge
    /// plugin has not made it yet.
    fn bridge(&self, host: &mut Route, subnet: &Subnet) -> io::Result<i32> {
        let index = match host.index(&self.bridge) {
            Ok(i) => i,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let mut m = Msg::new(RTM_NEWLINK, NLM_F_CREATE | NLM_F_EXCL, &ifinfo(0, 0, 0));
                m.str(IFLA_IFNAME, &self.bridge).u32(IFLA_MTU, subnet.mtu);
                m.begin(IFLA_LINKINFO).str(IFLA_INFO_KIND, "bridge").end();
                match host.ask(&mut m, "making the bridge") {
                    Err(e) if e.kind() != io::ErrorKind::AlreadyExists => return Err(e),
                    _ => {}
                }
                host.index(&self.bridge)?
            }
            Err(e) => return Err(e),
        };
        host.address(index, subnet.gateway, subnet.prefix, "the bridge's address")?;
        host.up(index, None, "the bridge")?;
        Ok(index)
    }

    /// Attaches the pod whose network namespace is held at `netns` and
    /// returns a result in the shape a CNI plugin returns, so that the
    /// rest of the runtime reads the address the same way.
    pub fn add(&self, pod: &str, netns: &Path) -> io::Result<Value> {
        let subnet = self.subnet()?;
        let ip = self.ipam.claim(&subnet, pod)?;
        let attached = self.attach(pod, netns, &subnet, ip);
        if attached.is_err() {
            let _ = self.del(pod);
        }
        attached?;
        let mut a = Object::default();
        a.insert("address", Value::from(format!("{ip}/{}", subnet.prefix)));
        a.insert("gateway", Value::from(subnet.gateway.to_string()));
        a.insert("interface", Value::from(1u64));
        let mut iface = Object::default();
        iface.insert("name", Value::from("eth0"));
        iface.insert("sandbox", Value::from(netns.to_string_lossy().into_owned()));
        let mut r = Object::default();
        r.insert("cniVersion", Value::from("1.0.0"));
        r.insert("interfaces", Value::Array(vec![Value::Object(iface)]));
        r.insert("ips", Value::Array(vec![Value::Object(a)]));
        r.insert("quantykube", Value::from(true));
        Ok(Value::Object(r))
    }

    fn attach(&self, pod: &str, netns: &Path, subnet: &Subnet, ip: Ipv4Addr) -> io::Result<()> {
        let mut host = Route::open(None)?;
        let bridge = self.bridge(&mut host, subnet)?;
        let name = host_name(pod);
        let ns = File::open(netns)?;
        {
            use std::os::fd::AsRawFd;
            let mut m = Msg::new(RTM_NEWLINK, NLM_F_CREATE | NLM_F_EXCL, &ifinfo(0, 0, 0));
            m.str(IFLA_IFNAME, &name).u32(IFLA_MTU, subnet.mtu);
            m.begin(IFLA_LINKINFO)
                .str(IFLA_INFO_KIND, "veth")
                .begin(IFLA_INFO_DATA)
                .begin(VETH_INFO_PEER)
                .raw(&ifinfo(0, 0, 0));
            m.str(IFLA_IFNAME, "eth0")
                .u32(IFLA_MTU, subnet.mtu)
                .u32(IFLA_NET_NS_FD, ns.as_raw_fd() as u32);
            m.end().end().end();
            host.ask(&mut m, "making the pod's veth pair")?;
        }
        let index = host.index(&name)?;
        host.up(index, Some(bridge), "the pod's port on the bridge")?;
        // a pod reaches itself through a service only if the bridge may
        // send its packets back out of the port they came in on
        fs::write(format!("/sys/class/net/{name}/brport/hairpin_mode"), "1")?;
        let mut inside = Route::open(Some(netns))?;
        let eth0 = inside.index("eth0")?;
        inside.address(eth0, ip, subnet.prefix, "the pod's address")?;
        inside.up(eth0, None, "eth0")?;
        inside.default_route(eth0, subnet.gateway)
    }

    /// Detaches the pod: its end on the host goes, which takes eth0 with
    /// it, and its address is free again. A pod already gone is fine.
    pub fn del(&self, pod: &str) -> io::Result<()> {
        detach(pod, &self.ipam)
    }
}

/// [`PodNet::del`] for a runtime that has no configuration to make a
/// PodNet from any more, as after a restart.
pub fn detach_under(runtime_dir: &Path, pod: &str) -> io::Result<()> {
    detach(pod, &Ipam::new(&runtime_dir.join("ipam"), Path::new(""))?)
}

fn detach(pod: &str, ipam: &Ipam) -> io::Result<()> {
    let name = host_name(pod);
    let mut host = Route::open(None)?;
    match host.index(&name) {
        Ok(index) => {
            let mut m = Msg::new(RTM_DELLINK, 0, &ifinfo(index, 0, 0));
            match host.ask(&mut m, "removing the pod's veth pair") {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    ipam.release(pod)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn subnet_env_as_flanneld_writes_it() {
        let s = Subnet::parse(
            "FLANNEL_NETWORK=10.244.0.0/16\nFLANNEL_SUBNET=10.244.1.1/24\n\
             FLANNEL_MTU=1450\nFLANNEL_IPMASQ=true\n",
        )
        .unwrap();
        assert_eq!(
            s,
            Subnet {
                gateway: Ipv4Addr::new(10, 244, 1, 1),
                prefix: 24,
                mtu: 1450
            }
        );
        let hosts: Vec<_> = s.hosts().collect();
        assert_eq!(hosts.len(), 253);
        assert_eq!(hosts[0], Ipv4Addr::new(10, 244, 1, 2));
        assert_eq!(hosts[252], Ipv4Addr::new(10, 244, 1, 254));
        assert!(Subnet::parse("FLANNEL_NETWORK=10.244.0.0/16\n").is_none());
    }

    #[test]
    fn what_is_stood_in_for() {
        let conf = |text: &str| -> Vec<Object> {
            let v = quantycore::json::parse(text).unwrap();
            v.as_array()
                .unwrap()
                .iter()
                .map(|p| p.as_object().unwrap().clone())
                .collect()
        };
        let flannel = r#"[{"type":"flannel","delegate":{"hairpinMode":true}},
            {"type":"portmap","capabilities":{"portMappings":true}}]"#;
        assert_eq!(
            stand_in(&conf(flannel)),
            Some(("cni0".into(), Source::Flannel(SUBNET_ENV.into())))
        );
        let bridge = r#"[{"type":"bridge","bridge":"br9","isGateway":true,"mtu":1400,
            "ipam":{"type":"host-local","ranges":[[{"subnet":"10.9.0.0/24"}]]}}]"#;
        assert_eq!(
            stand_in(&conf(bridge)),
            Some((
                "br9".into(),
                Source::Fixed(Subnet {
                    gateway: Ipv4Addr::new(10, 9, 0, 1),
                    prefix: 24,
                    mtu: 1400
                })
            ))
        );
        // what it would do differently from the plugins
        for other in [
            bridge.replace("\"isGateway\":true", "\"isGateway\":true,\"ipMasq\":true"),
            bridge.replace("host-local", "dhcp"),
            bridge.replace("\"subnet\"", "\"rangeStart\":\"10.9.0.9\",\"subnet\""),
            flannel.replace("portmap", "bandwidth"),
            r#"[{"type":"calico"}]"#.to_string(),
        ] {
            assert_eq!(stand_in(&conf(&other)), None, "{other}");
        }
    }

    fn is_root() -> bool {
        fs::read_to_string("/proc/self/status")
            .map(|s| {
                s.lines()
                    .any(|l| l.starts_with("Uid:") && l.split_whitespace().nth(1) == Some("0"))
            })
            .unwrap_or(false)
    }

    #[test]
    fn addresses_are_handed_out_once_and_come_back() {
        let base = std::env::temp_dir().join(format!("qk-ipam-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(base.join("cni")).unwrap();
        // one address host-local still holds
        fs::write(base.join("cni/10.99.0.3"), "other").unwrap();
        let ipam = Ipam::new(&base.join("ipam"), &base.join("cni")).unwrap();
        let s = Subnet::parse("FLANNEL_SUBNET=10.99.0.1/29\n").unwrap();
        let got: Vec<_> = ["a", "b", "c", "d"]
            .iter()
            .map(|p| ipam.claim(&s, p).unwrap().to_string())
            .collect();
        assert_eq!(got, ["10.99.0.2", "10.99.0.4", "10.99.0.5", "10.99.0.6"]);
        assert!(ipam.claim(&s, "e").is_err(), "a /29 has five");
        ipam.release("b").unwrap();
        assert_eq!(ipam.claim(&s, "e").unwrap().to_string(), "10.99.0.4");
        fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn a_pod_reaches_its_gateway_and_a_neighbour() {
        if !is_root() || Command::new("ping").arg("-V").output().is_err() {
            eprintln!("not root or no ping, pod network test skipped");
            return;
        }
        let base = std::env::temp_dir().join(format!("qk-podnet-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        // a subnet nothing else on a test machine uses
        fs::write(
            base.join("subnet.env"),
            "FLANNEL_SUBNET=10.213.7.1/24\nFLANNEL_MTU=1400\n",
        )
        .unwrap();
        let net = PodNet::with(
            &base.join("subnet.env"),
            "qktest0",
            Ipam::new(&base.join("ipam"), &base.join("none")).unwrap(),
        );
        let pods: Vec<(String, PathBuf)> = ["aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb"]
            .iter()
            .map(|id| {
                let dir = base.join(id);
                fs::create_dir_all(&dir).unwrap();
                quantykube_sys::hold_namespaces(&dir, b"t", false, true).unwrap();
                (id.to_string(), dir.join("net"))
            })
            .collect();
        let mut results = Vec::new();
        for (id, ns) in &pods {
            results.push(net.add(id, ns).unwrap());
        }
        let ip = |r: &Value| crate::cni::ip(r).unwrap();
        assert_eq!(ip(&results[0]), "10.213.7.2");
        assert_eq!(ip(&results[1]), "10.213.7.3");
        let ping = |ns: &Path, to: &str| {
            Command::new("nsenter")
                .arg(format!("--net={}", ns.display()))
                .args(["ping", "-c1", "-W2", to])
                .output()
                .unwrap()
                .status
                .success()
        };
        let mtu = fs::read_to_string(format!("/sys/class/net/{}/mtu", host_name(&pods[0].0)));
        assert_eq!(mtu.unwrap().trim(), "1400");
        assert!(ping(&pods[0].1, "10.213.7.1"), "the gateway");
        assert!(ping(&pods[0].1, "10.213.7.3"), "the other pod");
        net.del(&pods[0].0).unwrap();
        assert!(!Path::new(&format!("/sys/class/net/{}", host_name(&pods[0].0))).exists());
        assert!(!ping(&pods[1].1, "10.213.7.2"), "a removed pod");
        net.del(&pods[0].0).unwrap();
        net.del(&pods[1].0).unwrap();
        for (_, ns) in &pods {
            quantykube_sys::release_namespaces(ns.parent().unwrap()).unwrap();
        }
        let mut host = Route::open(None).unwrap();
        if let Ok(i) = host.index("qktest0") {
            let mut m = Msg::new(RTM_DELLINK, 0, &ifinfo(i, 0, 0));
            let _ = host.ask(&mut m, "test bridge");
        }
        fs::remove_dir_all(&base).unwrap();
    }
}
