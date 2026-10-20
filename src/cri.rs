//! The kubelet's two gRPC services, RuntimeService and ImageService, on
//! top of [`Runtime`].

use crate::cgroup;
use crate::cripb;
use crate::runtime::{self, ContainerSpec, PodSpec, Runtime};
use quantycore::grpc::{code, Responder, Service, Status};
use quantycore::proto::DecodeError;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// what the kubelet takes as a probe that timed out
const DEADLINE_EXCEEDED: u32 = 4;

pub struct Cri {
    rt: Arc<Runtime>,
    registries: crate::registry::Options,
}

impl Cri {
    pub fn new(rt: Arc<Runtime>, registries: crate::registry::Options) -> Arc<Cri> {
        Arc::new(Cri { rt, registries })
    }
}

fn bad_request(e: DecodeError) -> Status {
    Status::new(code::INVALID_ARGUMENT, format!("malformed request: {e:?}"))
}

fn status(e: runtime::Error) -> Status {
    match e {
        runtime::Error::NotFound(_) => Status::new(code::NOT_FOUND, e.to_string()),
        runtime::Error::Invalid(_) => Status::new(code::INVALID_ARGUMENT, e.to_string()),
        runtime::Error::Timeout => Status::new(DEADLINE_EXCEEDED, e.to_string()),
        runtime::Error::Io(_) => Status::new(code::INTERNAL, e.to_string()),
    }
}

fn unsupported(what: &str) -> Status {
    Status::new(
        code::INVALID_ARGUMENT,
        format!("{what} is not supported yet"),
    )
}

fn pairs(m: &cripb::Map) -> Vec<(String, String)> {
    m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn map(v: &[(String, String)]) -> cripb::Map {
    v.iter().cloned().collect()
}

fn selected(labels: &[(String, String)], selector: &cripb::Map) -> bool {
    selector
        .iter()
        .all(|(k, v)| labels.iter().any(|(lk, lv)| lk == k && lv == v))
}

fn pod_metadata(p: &PodSpec) -> cripb::PodSandboxMetadata {
    cripb::PodSandboxMetadata {
        name: p.name.clone(),
        uid: p.uid.clone(),
        namespace: p.namespace.clone(),
        attempt: p.attempt,
    }
}

fn pod_state(s: runtime::PodState) -> cripb::PodSandboxState {
    match s {
        runtime::PodState::Ready => cripb::PodSandboxState::Ready,
        runtime::PodState::NotReady => cripb::PodSandboxState::NotReady,
    }
}

fn container_state(s: runtime::ContainerState) -> cripb::ContainerState {
    match s {
        runtime::ContainerState::Created => cripb::ContainerState::Created,
        runtime::ContainerState::Running => cripb::ContainerState::Running,
        runtime::ContainerState::Exited => cripb::ContainerState::Exited,
    }
}

fn image_spec(id: &str) -> Option<cripb::ImageSpec> {
    Some(cripb::ImageSpec {
        image: id.to_string(),
        ..Default::default()
    })
}

fn image(i: &runtime::Image) -> cripb::Image {
    // the kubelet checks runAsNonRoot against these
    let user = i.config.user.split(':').next().unwrap_or("");
    let (uid, username) = match user.parse::<i64>() {
        Ok(uid) => (Some(cripb::Int64Value { value: uid }), String::new()),
        Err(_) if user.is_empty() => (Some(cripb::Int64Value { value: 0 }), String::new()),
        Err(_) => (None, user.to_string()),
    };
    cripb::Image {
        uid,
        username,
        id: i.id.clone(),
        repo_tags: i.refs.clone(),
        size: i.size,
        spec: image_spec(&i.id),
        ..Default::default()
    }
}

fn container_metadata(c: &ContainerSpec) -> Option<cripb::ContainerMetadata> {
    Some(cripb::ContainerMetadata {
        name: c.name.clone(),
        attempt: c.attempt,
    })
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

fn pod_spec(config: cripb::PodSandboxConfig) -> Result<PodSpec, Status> {
    let ns = config
        .linux
        .as_ref()
        .and_then(|l| l.security_context.as_ref())
        .and_then(|s| s.namespace_options.clone());
    if let Some(ns) = ns {
        if ns.network == cripb::NamespaceMode::Node {
            return Err(unsupported("a pod on the host network"));
        }
        if ns.pid == cripb::NamespaceMode::Pod {
            return Err(unsupported("a PID namespace shared by the pod"));
        }
        if ns.pid == cripb::NamespaceMode::Node || ns.ipc == cripb::NamespaceMode::Node {
            return Err(unsupported("a pod in the host's namespaces"));
        }
    }
    let cgroup_parent = config
        .linux
        .as_ref()
        .map(|l| l.cgroup_parent.clone())
        .unwrap_or_default();
    let resolv_conf = config.dns_config.map(|d| {
        let mut out = String::new();
        if !d.searches.is_empty() {
            out += &format!("search {}\n", d.searches.join(" "));
        }
        for s in &d.servers {
            out += &format!("nameserver {s}\n");
        }
        if !d.options.is_empty() {
            out += &format!("options {}\n", d.options.join(" "));
        }
        out
    });
    let meta = config.metadata.unwrap_or_default();
    Ok(PodSpec {
        cgroup_parent,
        resolv_conf: resolv_conf.unwrap_or_default(),
        name: meta.name,
        namespace: meta.namespace,
        uid: meta.uid,
        attempt: meta.attempt,
        hostname: config.hostname,
        log_directory: config.log_directory,
        labels: pairs(&config.labels),
        annotations: pairs(&config.annotations),
    })
}

fn container_spec(config: cripb::ContainerConfig) -> Result<ContainerSpec, Status> {
    let mut mounts = Vec::with_capacity(config.mounts.len());
    for m in config.mounts {
        if m.image.is_some() {
            return Err(unsupported("an image volume"));
        }
        if m.propagation != cripb::MountPropagation::Private {
            return Err(unsupported("mount propagation"));
        }
        if m.recursive_read_only {
            return Err(unsupported("a recursively read only mount"));
        }
        if !m.uid_mappings.is_empty() || !m.gid_mappings.is_empty() {
            return Err(unsupported("an id mapped mount"));
        }
        mounts.push(runtime::Mount {
            host_path: m.host_path.into(),
            container_path: m.container_path,
            readonly: m.readonly,
        });
    }
    let mut readonly_rootfs = false;
    let mut user = crate::user::Request::default();
    let mut limits = cgroup::Limits::default();
    let mut oom_score_adj = 0;
    if let Some(r) = config.linux.as_ref().and_then(|l| l.resources.as_ref()) {
        limits = cgroup::Limits {
            cpu_period: r.cpu_period,
            cpu_quota: r.cpu_quota,
            cpu_shares: r.cpu_shares,
            memory: r.memory_limit_in_bytes,
            memory_swap: r.memory_swap_limit_in_bytes,
            cpuset_cpus: r.cpuset_cpus.clone(),
            cpuset_mems: r.cpuset_mems.clone(),
            hugepages: r
                .hugepage_limits
                .iter()
                .map(|h| (h.page_size.clone(), h.limit))
                .collect(),
            unified: pairs(&r.unified),
        };
        oom_score_adj = r.oom_score_adj;
    }
    if !config.devices.is_empty() || !config.cdi_devices.is_empty() {
        return Err(unsupported("passing devices"));
    }
    if config.tty || config.stdin {
        return Err(unsupported("a terminal or stdin"));
    }
    if let Some(sc) = config
        .linux
        .as_ref()
        .and_then(|l| l.security_context.as_ref())
    {
        readonly_rootfs = sc.readonly_rootfs;
        if sc.privileged {
            return Err(unsupported("a privileged container"));
        }
        let id = |v: &Option<cripb::Int64Value>| -> Result<Option<u32>, Status> {
            v.as_ref()
                .map(|v| {
                    u32::try_from(v.value).map_err(|_| {
                        Status::new(code::INVALID_ARGUMENT, format!("bad id {}", v.value))
                    })
                })
                .transpose()
        };
        user = crate::user::Request {
            uid: id(&sc.run_as_user)?,
            gid: id(&sc.run_as_group)?,
            name: sc.run_as_username.clone(),
            supplemental_groups: sc
                .supplemental_groups
                .iter()
                .map(|&g| u32::try_from(g))
                .collect::<Result<_, _>>()
                .map_err(|_| Status::new(code::INVALID_ARGUMENT, "bad supplemental group"))?,
        };
    }
    let meta = config.metadata.unwrap_or_default();
    let mut env = Vec::with_capacity(config.envs.len());
    for kv in config.envs {
        let value = String::from_utf8(kv.value).map_err(|_| {
            Status::new(
                code::INVALID_ARGUMENT,
                format!("environment variable {} is not UTF-8", kv.key),
            )
        })?;
        env.push((kv.key, value));
    }
    Ok(ContainerSpec {
        name: meta.name,
        attempt: meta.attempt,
        image: config.image.unwrap_or_default().image,
        command: config.command,
        args: config.args,
        env,
        working_dir: config.working_dir,
        log_path: config.log_path,
        labels: pairs(&config.labels),
        annotations: pairs(&config.annotations),
        mounts,
        readonly_rootfs,
        limits,
        oom_score_adj,
        user,
    })
}

impl Cri {
    fn version(&self) -> cripb::VersionResponse {
        cripb::VersionResponse {
            version: "0.1.0".into(),
            runtime_name: "quantykube".into(),
            runtime_version: env!("CARGO_PKG_VERSION").into(),
            runtime_api_version: "v1".into(),
        }
    }

    fn status(&self) -> cripb::StatusResponse {
        let condition =
            |kind: &str, ok: bool, reason: &str, message: &str| cripb::RuntimeCondition {
                kind: kind.into(),
                status: ok,
                reason: reason.into(),
                message: message.into(),
            };
        cripb::StatusResponse {
            status: Some(cripb::RuntimeStatus {
                conditions: vec![
                    condition("RuntimeReady", true, "", ""),
                    match self.rt.network() {
                        Some(_) => condition("NetworkReady", true, "", ""),
                        None => condition(
                            "NetworkReady",
                            false,
                            "NetworkPluginNotReady",
                            "no CNI configuration",
                        ),
                    },
                ],
            }),
            runtime_handlers: vec![cripb::RuntimeHandler::default()],
            ..Default::default()
        }
    }

    fn pod_status(&self, id: &str) -> Result<cripb::PodSandboxStatusResponse, Status> {
        let p = self.rt.pod(id).map_err(status)?;
        Ok(cripb::PodSandboxStatusResponse {
            status: Some(cripb::PodSandboxStatus {
                id: p.id,
                metadata: Some(pod_metadata(&p.spec)),
                state: pod_state(p.state),
                created_at: p.created_ns,
                network: Some(cripb::PodSandboxNetworkStatus {
                    ip: p.ip.clone(),
                    ..Default::default()
                }),
                labels: map(&p.spec.labels),
                annotations: map(&p.spec.annotations),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn list_pods(&self, filter: Option<cripb::PodSandboxFilter>) -> cripb::ListPodSandboxResponse {
        let f = filter.unwrap_or_default();
        let items = self
            .rt
            .pods()
            .into_iter()
            .filter(|p| f.id.is_empty() || p.id.starts_with(&f.id))
            .filter(|p| {
                f.state
                    .as_ref()
                    .is_none_or(|s| s.state == pod_state(p.state))
            })
            .filter(|p| selected(&p.spec.labels, &f.label_selector))
            .map(|p| cripb::PodSandbox {
                metadata: Some(pod_metadata(&p.spec)),
                state: pod_state(p.state),
                created_at: p.created_ns,
                labels: map(&p.spec.labels),
                annotations: map(&p.spec.annotations),
                id: p.id,
                ..Default::default()
            })
            .collect();
        cripb::ListPodSandboxResponse { items }
    }

    fn list_containers(
        &self,
        filter: Option<cripb::ContainerFilter>,
    ) -> cripb::ListContainersResponse {
        let f = filter.unwrap_or_default();
        let containers = self
            .rt
            .containers()
            .into_iter()
            .filter(|c| f.id.is_empty() || c.id.starts_with(&f.id))
            .filter(|c| f.pod_sandbox_id.is_empty() || c.pod_id.starts_with(&f.pod_sandbox_id))
            .filter(|c| {
                f.state
                    .as_ref()
                    .is_none_or(|s| s.state == container_state(c.state))
            })
            .filter(|c| selected(&c.spec.labels, &f.label_selector))
            .map(|c| cripb::Container {
                metadata: container_metadata(&c.spec),
                image: image_spec(&c.image_id),
                image_ref: c.image_id.clone(),
                state: container_state(c.state),
                created_at: c.created_ns,
                labels: map(&c.spec.labels),
                annotations: map(&c.spec.annotations),
                pod_sandbox_id: c.pod_id,
                image_id: c.image_id,
                id: c.id,
            })
            .collect();
        cripb::ListContainersResponse { containers }
    }

    fn container_status(&self, id: &str) -> Result<cripb::ContainerStatusResponse, Status> {
        let c = self.rt.container(id).map_err(status)?;
        let log_path = self
            .rt
            .log_path(&c)
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default();
        Ok(cripb::ContainerStatusResponse {
            status: Some(cripb::ContainerStatus {
                metadata: container_metadata(&c.spec),
                state: container_state(c.state),
                created_at: c.created_ns,
                started_at: c.started_ns,
                finished_at: c.finished_ns,
                exit_code: c.exit_code,
                image: image_spec(&c.image_id),
                image_ref: c.image_id.clone(),
                reason: c.reason,
                labels: map(&c.spec.labels),
                annotations: map(&c.spec.annotations),
                log_path,
                image_id: c.image_id,
                id: c.id,
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn container_stats(&self, c: &runtime::Container) -> cripb::ContainerStats {
        let now = now_ns();
        let u = self.rt.usage(&c.id);
        let v =
            |f: fn(&cgroup::Usage) -> u64| u.as_ref().map(|u| cripb::UInt64Value { value: f(u) });
        cripb::ContainerStats {
            attributes: Some(cripb::ContainerAttributes {
                id: c.id.clone(),
                metadata: container_metadata(&c.spec),
                labels: map(&c.spec.labels),
                annotations: map(&c.spec.annotations),
            }),
            cpu: u.map(|_| cripb::CpuUsage {
                timestamp: now,
                usage_core_nano_seconds: v(|u| u.cpu_ns),
                ..Default::default()
            }),
            memory: u.map(|_| cripb::MemoryUsage {
                timestamp: now,
                working_set_bytes: v(|u| u.working_set),
                usage_bytes: v(|u| u.memory),
                rss_bytes: v(|u| u.rss),
                page_faults: v(|u| u.page_faults),
                major_page_faults: v(|u| u.major_page_faults),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn list_container_stats(
        &self,
        filter: Option<cripb::ContainerStatsFilter>,
    ) -> cripb::ListContainerStatsResponse {
        let f = filter.unwrap_or_default();
        let stats = self
            .rt
            .containers()
            .iter()
            .filter(|c| c.state == runtime::ContainerState::Running)
            .filter(|c| f.id.is_empty() || c.id.starts_with(&f.id))
            .filter(|c| f.pod_sandbox_id.is_empty() || c.pod_id.starts_with(&f.pod_sandbox_id))
            .filter(|c| selected(&c.spec.labels, &f.label_selector))
            .map(|c| self.container_stats(c))
            .collect();
        cripb::ListContainerStatsResponse { stats }
    }

    /// The sum over the pod's running containers.
    fn pod_stats(&self, p: &runtime::Pod) -> cripb::PodSandboxStats {
        let now = now_ns();
        let containers: Vec<cripb::ContainerStats> = self
            .rt
            .containers()
            .iter()
            .filter(|c| c.pod_id == p.id && c.state == runtime::ContainerState::Running)
            .map(|c| self.container_stats(c))
            .collect();
        let sum = |f: fn(&cripb::ContainerStats) -> Option<u64>| {
            Some(cripb::UInt64Value {
                value: containers.iter().filter_map(f).sum(),
            })
        };
        let cpu = |c: &cripb::ContainerStats| {
            c.cpu
                .as_ref()?
                .usage_core_nano_seconds
                .as_ref()
                .map(|v| v.value)
        };
        let ws = |c: &cripb::ContainerStats| {
            c.memory
                .as_ref()?
                .working_set_bytes
                .as_ref()
                .map(|v| v.value)
        };
        let usage =
            |c: &cripb::ContainerStats| c.memory.as_ref()?.usage_bytes.as_ref().map(|v| v.value);
        cripb::PodSandboxStats {
            attributes: Some(cripb::PodSandboxAttributes {
                id: p.id.clone(),
                metadata: Some(pod_metadata(&p.spec)),
                labels: map(&p.spec.labels),
                annotations: map(&p.spec.annotations),
            }),
            linux: Some(cripb::LinuxPodSandboxStats {
                cpu: Some(cripb::CpuUsage {
                    timestamp: now,
                    usage_core_nano_seconds: sum(cpu),
                    ..Default::default()
                }),
                memory: Some(cripb::MemoryUsage {
                    timestamp: now,
                    working_set_bytes: sum(ws),
                    usage_bytes: sum(usage),
                    ..Default::default()
                }),
                containers,
                ..Default::default()
            }),
        }
    }

    fn image_fs_info(&self) -> cripb::ImageFsInfoResponse {
        let used: u64 = self.rt.images().iter().map(|i| i.size).sum();
        let fs = cripb::FilesystemUsage {
            timestamp: now_ns(),
            fs_id: Some(cripb::FilesystemIdentifier {
                mountpoint: self.rt.dir().join("images").to_string_lossy().into_owned(),
            }),
            used_bytes: Some(cripb::UInt64Value { value: used }),
            inodes_used: None,
        };
        cripb::ImageFsInfoResponse {
            image_filesystems: vec![fs],
            container_filesystems: Vec::new(),
        }
    }

    fn call(&self, service: &str, method: &str, body: &[u8]) -> Result<Vec<u8>, Status> {
        use cripb::*;
        macro_rules! req {
            ($t:ty) => {
                <$t>::decode(body).map_err(bad_request)?
            };
        }
        let rt = &self.rt;
        Ok(match (service, method) {
            ("runtime.v1.RuntimeService", "Version") => self.version().encode(),
            ("runtime.v1.RuntimeService", "Status") => self.status().encode(),
            ("runtime.v1.RuntimeService", "RunPodSandbox") => {
                let r = req!(RunPodSandboxRequest);
                if !r.runtime_handler.is_empty() {
                    return Err(Status::new(
                        code::NOT_FOUND,
                        format!("no runtime handler {}", r.runtime_handler),
                    ));
                }
                let id = rt
                    .run_pod(pod_spec(r.config.unwrap_or_default())?)
                    .map_err(status)?;
                RunPodSandboxResponse { pod_sandbox_id: id }.encode()
            }
            ("runtime.v1.RuntimeService", "StopPodSandbox") => {
                let r = req!(StopPodSandboxRequest);
                match rt.stop_pod(&r.pod_sandbox_id, Duration::ZERO) {
                    // the kubelet stops pods it has already removed
                    Ok(()) | Err(runtime::Error::NotFound(_)) => {}
                    Err(e) => return Err(status(e)),
                }
                StopPodSandboxResponse {}.encode()
            }
            ("runtime.v1.RuntimeService", "RemovePodSandbox") => {
                let r = req!(RemovePodSandboxRequest);
                rt.remove_pod(&r.pod_sandbox_id).map_err(status)?;
                RemovePodSandboxResponse {}.encode()
            }
            ("runtime.v1.RuntimeService", "PodSandboxStatus") => {
                let r = req!(PodSandboxStatusRequest);
                self.pod_status(&r.pod_sandbox_id)?.encode()
            }
            ("runtime.v1.RuntimeService", "ListPodSandbox") => {
                self.list_pods(req!(ListPodSandboxRequest).filter).encode()
            }
            ("runtime.v1.RuntimeService", "CreateContainer") => {
                let r = req!(CreateContainerRequest);
                let spec = container_spec(r.config.unwrap_or_default())?;
                let id = rt
                    .create_container(&r.pod_sandbox_id, spec)
                    .map_err(status)?;
                CreateContainerResponse { container_id: id }.encode()
            }
            ("runtime.v1.RuntimeService", "StartContainer") => {
                let r = req!(StartContainerRequest);
                rt.start_container(&r.container_id).map_err(status)?;
                StartContainerResponse {}.encode()
            }
            ("runtime.v1.RuntimeService", "StopContainer") => {
                let r = req!(StopContainerRequest);
                let grace = Duration::from_secs(r.timeout.max(0) as u64);
                rt.stop_container(&r.container_id, grace).map_err(status)?;
                StopContainerResponse {}.encode()
            }
            ("runtime.v1.RuntimeService", "RemoveContainer") => {
                let r = req!(RemoveContainerRequest);
                rt.remove_container(&r.container_id).map_err(status)?;
                RemoveContainerResponse {}.encode()
            }
            ("runtime.v1.RuntimeService", "ListContainers") => self
                .list_containers(req!(ListContainersRequest).filter)
                .encode(),
            ("runtime.v1.RuntimeService", "ContainerStatus") => {
                let r = req!(ContainerStatusRequest);
                self.container_status(&r.container_id)?.encode()
            }
            ("runtime.v1.RuntimeService", "ExecSync") => {
                let r = req!(ExecSyncRequest);
                let timeout = (r.timeout > 0).then(|| Duration::from_secs(r.timeout as u64));
                let out = rt
                    .exec_sync(&r.container_id, &r.cmd, timeout)
                    .map_err(status)?;
                ExecSyncResponse {
                    stdout: out.stdout,
                    stderr: out.stderr,
                    exit_code: out.exit_code,
                }
                .encode()
            }
            ("runtime.v1.RuntimeService", "ContainerStats") => {
                let r = req!(ContainerStatsRequest);
                let c = rt.container(&r.container_id).map_err(status)?;
                ContainerStatsResponse {
                    stats: Some(self.container_stats(&c)),
                }
                .encode()
            }
            ("runtime.v1.RuntimeService", "ListContainerStats") => self
                .list_container_stats(req!(ListContainerStatsRequest).filter)
                .encode(),
            ("runtime.v1.RuntimeService", "PodSandboxStats") => {
                let r = req!(PodSandboxStatsRequest);
                let p = rt.pod(&r.pod_sandbox_id).map_err(status)?;
                PodSandboxStatsResponse {
                    stats: Some(self.pod_stats(&p)),
                }
                .encode()
            }
            ("runtime.v1.RuntimeService", "ListPodSandboxStats") => {
                let f = req!(ListPodSandboxStatsRequest).filter.unwrap_or_default();
                ListPodSandboxStatsResponse {
                    stats: rt
                        .pods()
                        .iter()
                        .filter(|p| f.id.is_empty() || p.id.starts_with(&f.id))
                        .filter(|p| selected(&p.spec.labels, &f.label_selector))
                        .map(|p| self.pod_stats(p))
                        .collect(),
                }
                .encode()
            }
            ("runtime.v1.ImageService", "ListImages") => {
                let filter = req!(ListImagesRequest)
                    .filter
                    .and_then(|f| f.image)
                    .map(|s| s.image)
                    .filter(|s| !s.is_empty());
                let images = match filter {
                    Some(name) => rt.find_image(&name).into_iter().collect(),
                    None => rt.images(),
                };
                ListImagesResponse {
                    images: images.iter().map(image).collect(),
                }
                .encode()
            }
            ("runtime.v1.ImageService", "ImageStatus") => {
                let name = req!(ImageStatusRequest).image.unwrap_or_default().image;
                ImageStatusResponse {
                    image: rt.find_image(&name).as_ref().map(image),
                    ..Default::default()
                }
                .encode()
            }
            ("runtime.v1.ImageService", "PullImage") => {
                let r = req!(PullImageRequest);
                let name = r.image.unwrap_or_default().image;
                let auth = r.auth.unwrap_or_default();
                if auth != cripb::AuthConfig::default() {
                    return Err(Status::new(
                        code::UNIMPLEMENTED,
                        "registry credentials are not supported yet",
                    ));
                }
                let img = rt
                    .pull(&name, &self.registries)
                    .map_err(|e| Status::new(code::UNKNOWN, format!("pulling {name}: {e}")))?;
                PullImageResponse { image_ref: img.id }.encode()
            }
            ("runtime.v1.ImageService", "RemoveImage") => {
                let name = req!(RemoveImageRequest).image.unwrap_or_default().image;
                match rt.remove_image(&name) {
                    Ok(()) | Err(runtime::Error::NotFound(_)) => {}
                    Err(e) => return Err(status(e)),
                }
                RemoveImageResponse {}.encode()
            }
            ("runtime.v1.ImageService", "ImageFsInfo") => self.image_fs_info().encode(),
            _ => {
                return Err(Status::new(
                    code::UNIMPLEMENTED,
                    format!("{service}/{method} is not implemented"),
                ))
            }
        })
    }
}

impl Service for Cri {
    fn is_streaming(&self, _method: &str) -> bool {
        false
    }

    fn unary(&self, path: &str, body: &[u8]) -> Result<Vec<u8>, Status> {
        let mut parts = path.trim_start_matches('/').splitn(2, '/');
        let service = parts.next().unwrap_or("");
        let method = parts.next().unwrap_or("");
        self.call(service, method, body)
    }

    fn stream(&self, method: &str, _requests: Receiver<Vec<u8>>, responder: Responder) {
        responder.finish(Status::new(
            code::UNIMPLEMENTED,
            format!("{method} is not implemented"),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cripb::{
        ContainerConfig, LinuxContainerConfig, LinuxContainerSecurityContext,
        LinuxPodSandboxConfig, LinuxSandboxSecurityContext, NamespaceMode, NamespaceOption,
        PodSandboxConfig,
    };

    fn pod(network: NamespaceMode, pid: NamespaceMode) -> PodSandboxConfig {
        PodSandboxConfig {
            linux: Some(LinuxPodSandboxConfig {
                security_context: Some(LinuxSandboxSecurityContext {
                    namespace_options: Some(NamespaceOption {
                        network,
                        pid,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // A pod or container this runtime would run differently from what the
    // kubelet asked for is refused, not run.
    #[test]
    fn what_cannot_be_honoured_is_refused() {
        use NamespaceMode::*;
        assert!(pod_spec(pod(Pod, Container)).is_ok());
        assert!(pod_spec(PodSandboxConfig::default()).is_ok());
        assert!(pod_spec(pod(Node, Container)).is_err());
        assert!(pod_spec(pod(Pod, Pod)).is_err());

        let user = |uid: i64| ContainerConfig {
            linux: Some(LinuxContainerConfig {
                security_context: Some(LinuxContainerSecurityContext {
                    run_as_user: Some(cripb::Int64Value { value: uid }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(container_spec(user(1000)).is_ok());
        assert!(container_spec(user(-1)).is_err());
        let shared = ContainerConfig {
            mounts: vec![cripb::Mount {
                propagation: cripb::MountPropagation::Bidirectional,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(container_spec(shared).is_err());
        let bad_env = ContainerConfig {
            envs: vec![cripb::KeyValue {
                key: "K".into(),
                value: vec![0xff],
            }],
            ..Default::default()
        };
        assert!(container_spec(bad_env).is_err());
    }
}
