//! The installer's operator (ADR-009, ADR-040): it follows the
//! `Installation` named quantykube and moves the cluster onto QuantyKube
//! one step at a time, the store first, then the runtime of each node
//! labelled for it, and back again in the reverse order when asked. Every
//! step is recorded in the object's status before the next begins, so an
//! operator that is restarted halfway, as it is when its own node changes
//! runtime, carries on where it was.

use crate::kube::{Kube, Response};
use quantycore::json::{self, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const GROUP: &str = "install.quantyroot.io";
const INSTALLATION: &str = "/apis/install.quantyroot.io/v1alpha1/installations/quantykube";
const INSTALLATIONS: &str = "/apis/install.quantyroot.io/v1alpha1/installations";
/// Nodes with this label get QuantyKube's runtime.
pub const LABEL: &str = "install.quantyroot.io/runtime=quantykube";
/// How long a node's move may take before it counts as failed.
const NODE_DEADLINE: u64 = 600;

pub struct Operator {
    pub kube: Kube,
    pub image: String,
    pub namespace: String,
    /// Its own pod, which a drain leaves where it is.
    pub pod: String,
}

/// A step that is not done yet: try again on the next pass.
const WAIT: &str = "waiting";

fn quote(s: &str) -> String {
    Value::String(s.to_string()).to_string()
}

fn parse(text: &str) -> Value {
    json::parse(text).expect("built from a literal and quoted strings")
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn str_at<'a>(v: &'a Value, pointer: &str) -> &'a str {
    v.pointer(pointer).and_then(Value::as_str).unwrap_or("")
}

/// A pod's name: lowercase letters, digits and dashes, at most 63.
fn agent_name(node: &str, verb: &str, what: &str) -> String {
    format!("quantykube-{verb}-{what}-{node}")
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .take(63)
        .collect::<String>()
        .trim_end_matches('-')
        .to_string()
}

fn checked(r: Response, what: &str) -> Result<Response, String> {
    if r.ok() {
        Ok(r)
    } else {
        Err(format!("{what}: {}", r.message()))
    }
}

impl Operator {
    /// Runs forever, one pass every few seconds; a failed pass is retried.
    pub fn run(&self) -> ! {
        let mut last = String::new();
        loop {
            match self.pass() {
                Ok(()) => last.clear(),
                Err(e) if e == WAIT => {}
                Err(e) => {
                    if e != last {
                        eprintln!("quantykube operator: {e}");
                        last = e;
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(3));
        }
    }

    fn pass(&self) -> Result<(), String> {
        let r = self.kube.get(INSTALLATION)?;
        if r.status == 404 {
            // one kubectl apply is enough: the default asks for the store
            // and for the runtime on the nodes labelled for it
            let body = parse(&format!(
                r#"{{"apiVersion":"{GROUP}/v1alpha1","kind":"Installation",
                   "metadata":{{"name":"quantykube"}},
                   "spec":{{"store":true,"runtimeSelector":{},"undo":false}}}}"#,
                quote(LABEL)
            ));
            checked(
                self.kube.post(INSTALLATIONS, &body)?,
                "creating the installation",
            )?;
            return Err(WAIT.into());
        }
        let inst = checked(r, "reading the installation")?.body;
        let status = inst.get("status").cloned().unwrap_or(Value::Null);
        let phase = str_at(&status, "/phase");
        let undo = inst.pointer("/spec/undo").and_then(Value::as_bool) == Some(true);
        if phase == "Failed" && !undo {
            return Ok(());
        }
        let store = inst.pointer("/spec/store").and_then(Value::as_bool) == Some(true);
        let selector = str_at(&inst, "/spec/runtimeSelector");
        if !undo {
            if store && str_at(&status, "/store") != "done" {
                self.set(r#"{"phase":"Moving","message":"moving the store"}"#)?;
                return self.store("install");
            }
            for node in self.nodes(selector)? {
                if str_at(&status, &format!("/nodes/{node}")) != "done" {
                    return self.node(&node, "install", &status);
                }
            }
            if phase != "Done" {
                self.set(r#"{"phase":"Done","message":"the cluster runs on QuantyKube"}"#)?;
            }
            return Ok(());
        }
        let moved: Vec<String> = status
            .get("nodes")
            .and_then(Value::as_object)
            .map(|o| {
                o.iter()
                    .filter(|(_, v)| v.as_str() == Some("done"))
                    .map(|(k, _)| k.to_string())
                    .collect()
            })
            .unwrap_or_default();
        if let Some(node) = moved.first() {
            return self.node(node, "undo", &status);
        }
        if str_at(&status, "/store") == "done" {
            self.set(r#"{"phase":"Undoing","message":"moving the store back to etcd"}"#)?;
            return self.store("undo");
        }
        if phase != "Undone" {
            self.set(r#"{"phase":"Undone","message":"the cluster runs as before"}"#)?;
        }
        Ok(())
    }

    /// Merges `fields` into the installation's status.
    fn set(&self, fields: &str) -> Result<(), String> {
        let body = parse(&format!(r#"{{"status":{fields}}}"#));
        checked(
            self.kube
                .merge_patch(&format!("{INSTALLATION}/status"), &body)?,
            "updating the status",
        )?;
        Ok(())
    }

    fn fail(&self, message: &str) -> Result<(), String> {
        self.set(&format!(
            r#"{{"phase":"Failed","message":{}}}"#,
            quote(message)
        ))?;
        Err(message.to_string())
    }

    fn nodes(&self, selector: &str) -> Result<Vec<String>, String> {
        if selector.is_empty() {
            return Ok(Vec::new());
        }
        let r = checked(
            self.kube.get(&format!(
                "/api/v1/nodes?labelSelector={}",
                selector.replace('=', "%3D").replace('/', "%2F")
            ))?,
            "listing nodes",
        )?;
        let mut names: Vec<String> = r
            .body
            .get("items")
            .and_then(Value::as_array)
            .unwrap_or(&[])
            .iter()
            .map(|n| str_at(n, "/metadata/name").to_string())
            .collect();
        names.sort();
        Ok(names)
    }

    /// The one control plane node, where the API server and etcd run.
    fn control_plane(&self) -> Result<String, String> {
        let names = self.nodes("node-role.kubernetes.io/control-plane")?;
        match names.as_slice() {
            [one] => Ok(one.clone()),
            [] => Err("no node is labelled as the control plane".into()),
            _ => Err("several control plane nodes; one is supported".into()),
        }
    }

    fn pods(&self) -> String {
        format!("/api/v1/namespaces/{}/pods", self.namespace)
    }

    /// Starts `quantykube host -- verb what` on `node` unless it runs, and
    /// returns its pod's phase.
    fn agent(&self, node: &str, verb: &str, what: &str) -> Result<String, String> {
        let name = agent_name(node, verb, what);
        let r = self.kube.get(&format!("{}/{name}", self.pods()))?;
        if r.ok() {
            return Ok(str_at(&r.body, "/status/phase").to_string());
        }
        let pod = parse(&format!(
            r#"{{"apiVersion":"v1","kind":"Pod",
               "metadata":{{"name":{},"labels":{{"app":"quantykube-agent"}}}},
               "spec":{{"nodeName":{},"hostPID":true,"hostNetwork":true,
                 "restartPolicy":"Never","tolerations":[{{"operator":"Exists"}}],
                 "containers":[{{"name":"agent","image":{},
                   "args":["host","--root","/host","--",{},{}],
                   "securityContext":{{"privileged":true}},
                   "volumeMounts":[{{"name":"host","mountPath":"/host"}}]}}],
                 "volumes":[{{"name":"host","hostPath":{{"path":"/"}}}}]}}}}"#,
            quote(&name),
            quote(node),
            quote(&self.image),
            quote(verb),
            quote(what)
        ));
        checked(self.kube.post(&self.pods(), &pod)?, "starting the agent")?;
        Ok("Pending".into())
    }

    fn remove_agent(&self, node: &str, verb: &str, what: &str) {
        let name = agent_name(node, verb, what);
        let _ = self.kube.delete(&format!("{}/{name}", self.pods()));
    }

    fn agent_log(&self, node: &str, verb: &str, what: &str) -> String {
        let name = agent_name(node, verb, what);
        self.kube
            .get(&format!("{}/{name}/log?tailLines=20", self.pods()))
            .ok()
            .and_then(|r| r.body.as_str().map(str::to_string))
            .unwrap_or_default()
    }

    fn store(&self, verb: &str) -> Result<(), String> {
        let node = self.control_plane()?;
        match self.agent(&node, verb, "store")?.as_str() {
            "Succeeded" => {
                self.remove_agent(&node, verb, "store");
                let done = if verb == "install" { "done" } else { "undone" };
                self.set(&format!(r#"{{"store":"{done}"}}"#))?;
                Err(WAIT.into())
            }
            "Failed" => {
                let log = self.agent_log(&node, verb, "store");
                self.remove_agent(&node, verb, "store");
                self.fail(&format!("{verb} store on {node} failed: {log}"))
            }
            _ => Err(WAIT.into()),
        }
    }

    /// Moves one node: cordon, drain, the agent, then waits for the node
    /// to be ready on the runtime it should have, and uncordons it.
    fn node(&self, node: &str, verb: &str, status: &Value) -> Result<(), String> {
        let path = format!("/api/v1/nodes/{node}");
        let n = checked(self.kube.get(&path)?, "reading the node")?.body;
        let runtime = str_at(&n, "/status/nodeInfo/containerRuntimeVersion");
        let on_ours = runtime.starts_with("quantykube://");
        let ready = n
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .unwrap_or(&[])
            .iter()
            .any(|c| str_at(c, "/type") == "Ready" && str_at(c, "/status") == "True");
        let started_key = format!("/started/{node}");
        let started = status
            .pointer(&started_key)
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if started != 0 && ready && on_ours == (verb == "install") {
            self.remove_agent(node, verb, "runtime");
            checked(
                self.kube
                    .merge_patch(&path, &parse(r#"{"spec":{"unschedulable":false}}"#))?,
                "uncordoning",
            )?;
            let done = if verb == "install" { "done" } else { "undone" };
            self.set(&format!(
                r#"{{"nodes":{{{}:"{done}"}},"started":{{{}:null}}}}"#,
                quote(node),
                quote(node)
            ))?;
            return Err(WAIT.into());
        }
        if started != 0 {
            if now() > started + NODE_DEADLINE {
                let log = self.agent_log(node, verb, "runtime");
                return self.fail(&format!(
                    "{verb} runtime on {node} did not finish in {NODE_DEADLINE}s, \
                     the node runs {runtime}; journalctl -u quantykube-{verb}-runtime \
                     on the node says why: {log}"
                ));
            }
            return Err(WAIT.into());
        }
        self.set(&format!(
            r#"{{"phase":"{}","message":{}}}"#,
            if verb == "install" {
                "Moving"
            } else {
                "Undoing"
            },
            quote(&format!("{verb} runtime on {node}: draining"))
        ))?;
        checked(
            self.kube
                .merge_patch(&path, &parse(r#"{"spec":{"unschedulable":true}}"#))?,
            "cordoning",
        )?;
        self.drain(node)?;
        self.agent(node, verb, "runtime")?;
        self.set(&format!(r#"{{"started":{{{}:{}}}}}"#, quote(node), now()))?;
        Err(WAIT.into())
    }

    /// Evicts what a drain evicts: every pod on the node but DaemonSets',
    /// static pods and the installer's own. Not done until they are gone.
    fn drain(&self, node: &str) -> Result<(), String> {
        let r = checked(
            self.kube.get(&format!(
                "/api/v1/pods?fieldSelector=spec.nodeName%3D{node}"
            ))?,
            "listing the node's pods",
        )?;
        let mut left = 0;
        for p in r.body.get("items").and_then(Value::as_array).unwrap_or(&[]) {
            let name = str_at(p, "/metadata/name");
            let ns = str_at(p, "/metadata/namespace");
            let phase = str_at(p, "/status/phase");
            let daemon = p
                .pointer("/metadata/ownerReferences")
                .and_then(Value::as_array)
                .unwrap_or(&[])
                .iter()
                .any(|o| str_at(o, "/kind") == "DaemonSet");
            let mirror = p
                .pointer("/metadata/annotations/kubernetes.io~1config.mirror")
                .is_some();
            let ours = ns == self.namespace
                && (name == self.pod || str_at(p, "/metadata/labels/app") == "quantykube-agent");
            if daemon || mirror || ours || phase == "Succeeded" || phase == "Failed" {
                continue;
            }
            left += 1;
            let eviction = parse(&format!(
                r#"{{"apiVersion":"policy/v1","kind":"Eviction",
                   "metadata":{{"name":{},"namespace":{}}}}}"#,
                quote(name),
                quote(ns)
            ));
            // 429 while a disruption budget holds it: asked again next pass
            let _ = self.kube.post(
                &format!("/api/v1/namespaces/{ns}/pods/{name}/eviction"),
                &eviction,
            );
        }
        if left > 0 {
            Err(WAIT.into())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_agent_s_name_is_a_valid_pod_name() {
        assert_eq!(
            agent_name("W1.example.com", "install", "runtime"),
            "quantykube-install-runtime-w1-example-com"
        );
        let long = agent_name(&"n".repeat(80), "undo", "store");
        assert_eq!(long.len(), 63);
        assert!(!agent_name("node-", "undo", "store").ends_with('-'));
    }
}
