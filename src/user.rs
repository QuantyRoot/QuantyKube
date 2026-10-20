//! Which user a container runs as: what the kubelet asked for, else what
//! the image says, resolved against the image's own `/etc/passwd` and
//! `/etc/group` the way runc resolves it.

use std::fs;
use std::path::Path;

/// What the kubelet sent; all unset means the image decides.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Request {
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub name: String,
    pub supplemental_groups: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
}

struct Passwd {
    name: String,
    uid: u32,
    gid: u32,
}

fn passwd(root: &Path) -> Vec<Passwd> {
    let text = fs::read_to_string(root.join("etc/passwd")).unwrap_or_default();
    text.lines()
        .filter_map(|l| {
            let f: Vec<&str> = l.split(':').collect();
            Some(Passwd {
                name: f.first()?.to_string(),
                uid: f.get(2)?.parse().ok()?,
                gid: f.get(3)?.parse().ok()?,
            })
        })
        .collect()
}

/// Name, gid and members of each group.
fn groups(root: &Path) -> Vec<(String, u32, Vec<String>)> {
    let text = fs::read_to_string(root.join("etc/group")).unwrap_or_default();
    text.lines()
        .filter_map(|l| {
            let f: Vec<&str> = l.split(':').collect();
            let members = f
                .get(3)
                .map(|m| {
                    m.split(',')
                        .filter(|s| !s.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            Some((f.first()?.to_string(), f.get(2)?.parse().ok()?, members))
        })
        .collect()
}

/// `image_user` is the config's `User`: a name or uid, optionally
/// followed by `:` and a group name or gid. `root` is the image's root
/// filesystem.
pub fn resolve(root: &Path, image_user: &str, req: &Request) -> Result<User, String> {
    let users = passwd(root);
    let all_groups = groups(root);
    let (user_part, group_part) = match image_user.split_once(':') {
        Some((u, g)) => (u, Some(g)),
        None => (image_user, None),
    };
    let by_uid = |uid: u32| users.iter().find(|p| p.uid == uid);
    let by_name = |name: &str| users.iter().find(|p| p.name == name);
    let (uid, entry) = if let Some(uid) = req.uid {
        (uid, by_uid(uid))
    } else if !req.name.is_empty() {
        let p = by_name(&req.name).ok_or_else(|| format!("no user {} in the image", req.name))?;
        (p.uid, Some(p))
    } else if user_part.is_empty() {
        (0, by_uid(0))
    } else if let Ok(uid) = user_part.parse::<u32>() {
        (uid, by_uid(uid))
    } else {
        let p = by_name(user_part).ok_or_else(|| format!("no user {user_part} in the image"))?;
        (p.uid, Some(p))
    };
    // the image's group applies only when the kubelet did not choose the
    // user, as in containerd
    let image_group = group_part.filter(|_| req.uid.is_none() && req.name.is_empty());
    let gid = match (req.gid, image_group) {
        (Some(gid), _) => gid,
        (None, Some(g)) => match g.parse::<u32>() {
            Ok(gid) => gid,
            Err(_) => all_groups
                .iter()
                .find(|(n, _, _)| n == g)
                .map(|(_, gid, _)| *gid)
                .ok_or_else(|| format!("no group {g} in the image"))?,
        },
        (None, None) => entry.map_or(0, |p| p.gid),
    };
    let mut groups = vec![gid];
    if let Some(p) = entry {
        for (_, g, members) in &all_groups {
            if members.contains(&p.name) && !groups.contains(g) {
                groups.push(*g);
            }
        }
    }
    for g in &req.supplemental_groups {
        if !groups.contains(g) {
            groups.push(*g);
        }
    }
    Ok(User { uid, gid, groups })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn users_resolve_as_runc_resolves_them() {
        let root = std::env::temp_dir().join(format!("qk-user-{}", std::process::id()));
        fs::create_dir_all(root.join("etc")).unwrap();
        fs::write(
            root.join("etc/passwd"),
            "root:x:0:0::/root:/bin/sh\nnginx:x:101:101::/:/sbin/nologin\n",
        )
        .unwrap();
        fs::write(
            root.join("etc/group"),
            "root:x:0:\nnginx:x:101:\nwww:x:33:nginx,other\n",
        )
        .unwrap();
        let r = |image: &str, req: Request| resolve(&root, image, &req);
        let u = |uid, gid, groups: &[u32]| User {
            uid,
            gid,
            groups: groups.to_vec(),
        };
        assert_eq!(r("", Request::default()), Ok(u(0, 0, &[0])));
        assert_eq!(r("nginx", Request::default()), Ok(u(101, 101, &[101, 33])));
        assert_eq!(r("1000:www", Request::default()), Ok(u(1000, 33, &[33])));
        let kubelet = Request {
            uid: Some(101),
            supplemental_groups: vec![2000],
            ..Default::default()
        };
        assert_eq!(r("0:0", kubelet), Ok(u(101, 101, &[101, 33, 2000])));
        assert!(r("nobody", Request::default()).is_err());
        fs::remove_dir_all(&root).unwrap();
    }
}
