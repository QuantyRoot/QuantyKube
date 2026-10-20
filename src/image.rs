//! Unpacking OCI image layers into a root filesystem.
//!
//! A layer is a gzip compressed tar archive whose entries add, replace or,
//! through whiteout files, remove what the layers below it put there.
//! Layers come from registries nobody here controls, so every path is
//! resolved inside the root by this code, never by the kernel: a symlink
//! in a layer is followed as if the root were `/`, and nothing a layer
//! contains can name a file outside it.

use quantycore::inflate::GzipReader;
use quantycore::tar::{safe_relative, Archive, Kind};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{lchown, symlink, PermissionsExt};
use std::path::{Component, Path, PathBuf};

const WHITEOUT: &[u8] = b".wh.";
const OPAQUE: &[u8] = b".wh..wh..opq";
/// The same bound the kernel puts on symlinks in one path lookup.
const MAX_LINKS: usize = 40;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub entries: u64,
    pub bytes: u64,
    pub whiteouts: u64,
    /// device nodes are left to the runtime, which creates /dev itself
    pub skipped: u64,
    /// bytes of the uncompressed archive, headers included
    pub unpacked: u64,
}

/// Fails the read past `left` bytes, so that a small layer cannot inflate
/// to fill the disk.
struct Limited<R> {
    inner: R,
    left: u64,
    read: u64,
}

impl<R: Read> Read for Limited<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let room = buf
            .len()
            .min(usize::try_from(self.left.saturating_add(1)).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..room])?;
        if n as u64 > self.left {
            return Err(bad(
                "the image unpacks to more than the runtime allows".into()
            ));
        }
        self.left -= n as u64;
        self.read += n as u64;
        Ok(n)
    }
}

fn bad(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn show(p: &[u8]) -> String {
    String::from_utf8_lossy(p).into_owned()
}

/// Resolves `rel` inside `root`, following symlinks in every component
/// but the last as the container would see them. `..` stops at the root.
fn resolve_parent(root: &Path, rel: &Path) -> io::Result<PathBuf> {
    let mut parts: Vec<PathBuf> = rel
        .parent()
        .into_iter()
        .flat_map(|p| p.components())
        .map(|c| PathBuf::from(c.as_os_str()))
        .collect();
    parts.reverse();
    let mut at: Vec<PathBuf> = Vec::new();
    let mut links = 0;
    while let Some(part) = parts.pop() {
        match part.components().next() {
            Some(Component::ParentDir) => {
                at.pop();
                continue;
            }
            Some(Component::CurDir) | Some(Component::RootDir) | None => continue,
            _ => {}
        }
        let here: PathBuf = root.join(at.iter().collect::<PathBuf>()).join(&part);
        match fs::symlink_metadata(&here) {
            Ok(m) if m.file_type().is_symlink() => {
                links += 1;
                if links > MAX_LINKS {
                    return Err(bad(format!("too many symlinks under {}", rel.display())));
                }
                let target = fs::read_link(&here)?;
                if target.is_absolute() {
                    at.clear();
                }
                let mut more: Vec<PathBuf> = target
                    .components()
                    .map(|c| PathBuf::from(c.as_os_str()))
                    .collect();
                more.reverse();
                parts.extend(more);
            }
            Ok(m) if !m.is_dir() => {
                return Err(bad(format!("{} is not a directory", here.display())))
            }
            Ok(_) => at.push(part),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&here)?;
                at.push(part);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(root.join(at.iter().collect::<PathBuf>()))
}

fn remove_any(p: &Path) -> io::Result<()> {
    match fs::symlink_metadata(p) {
        Ok(m) if m.is_dir() => fs::remove_dir_all(p),
        Ok(_) => fs::remove_file(p),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Unpacks one gzip compressed layer on top of `root`.
pub fn unpack_layer(layer: impl Read, root: &Path) -> io::Result<Stats> {
    unpack_layer_limited(layer, root, u64::MAX)
}

/// Unpacks a layer, failing once its archive inflates past `limit` bytes.
pub fn unpack_layer_limited(layer: impl Read, root: &Path, limit: u64) -> io::Result<Stats> {
    let root = fs::canonicalize(root)?;
    let mut archive = Archive::new(Limited {
        inner: GzipReader::new(layer),
        left: limit,
        read: 0,
    });
    let mut stats = Stats::default();
    // an opaque marker hides what lower layers put in a directory, not
    // what this layer already wrote there
    let mut written: HashSet<PathBuf> = HashSet::new();
    let mut dirs: Vec<(PathBuf, u32)> = Vec::new();
    while let Some(mut entry) = archive.next_entry()? {
        stats.entries += 1;
        let h = entry.header().clone();
        let rel =
            safe_relative(&h.path).ok_or_else(|| bad(format!("unsafe path {}", show(&h.path))))?;
        let rel = Path::new(OsStr::from_bytes(rel.strip_suffix(b"/").unwrap_or(rel)));
        if rel.as_os_str().is_empty() {
            continue;
        }
        let parent = resolve_parent(&root, rel)?;
        let name = rel.file_name().map(|n| n.as_bytes()).unwrap_or_default();
        if name == OPAQUE {
            stats.whiteouts += 1;
            for child in fs::read_dir(&parent)? {
                let child = child?.path();
                if !written.contains(&child) {
                    remove_any(&child)?;
                }
            }
            continue;
        }
        if let Some(hidden) = name.strip_prefix(WHITEOUT) {
            stats.whiteouts += 1;
            remove_any(&parent.join(OsStr::from_bytes(hidden)))?;
            continue;
        }
        let dest = parent.join(OsStr::from_bytes(name));
        let mode = h.mode & 0o7777;
        match h.kind {
            Kind::Directory => {
                match fs::symlink_metadata(&dest) {
                    Ok(m) if m.is_dir() => {}
                    Ok(_) => {
                        remove_any(&dest)?;
                        fs::create_dir(&dest)?;
                    }
                    Err(_) => fs::create_dir(&dest)?,
                }
                // applied at the end, so a read only directory can still
                // receive the entries that follow it
                dirs.push((dest.clone(), mode));
            }
            Kind::Regular => {
                remove_any(&dest)?;
                let mut f = fs::File::create(&dest)?;
                stats.bytes += io::copy(&mut entry, &mut f)?;
                f.set_permissions(fs::Permissions::from_mode(mode))?;
                let t =
                    std::time::UNIX_EPOCH + std::time::Duration::from_secs(h.mtime.max(0) as u64);
                f.set_modified(t)?;
            }
            Kind::Symlink => {
                remove_any(&dest)?;
                symlink(OsStr::from_bytes(&h.link), &dest)?;
            }
            Kind::HardLink => {
                let target = safe_relative(&h.link)
                    .ok_or_else(|| bad(format!("unsafe link target {}", show(&h.link))))?;
                let target = Path::new(OsStr::from_bytes(target));
                let source =
                    resolve_parent(&root, target)?.join(target.file_name().unwrap_or_default());
                remove_any(&dest)?;
                fs::hard_link(&source, &dest)?;
            }
            Kind::Char | Kind::Block | Kind::Fifo | Kind::Other(_) => {
                stats.skipped += 1;
                continue;
            }
        }
        // ownership as recorded; unpacking as an ordinary user cannot
        // give files away, and that is the only case this tolerates
        if let Err(e) = lchown(&dest, Some(h.uid as u32), Some(h.gid as u32)) {
            if e.kind() != io::ErrorKind::PermissionDenied {
                return Err(e);
            }
        }
        written.insert(dest);
    }
    for (dir, mode) in dirs.into_iter().rev() {
        fs::set_permissions(&dir, fs::Permissions::from_mode(mode))?;
    }
    // the gzip trailer is only checked once the stream is read to its end
    let mut rest = archive.into_inner();
    io::copy(&mut rest, &mut io::sink())?;
    stats.unpacked = rest.read;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    struct Dir(PathBuf);

    impl Dir {
        fn new(name: &str) -> Dir {
            static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let p =
                std::env::temp_dir().join(format!("qk-image-{name}-{}-{n}", std::process::id()));
            let _ = fs::remove_dir_all(&p);
            fs::create_dir_all(&p).unwrap();
            Dir(p)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// A layer built by GNU tar from a script run in a scratch directory.
    fn layer(script: &str) -> Vec<u8> {
        layer_of(script, ".")
    }

    /// The same, archiving only `paths`, so parent directories can be left
    /// out of the archive the way an attacker would leave them out.
    fn layer_of(script: &str, paths: &str) -> Vec<u8> {
        let d = Dir::new("src");
        let recursion = if paths == "." { "" } else { "--no-recursion" };
        let ok = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "set -e; cd '{}'; {script}; tar -czf - --format=pax {recursion} {paths}",
                d.0.display()
            ))
            .output()
            .unwrap();
        assert!(
            ok.status.success(),
            "{}",
            String::from_utf8_lossy(&ok.stderr)
        );
        ok.stdout
    }

    #[test]
    fn layers_stack_and_whiteouts_remove() {
        let root = Dir::new("stack");
        let lower = layer(
            "mkdir -p etc keep gone opq; echo one > etc/a; echo x > gone/f; \
             echo old > opq/old; ln -s a etc/link; ln etc/a etc/hard",
        );
        let upper = layer(
            "mkdir -p etc opq; echo two > etc/a; touch .wh.gone; \
             touch opq/.wh..wh..opq; echo new > opq/new",
        );
        unpack_layer(&lower[..], &root.0).unwrap();
        let s = unpack_layer(&upper[..], &root.0).unwrap();
        assert_eq!(s.whiteouts, 2);
        let read = |p: &str| fs::read_to_string(root.0.join(p)).unwrap();
        assert_eq!(read("etc/a"), "two\n");
        assert_eq!(read("etc/link"), "two\n");
        assert_eq!(read("etc/hard"), "one\n", "a hard link keeps the old inode");
        assert!(!root.0.join("gone").exists());
        assert!(root.0.join("keep").is_dir());
        assert!(!root.0.join("opq/old").exists());
        assert_eq!(read("opq/new"), "new\n");
    }

    #[test]
    fn symlinks_in_a_layer_resolve_inside_the_root() {
        let outside = Dir::new("outside");
        let root = Dir::new("escape");
        let o = outside.0.display();
        let links = layer(&format!(
            "ln -s '{o}' abs; ln -s ../../../../../../../..'{o}' rel; \
             mkdir -p usr/lib; ln -s usr/lib lib"
        ));
        unpack_layer(&links[..], &root.0).unwrap();
        // only the files: their parents are the symlinks above
        let files = layer_of(
            "mkdir abs rel lib; echo a > abs/x; echo r > rel/y; echo l > lib/z",
            "./abs/x ./rel/y ./lib/z",
        );
        unpack_layer(&files[..], &root.0).unwrap();
        assert_eq!(
            fs::read_dir(&outside.0).unwrap().count(),
            0,
            "a file escaped"
        );
        let inside = root.0.join(outside.0.strip_prefix("/").unwrap());
        assert_eq!(fs::read_to_string(inside.join("x")).unwrap(), "a\n");
        assert_eq!(fs::read_to_string(inside.join("y")).unwrap(), "r\n");
        assert_eq!(fs::read_to_string(root.0.join("usr/lib/z")).unwrap(), "l\n");
    }

    #[test]
    fn dotdot_paths_are_refused() {
        let root = Dir::new("dotdot");
        let mut tar = Vec::new();
        let mut h = [0u8; 512];
        h[..8].copy_from_slice(b"../evil\0");
        h[100..108].copy_from_slice(b"0000644\0");
        h[124..136].copy_from_slice(b"00000000000\0");
        h[136..148].copy_from_slice(b"00000000000\0");
        h[156] = b'0';
        h[257..263].copy_from_slice(b"ustar\0");
        h[263..265].copy_from_slice(b"00");
        h[148..156].copy_from_slice(b"        ");
        let sum: u32 = h.iter().map(|&b| u32::from(b)).sum();
        h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        tar.extend_from_slice(&h);
        tar.extend_from_slice(&[0u8; 1024]);
        let gz = Command::new("gzip")
            .arg("-c")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut c| {
                use std::io::Write;
                c.stdin.take().unwrap().write_all(&tar)?;
                c.wait_with_output()
            })
            .unwrap()
            .stdout;
        let err = unpack_layer(&gz[..], &root.0).unwrap_err();
        assert!(err.to_string().contains("unsafe path"), "{err}");
    }

    // a few kilobytes of gzip can inflate to fill a disk: the runtime
    // stops at the size it allows
    #[test]
    fn a_layer_that_inflates_past_the_limit_is_refused() {
        let bomb = layer("head -c 8000000 /dev/zero > big");
        assert!(bomb.len() < 100_000, "{} bytes compressed", bomb.len());
        let root = Dir::new("bomb");
        let err = unpack_layer_limited(&bomb[..], &root.0, 1_000_000).unwrap_err();
        assert!(
            err.to_string().contains("more than the runtime allows"),
            "{err}"
        );
        let fits = Dir::new("fits");
        let s = unpack_layer_limited(&bomb[..], &fits.0, 9_000_000).unwrap();
        assert!(s.unpacked > 8_000_000);
    }
}
