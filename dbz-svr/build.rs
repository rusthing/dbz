use std::fs;
use std::path::Path;

fn main() {
    let project_root = env!("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest_dir_path = Path::new(&out_dir)
        .ancestors()
        .nth(3)
        .expect("Failed to get parent directory");

    const CONFIG_EXTS: &[&str] = &["toml", "json", "json5", "yml", "yaml", "ini", "ron"];

    let pkg_name = env!("CARGO_PKG_NAME");

    for suffix in ["", "-dev", "-test", "-prod"] {
        let file_name = format!("{pkg_name}{suffix}");
        for ext in CONFIG_EXTS {
            copy_file(project_root, &file_name, ext, dest_dir_path);
        }
    }

    for ext in CONFIG_EXTS {
        copy_file(project_root, "log", ext, dest_dir_path);
    }

    copy_dir(project_root, "migrations", dest_dir_path);
    copy_dir(project_root, "cert", dest_dir_path);
}

fn copy_dir(project_root: &str, src_dir: &str, dest_dir_path: &Path) {
    let src_dir_path = Path::new(project_root).join(src_dir);
    if !src_dir_path.exists() || !src_dir_path.is_dir() {
        return;
    }

    let dest_dir = dest_dir_path.join(src_dir);
    copy_dir_recursive(&src_dir_path, &dest_dir);
}

fn copy_dir_recursive(src: &Path, dest: &Path) {
    fs::create_dir_all(dest).ok();
    if let Ok(entries) = fs::read_dir(src) {
        for entry in entries.flatten() {
            let src_path = entry.path();
            let dest_path = dest.join(entry.file_name());
            if src_path.is_dir() {
                copy_dir_recursive(&src_path, &dest_path);
            } else {
                fs::copy(&src_path, &dest_path).ok();
            }
        }
    }
}

fn copy_file(project_root: &str, file_name: &str, ext: &str, dest_dir_path: &Path) {
    let src_path = Path::new(project_root).join(format!("{file_name}.{ext}"));
    if src_path.exists() {
        let dest_path = dest_dir_path.join(format!("{file_name}.{ext}"));
        fs::copy(&src_path, &dest_path).ok();
    }
}