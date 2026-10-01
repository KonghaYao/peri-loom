use std::{
    env, fs,
    path::{Path, PathBuf},
};
fn collect(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read web assets") {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect(root, &path, out);
        } else if path.is_file() {
            out.push(path.strip_prefix(root).unwrap().to_owned());
        }
    }
}
fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../web/dist");
    println!("cargo:rerun-if-changed={}", root.display());
    if !root.join("index.html").is_file() {
        // 分布式镜像继续由独立 web 容器服务静态资源，不增加 Node 构建依赖。
        fs::write(
            Path::new(&env::var("OUT_DIR").unwrap()).join("assets.rs"),
            "pub static ASSETS: &[(&str, &[u8])] = &[];",
        )
        .unwrap();
        return;
    }
    let root = root.canonicalize().unwrap();
    let mut files = Vec::new();
    collect(&root, &root, &mut files);
    files.sort();
    let mut code = String::from("pub static ASSETS: &[(&str, &[u8])] = &[\n");
    for path in files {
        code.push_str(&format!(
            "({:?}, include_bytes!({:?})),\n",
            path.to_string_lossy().replace('\\', "/"),
            root.join(&path)
        ));
    }
    code.push_str("];\n");
    fs::write(
        Path::new(&env::var("OUT_DIR").unwrap()).join("assets.rs"),
        code,
    )
    .unwrap();
}
