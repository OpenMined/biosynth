use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, Context, Result};

use crate::UpdateArgs;

const INSTALL_URL: &str = "https://raw.githubusercontent.com/OpenMined/biosynth/main/install.sh";

pub fn run_update(args: UpdateArgs) -> Result<()> {
    if cfg!(windows) {
        return Err(anyhow!(
            "update is not supported on Windows yet; download the latest release zip instead"
        ));
    }

    let mut cmd = Command::new("sh");
    let fetch = format!(
        "if command -v curl >/dev/null 2>&1; then curl -fsSL {url}; \
elif command -v wget >/dev/null 2>&1; then wget -qO- {url}; \
else echo \"error: curl or wget is required\" >&2; exit 1; fi | sh",
        url = INSTALL_URL
    );
    cmd.arg("-c").arg(fetch);

    if let Some(version) = args.version {
        cmd.env("BVS_VERSION", version);
    }
    if let Some(install_dir) = args.install_dir {
        cmd.env("BVS_INSTALL_DIR", path_to_env(install_dir));
    }

    let status = cmd.status().context("failed to run install script")?;
    if !status.success() {
        return Err(anyhow!("install script failed"));
    }

    Ok(())
}

fn path_to_env(path: PathBuf) -> String {
    path.to_string_lossy().to_string()
}
