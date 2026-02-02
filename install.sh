#!/usr/bin/env sh
set -eu

REPO="OpenMined/biosynth"
BIN="bvs"

say() { printf "%s\n" "$*"; }
err() { printf "error: %s\n" "$*" >&2; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1
}

if ! need_cmd uname; then
  err "uname is required"
  exit 1
fi

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$ARCH" in
  x86_64|amd64) ARCH="x86_64" ;;
  arm64|aarch64) ARCH="aarch64" ;;
  *)
    err "unsupported architecture: $ARCH"
    exit 1
    ;;
esac

EXT="tar.gz"
TARGET=""

case "$OS" in
  Darwin)
    TARGET="${ARCH}-apple-darwin"
    ;;
  Linux)
    LIBC="gnu"
    if need_cmd ldd && ldd --version 2>&1 | grep -qi musl; then
      LIBC="musl"
    elif [ -n "$(ls /lib/ld-musl-*.so.* 2>/dev/null)" ]; then
      LIBC="musl"
    fi
    TARGET="${ARCH}-unknown-linux-${LIBC}"
    ;;
  MINGW*|MSYS*|CYGWIN*)
    TARGET="${ARCH}-pc-windows-msvc"
    EXT="zip"
    ;;
  *)
    err "unsupported OS: $OS"
    exit 1
    ;;
esac

if need_cmd curl; then
  FETCH="curl -fsSL"
elif need_cmd wget; then
  FETCH="wget -qO-"
else
  err "curl or wget is required"
  exit 1
fi

VERSION="${BVS_VERSION:-}"
if [ -z "$VERSION" ]; then
  say "Fetching latest release tag..."
  VERSION="$($FETCH "https://api.github.com/repos/${REPO}/releases/latest" \
    | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
fi

if [ -z "$VERSION" ]; then
  err "failed to resolve latest release tag"
  err "set BVS_VERSION to a specific tag (e.g. v0.1.9) and retry"
  exit 1
fi

ASSET="${BIN}-${TARGET}.${EXT}"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}"
INSTALL_DIR="${BVS_INSTALL_DIR:-$HOME/.local/bin}"

say "Installing ${BIN} ${VERSION} for ${TARGET}..."
say "Download: ${URL}"

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

ARCHIVE="${TMP_DIR}/${ASSET}"
if need_cmd curl; then
  curl -fL -o "$ARCHIVE" "$URL"
else
  wget -qO "$ARCHIVE" "$URL"
fi

mkdir -p "$INSTALL_DIR"

case "$EXT" in
  tar.gz)
    tar -xzf "$ARCHIVE" -C "$TMP_DIR"
    ;;
  zip)
    if ! need_cmd unzip; then
      err "unzip is required to install Windows archives"
      exit 1
    fi
    unzip -q "$ARCHIVE" -d "$TMP_DIR"
    ;;
esac

if [ ! -f "${TMP_DIR}/${BIN}" ]; then
  err "archive missing ${BIN} binary"
  exit 1
fi

install -m 0755 "${TMP_DIR}/${BIN}" "${INSTALL_DIR}/${BIN}"

say "Installed to ${INSTALL_DIR}/${BIN}"
if ! command -v "$BIN" >/dev/null 2>&1; then
  say "Add ${INSTALL_DIR} to your PATH to use '${BIN}'"
fi
