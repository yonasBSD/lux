#!/bin/sh
set -e

REPO="lux-db/lux"
BINARY="luxctl"
INSTALL_DIR="/usr/local/bin"

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Linux)  OS_NAME="linux" ;;
    Darwin) OS_NAME="macos" ;;
    *)      echo "Unsupported OS: $OS"; exit 1 ;;
esac

case "$ARCH" in
    x86_64|amd64)  ARCH_NAME="x86_64" ;;
    aarch64|arm64) ARCH_NAME="arm64" ;;
    *)             echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

ARTIFACT="${BINARY}-${OS_NAME}-${ARCH_NAME}"
LATEST_TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases" | grep -o '"tag_name": *"luxctl-v[^"]*"' | head -1 | grep -o 'luxctl-v[^"]*')
if [ -z "$LATEST_TAG" ]; then
    echo "Could not find a luxctl release. Check https://github.com/${REPO}/releases"
    exit 1
fi
LATEST_URL="https://github.com/${REPO}/releases/download/${LATEST_TAG}/${ARTIFACT}.tar.gz"

echo "Installing ${BINARY} (${OS_NAME}/${ARCH_NAME})..."

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

curl -fsSL "$LATEST_URL" -o "$TMPDIR/${ARTIFACT}.tar.gz" || {
    echo "Failed to download from $LATEST_URL"
    echo "Check https://github.com/${REPO}/releases for available binaries."
    exit 1
}

tar xzf "$TMPDIR/${ARTIFACT}.tar.gz" -C "$TMPDIR"

if [ -w "$INSTALL_DIR" ]; then
    mv "$TMPDIR/$ARTIFACT" "$INSTALL_DIR/$BINARY"
else
    sudo mv "$TMPDIR/$ARTIFACT" "$INSTALL_DIR/$BINARY"
fi

chmod +x "$INSTALL_DIR/$BINARY"

echo "Installed ${BINARY} to ${INSTALL_DIR}/${BINARY}"
echo ""
echo "Run 'luxctl login' to get started."
