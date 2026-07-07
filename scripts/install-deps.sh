#!/usr/bin/env bash
set -euo pipefail

# Installs the Java 21 + Maven toolchain required to build this project's Flink jobs
# (flink/orderbook-job/pom.xml and flink/orderbook-consolidator/pom.xml pin java.version=21).
# Idempotent — safe to re-run; only installs what's missing.
#
# Debian/Ubuntu's apt repos (including backports) only ship OpenJDK 17, so Java 21 comes from a
# checksum-verified Eclipse Temurin tarball instead of a third-party apt repo.

JDK_MAJOR="21"
JDK_DIR="/opt/jdk-${JDK_MAJOR}"

command -v apt-get >/dev/null 2>&1 || { echo "ERROR: this script requires a Debian/Ubuntu host (apt-get not found)."; exit 1; }

APT_UPDATED=0
apt_update_once() {
    if [[ "$APT_UPDATED" -eq 0 ]]; then
        sudo apt-get update -qq
        APT_UPDATED=1
    fi
}

install_if_missing() {
    local cmd="$1" pkg="$2"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "==> Installing missing dependency: $pkg"
        apt_update_once
        sudo apt-get install -y "$pkg"
    fi
}

install_if_missing curl curl
install_if_missing jq jq

case "$(uname -m)" in
    x86_64) ARCH="x64" ;;
    aarch64) ARCH="aarch64" ;;
    *) echo "ERROR: unsupported architecture $(uname -m)"; exit 1 ;;
esac

# --- Java 21 (Eclipse Temurin) ---
if [[ -x "$JDK_DIR/bin/java" ]] && "$JDK_DIR/bin/java" -version 2>&1 | head -1 | grep -q "\"${JDK_MAJOR}\."; then
    echo "==> Temurin ${JDK_MAJOR} already present at ${JDK_DIR}, skipping download."
else
    echo "==> Fetching latest Temurin ${JDK_MAJOR} (${ARCH}) release info..."
    ASSET=$(curl -sf "https://api.adoptium.net/v3/assets/latest/${JDK_MAJOR}/hotspot?os=linux&architecture=${ARCH}&image_type=jdk&vendor=eclipse")
    JDK_URL=$(echo "$ASSET" | jq -r '.[0].binary.package.link')
    JDK_SHA256=$(echo "$ASSET" | jq -r '.[0].binary.package.checksum')
    [[ -n "$JDK_URL" && "$JDK_URL" != "null" ]] || { echo "ERROR: could not resolve Temurin ${JDK_MAJOR} download URL."; exit 1; }

    TMP_TARBALL="/tmp/temurin-${JDK_MAJOR}-$$.tar.gz"
    trap 'rm -f "$TMP_TARBALL"' EXIT
    echo "==> Downloading $JDK_URL"
    curl -sL -o "$TMP_TARBALL" "$JDK_URL"
    echo "${JDK_SHA256}  ${TMP_TARBALL}" | sha256sum -c -

    echo "==> Installing to ${JDK_DIR}"
    sudo mkdir -p "$JDK_DIR"
    sudo tar -xzf "$TMP_TARBALL" -C "$JDK_DIR" --strip-components=1
    rm -f "$TMP_TARBALL"
    trap - EXIT
fi

echo "==> Registering Temurin ${JDK_MAJOR} as the default java/javac..."
sudo update-alternatives --install /usr/bin/java java "$JDK_DIR/bin/java" 2111
sudo update-alternatives --install /usr/bin/javac javac "$JDK_DIR/bin/javac" 2111
sudo update-alternatives --set java "$JDK_DIR/bin/java"
sudo update-alternatives --set javac "$JDK_DIR/bin/javac"

if grep -q "^JAVA_HOME=" /etc/environment 2>/dev/null; then
    CURRENT_JAVA_HOME=$(grep "^JAVA_HOME=" /etc/environment | tail -1 | cut -d= -f2-)
    if [[ "$CURRENT_JAVA_HOME" != "$JDK_DIR" ]]; then
        echo "==> WARNING: /etc/environment already sets JAVA_HOME=${CURRENT_JAVA_HOME}, leaving it as-is (expected ${JDK_DIR})"
    else
        echo "==> JAVA_HOME already set correctly in /etc/environment"
    fi
else
    echo "==> Setting JAVA_HOME=${JDK_DIR} in /etc/environment"
    echo "JAVA_HOME=${JDK_DIR}" | sudo tee -a /etc/environment >/dev/null
fi

# --- Maven ---
if command -v mvn >/dev/null 2>&1; then
    echo "==> Maven already installed, skipping."
else
    echo "==> Installing Maven..."
    apt_update_once
    sudo apt-get install -y maven
fi

echo ""
echo "==> Done. Toolchain:"
mvn -version
