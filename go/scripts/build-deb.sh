#!/usr/bin/env bash
# Build a .deb that ships the sharding-demo and sharding-demo-tui binaries.
# Usage: scripts/build-deb.sh [VERSION] [ARCH]
#   VERSION defaults to 0.1.0
#   ARCH    defaults to `dpkg --print-architecture`

set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${1:-0.1.1}"
ARCH="${2:-$(dpkg --print-architecture)}"
PKG="sharding-demo"
DIST_DIR="dist"
STAGE_DIR="${DIST_DIR}/${PKG}_${VERSION}_${ARCH}"
DEB_PATH="${DIST_DIR}/${PKG}_${VERSION}_${ARCH}.deb"

case "$ARCH" in
    amd64) GOARCH=amd64 ;;
    arm64) GOARCH=arm64 ;;
    armhf) GOARCH=arm ;;
    386)   GOARCH=386 ;;
    *)     echo "unsupported arch: $ARCH" >&2; exit 2 ;;
esac

echo "==> Building binaries for $ARCH (GOARCH=$GOARCH) v$VERSION"
rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR/DEBIAN"
mkdir -p "$STAGE_DIR/usr/bin"
mkdir -p "$STAGE_DIR/usr/share/doc/$PKG"

CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" \
    go build -trimpath -ldflags="-s -w" -o "$STAGE_DIR/usr/bin/sharding-demo" ./cmd/demo
CGO_ENABLED=0 GOOS=linux GOARCH="$GOARCH" \
    go build -trimpath -ldflags="-s -w" -o "$STAGE_DIR/usr/bin/sharding-demo-tui"  ./cmd/tui

echo "==> Generating control"
sed -e "s|@VERSION@|$VERSION|g" -e "s|@ARCH@|$ARCH|g" \
    packaging/debian/control.in > "$STAGE_DIR/DEBIAN/control"

INSTALLED_KB=$(du -sk "$STAGE_DIR/usr" | cut -f1)
echo "Installed-Size: $INSTALLED_KB" >> "$STAGE_DIR/DEBIAN/control"

cp packaging/debian/copyright "$STAGE_DIR/usr/share/doc/$PKG/copyright"
if [ -f ../README.md ]; then
    gzip -n -9 -c ../README.md > "$STAGE_DIR/usr/share/doc/$PKG/README.md.gz"
fi

# Minimal native-package changelog so lintian is happy.
DATE=$(date -R)
cat > "$STAGE_DIR/usr/share/doc/$PKG/changelog" <<EOF
$PKG ($VERSION) unstable; urgency=low

  * Built from source via scripts/build-deb.sh.

 -- Vlad Ananyev <vlananyev@gmail.com>  $DATE
EOF
gzip -n -9 "$STAGE_DIR/usr/share/doc/$PKG/changelog"

# Permissions per Debian policy
find "$STAGE_DIR/usr" -type d -exec chmod 0755 {} +
find "$STAGE_DIR/usr" -type f -exec chmod 0644 {} +
chmod 0755 "$STAGE_DIR/usr/bin/"*
chmod 0755 "$STAGE_DIR/DEBIAN"
chmod 0644 "$STAGE_DIR/DEBIAN/control"

echo "==> Building $DEB_PATH"
dpkg-deb --root-owner-group --build "$STAGE_DIR" "$DEB_PATH" >/dev/null

echo
dpkg-deb --info "$DEB_PATH"
echo
echo "Contents:"
dpkg-deb --contents "$DEB_PATH"
echo
echo "Built: $DEB_PATH"
