// Ad-hoc re-sign the packed .app. electron-builder skips signing when no
// identity is configured, but its packing edits (Info.plist, asar, rename)
// invalidate Electron's original linker signature — a quarantined download
// then fails Gatekeeper with "app is damaged" instead of the recoverable
// "unverified developer" flow. Ad-hoc signing restores a valid signature.
const { execFileSync } = require('child_process');
const path = require('path');

exports.default = function adhocSign(context) {
  if (context.electronPlatformName !== 'darwin') return;
  const appPath = path.join(
    context.appOutDir,
    `${context.packager.appInfo.productFilename}.app`,
  );
  execFileSync('codesign', ['--force', '--deep', '--sign', '-', appPath], {
    stdio: 'inherit',
  });
  execFileSync('codesign', ['--verify', '--deep', '--strict', appPath], {
    stdio: 'inherit',
  });
};
