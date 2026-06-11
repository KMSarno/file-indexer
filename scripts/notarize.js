// afterSign hook: notarize the signed .app with Apple via notarytool, using an
// App Store Connect API key. Runs only when the API-key credentials are present
// (i.e. in CI / a release build) — a plain local `npm run dist` without them
// produces a signed-but-not-notarized app and skips this step cleanly.
const { notarize } = require('@electron/notarize');
const { execFileSync } = require('child_process');

exports.default = async function notarizing(context) {
  if (context.electronPlatformName !== 'darwin') return;

  const { APPLE_API_KEY, APPLE_API_KEY_ID, APPLE_API_ISSUER } = process.env;
  if (!APPLE_API_KEY || !APPLE_API_KEY_ID || !APPLE_API_ISSUER) {
    console.log('Notarize: APPLE_API_* not set — skipping (signed but not notarized).');
    return;
  }

  const appName = context.packager.appInfo.productFilename;
  const appPath = `${context.appOutDir}/${appName}.app`;

  console.log(`Notarize: submitting ${appPath} to Apple…`);
  await notarize({
    tool: 'notarytool',
    appPath,
    appleApiKey: APPLE_API_KEY,         // path to the .p8 key file
    appleApiKeyId: APPLE_API_KEY_ID,
    appleApiIssuer: APPLE_API_ISSUER,
  });

  // Staple the ticket so the app validates offline (notarize() submits but does
  // not staple).
  console.log('Notarize: stapling ticket…');
  execFileSync('xcrun', ['stapler', 'staple', appPath], { stdio: 'inherit' });
  console.log('Notarize: done.');
};
