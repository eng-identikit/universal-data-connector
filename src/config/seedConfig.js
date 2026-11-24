const fs = require('fs');
const path = require('path');

/**
 * Copies the default configuration files shipped with the image (config.defaults/) into
 * config/ when they are missing there. A volume mounted on config/ (a Kubernetes emptyDir,
 * an empty Docker volume) hides the files of the image: without this the transformer
 * library, the DDS example and the storage defaults would be gone.
 *
 * Existing files are never overwritten. Synchronous: runs once before anything reads config/.
 *
 *   UDC_CONFIG_DEFAULTS_DIR   where the defaults are (default ./config.defaults; absent outside the image)
 */
function seedConfigDirectory({
  configDir = path.join(process.cwd(), 'config'),
  defaultsDir = process.env.UDC_CONFIG_DEFAULTS_DIR || path.join(process.cwd(), 'config.defaults')
} = {}) {
  const copied = [];
  if (!fs.existsSync(defaultsDir)) return copied;
  const walk = (from, to) => {
    fs.mkdirSync(to, { recursive: true });
    for (const entry of fs.readdirSync(from, { withFileTypes: true })) {
      const source = path.join(from, entry.name);
      const target = path.join(to, entry.name);
      if (entry.isDirectory()) {
        walk(source, target);
      } else if (entry.isFile() && !fs.existsSync(target)) {
        fs.copyFileSync(source, target);
        copied.push(path.relative(configDir, target));
      }
    }
  };
  walk(defaultsDir, configDir);
  return copied;
}

module.exports = { seedConfigDirectory };
