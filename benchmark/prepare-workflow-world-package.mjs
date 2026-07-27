import { readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

const virtualStore = path.resolve(
  import.meta.dirname,
  '..',
  'node_modules',
  '.pnpm'
);
const entries = await readdir(virtualStore, { withFileTypes: true });
let changed = 0;
let found = 0;

for (const entry of entries) {
  if (!entry.isDirectory() || !entry.name.startsWith('@workflow+world@')) {
    continue;
  }
  const packagePath = path.join(
    virtualStore,
    entry.name,
    'node_modules',
    '@workflow',
    'world',
    'package.json'
  );
  let manifest;
  try {
    manifest = JSON.parse(await readFile(packagePath, 'utf8'));
  } catch (error) {
    if (error?.code === 'ENOENT') continue;
    throw error;
  }
  if (manifest.name !== '@workflow/world') continue;
  found += 1;
  if (manifest.sideEffects === false) continue;
  manifest.sideEffects = false;
  await writeFile(packagePath, `${JSON.stringify(manifest, null, 2)}\n`);
  changed += 1;
}

if (found === 0) {
  throw new Error('No installed @workflow/world package was found');
}

console.log(
  `[bench] marked ${changed}/${found} installed @workflow/world package(s) as side-effect-free`
);
