import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';

const bundlePath = path.resolve(
  import.meta.dirname,
  'app',
  'app',
  '.well-known',
  'workflow',
  'v1',
  'flow',
  'route.js'
);
const [bundle, metadata] = await Promise.all([
  readFile(bundlePath, 'utf8'),
  stat(bundlePath),
]);

if (bundle.includes('node_modules/.pnpm/zod@')) {
  throw new Error(
    'Workflow VM bundle still contains Zod; @workflow/world tree-shaking regressed'
  );
}

const maxBytes = 100 * 1024;
if (metadata.size > maxBytes) {
  throw new Error(
    `Workflow VM bundle is ${metadata.size} bytes, above the ${maxBytes}-byte regression budget`
  );
}

console.log(
  `[bench] workflow VM bundle is ${metadata.size} bytes and contains no Zod runtime`
);
