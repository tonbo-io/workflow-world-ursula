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

const forbiddenWorldMarkers = [
  'node_modules/.pnpm/@workflow+world@',
  'node_modules/.pnpm/@tonbo-io+world-ursula',
];
const leakedWorldMarker = forbiddenWorldMarkers.find((marker) =>
  bundle.includes(marker)
);
if (leakedWorldMarker) {
  throw new Error(
    `Workflow VM bundle contains World implementation code (${leakedWorldMarker}); tree-shaking regressed`
  );
}

// DurableAgent intentionally brings the AI SDK and Zod into the workflow VM.
// Keep a regression budget around the measured official agent workload while
// separately asserting above that the World implementation itself stayed out.
const maxBytes = 1024 * 1024;
if (metadata.size > maxBytes) {
  throw new Error(
    `Workflow VM bundle is ${metadata.size} bytes, above the ${maxBytes}-byte regression budget`
  );
}

console.log(
  `[bench] workflow VM bundle is ${metadata.size} bytes and contains no World implementation code`
);
