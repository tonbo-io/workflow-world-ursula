import { execFile } from 'node:child_process';
import fs from 'node:fs';
import { promisify } from 'node:util';

const exec = promisify(execFile);
const jobNamespace = process.env.JOB_NAMESPACE;
const appNamespace = process.env.APP_NAMESPACE ?? jobNamespace;
const output = process.env.RESOURCE_OUTPUT;
const includeUrsula = process.env.INCLUDE_URSULA === '1';
const intervalMs = Number.parseInt(process.env.SAMPLE_INTERVAL_MS ?? '10000', 10);

if (!jobNamespace || !appNamespace || !output) {
  throw new Error('JOB_NAMESPACE, APP_NAMESPACE, and RESOURCE_OUTPUT are required');
}

async function kubectl(args) {
  const { stdout } = await exec('kubectl', args, {
    maxBuffer: 10 * 1024 * 1024,
  });
  return stdout.trim();
}

async function pods(namespace, selector) {
  const output = await kubectl([
    'get',
    'pods',
    '-n',
    namespace,
    '-l',
    selector,
    '-o',
    'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}',
  ]);
  return output ? output.split('\n') : [];
}

async function samplePod(timestamp, kind, namespace, pod) {
  try {
    const output = await kubectl([
      'exec',
      '-n',
      namespace,
      pod,
      '--',
      'sh',
      '-c',
      'awk \'/^usage_usec /{u=$2}/^throttled_usec /{t=$2}END{printf "%s,%s",u,t}\' /sys/fs/cgroup/cpu.stat; printf ","; sed -n "1p" /sys/fs/cgroup/memory.current',
    ]);
    return `${timestamp},${kind},${pod},${output}\n`;
  } catch {
    return '';
  }
}

async function jobFinished() {
  try {
    const status = await kubectl([
      'get',
      'job',
      'workflow-capacity-runner',
      '-n',
      jobNamespace,
      '-o',
      'jsonpath={.status.completionTime},{.status.failed}',
    ]);
    const [completionTime, failed] = status.split(',');
    return Boolean(completionTime || failed);
  } catch {
    return false;
  }
}

fs.writeFileSync(
  output,
  'timestamp,kind,pod,usage_usec,throttled_usec,memory_bytes\n'
);

for (;;) {
  try {
    const timestamp = new Date().toISOString();
    const targets = (await pods(appNamespace, 'app=workflow-benchmark-app')).map(
      (pod) => ['app', appNamespace, pod]
    );
    if (includeUrsula) {
      targets.push(
        ...(await pods('ursula', 'app.kubernetes.io/name=ursula')).map((pod) => [
          'voter',
          'ursula',
          pod,
        ])
      );
    }
    const rows = await Promise.all(
      targets.map(([kind, namespace, pod]) =>
        samplePod(timestamp, kind, namespace, pod)
      )
    );
    fs.appendFileSync(output, rows.join(''));
    if (await jobFinished()) break;
  } catch (error) {
    console.warn(`[resource-sampler] sample failed: ${error.message}`);
  }
  await new Promise((resolve) => setTimeout(resolve, intervalMs));
}
