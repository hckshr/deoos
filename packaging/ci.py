"""Native CI build and ABI audit; no publishing."""
import argparse
import json
import pathlib
import platform
import re
import runpy
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / 'outputs' / 'release'
EVIDENCE = ROOT / 'outputs' / 'evidence'
CONTAINER = 'deoos-ci-build'
RUST = '1.93.1'
NODE = '22.23.2'
LINUX = {
    'linux-arm64': ('arm64', 'aarch64', 'quay.io/pypa/manylinux_2_28_aarch64@sha256:acc4e63610fef1da3d687322793665205415c2b22c0d2e403f1a44eb834d63fc',
                    'e3853c5a252fca15252d07cb23a1bdd9377a8c6f3efa01531109281ae47f841c', 'fff4078c5def658577f92c88db7db3bc0072924bfb93fe52c1e744a54e94abb8'),
    'linux-x64': ('amd64', 'x86_64', 'quay.io/pypa/manylinux_2_28_x86_64@sha256:c2261579b9c2e5d45aa93312f73e2a302182e3e977b558581a1838d6fed3d8e6',
                  '20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c', 'd60acfe00a2932254bb0ad20e01b0d74397a0875595de719654b214f4b03f307'),
}
PACKAGER = runpy.run_path(str(ROOT / 'packaging/build_package.py'))


def run(*args, capture=False):
    result = subprocess.run(args, cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else None


def write(name, data):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / name).write_text(json.dumps(data, indent=2) + '\n')


def host(target):
    assert PACKAGER['target']()[0] == target, f'Expected native {target}, got {platform.system()}/{platform.machine()}'


def verify(target):
    host(target)
    _, suffix, npm_os, cpu = PACKAGER['target']()
    minimum = PACKAGER['verify_release_architecture'](suffix, npm_os, cpu)
    node = json.loads(run('node', '-p', 'JSON.stringify({platform:process.platform,arch:process.arch,version:process.version})', capture=True))
    assert node['platform'] == npm_os and node['arch'] == cpu and node['version'] == 'v' + NODE
    write('native-build.json', {'target': target, 'system': platform.system(), 'machine': platform.machine(), 'glibc': platform.libc_ver(), 'macos_minimum': minimum, 'rust': run('rustc', '--version', capture=True).strip(), 'node': node})


def audit(target):
    verify(target)
    assert platform.libc_ver() == ('glibc', '2.28')
    dependencies_allowed = {'libgcc_s.so.1', 'libpthread.so.0', 'libm.so.6', 'libdl.so.2', 'libc.so.6', 'librt.so.1', 'ld-linux-x86-64.so.2', 'ld-linux-aarch64.so.1'}
    for artifact in ['engine/target/release/deoos-server', 'engine/target/release/libdeoos_engine.so', 'bindings/node/target/release/libdeoos_node.so']:
        path = ROOT / artifact
        for flag, label in [('-h', 'header'), ('-d', 'dependencies'), ('--version-info', 'versions')]:
            text = run('readelf', flag, str(path), capture=True)
            (EVIDENCE / f'{path.name}-{label}.txt').write_text(text)
            if label == 'versions':
                versions = [tuple(map(int, value.split('.'))) for value in re.findall(r'GLIBC_(\d+(?:\.\d+)+)', text)]
                assert versions and max(versions) <= (2, 28), f'{path} exceeds glibc 2.28'
            if label == 'dependencies':
                needed = set(re.findall(r'\(NEEDED\).*\[(.*?)\]', text))
                assert needed <= dependencies_allowed, f'Unaudited dependency: {needed - dependencies_allowed}'
    wheel, = OUTPUTS.glob('deoos-*/python/*.whl')
    text = run('auditwheel', 'show', str(wheel), capture=True)
    (EVIDENCE / 'auditwheel.txt').write_text(text)
    match = re.search(r'following platform tag:\s*"manylinux_(\d+)_(\d+)_', ' '.join(text.split()))
    assert match and tuple(map(int, match.groups())) <= (2, 28), 'Unverified manylinux baseline'


def linux_build(target):
    host(target)
    docker_arch, rust_arch, image, rustup_sha, node_sha = LINUX[target]
    node_arch = 'arm64' if target == 'linux-arm64' else 'x64'
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    run('docker', 'run', '-d', '--name', CONTAINER, '--platform', f'linux/{docker_arch}', '-v', f'{ROOT}:/work/repo', '-v', f'{OUTPUTS}:/work/outputs', image, 'sleep', 'infinity')
    shell = f'''set -euo pipefail
export RUSTUP_HOME=/opt/deoos-rustup CARGO_HOME=/opt/deoos-cargo CARGO_BUILD_JOBS=4
export PATH=/opt/deoos-node/bin:/opt/deoos-cargo/bin:/opt/python/cp312-cp312/bin:$PATH
mkdir -p /work/outputs/evidence /tmp/deoos-tools
cd /tmp/deoos-tools
curl --fail --location --retry 3 -o rustup-init https://static.rust-lang.org/rustup/archive/1.28.2/{rust_arch}-unknown-linux-gnu/rustup-init
printf '{rustup_sha}  rustup-init\\n' | sha256sum --check
chmod +x rustup-init
./rustup-init -y --profile minimal --default-toolchain {RUST} --no-modify-path
rustup set auto-self-update disable
curl --fail --location --retry 3 -o node.tar.xz https://nodejs.org/dist/v{NODE}/node-v{NODE}-linux-{node_arch}.tar.xz
printf '{node_sha}  node.tar.xz\\n' | sha256sum --check
mkdir -p /opt/deoos-node
tar -xJf node.tar.xz -C /opt/deoos-node --strip-components=1
cd /work/repo
python -m pip install -r tests/requirements.txt auditwheel==6.8.2 pyelftools==0.33
python packaging/build_package.py --build-only
python packaging/build_package.py --package-only
python packaging/ci.py audit {target}
DEOOS_LINUX_PLATFORM=manylinux_2_28_{rust_arch} python packaging/build_package.py --package-only
python packaging/ci.py audit {target}
'''
    run('docker', 'exec', CONTAINER, 'bash', '-c', shell)
    write('linux-image.json', {'target': target, 'image': image, 'rustup_version': '1.28.2', 'rustup_sha256': rustup_sha, 'node_sha256': node_sha})


def cleanup():
    if platform.system() == 'Linux':
        subprocess.run(['docker', 'rm', '-f', CONTAINER], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    write('ci-cleanup.json', {'container': CONTAINER, 'removed': platform.system() == 'Linux'})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['linux-build', 'verify', 'audit', 'cleanup'])
    parser.add_argument('target', nargs='?', choices=tuple(PACKAGER['TARGETS']))
    args = parser.parse_args()
    if args.command == 'cleanup':
        cleanup()
    else:
        if not args.target:
            parser.error('target is required')
        globals()[args.command.replace('-', '_')](args.target)


if __name__ == '__main__':
    main()
