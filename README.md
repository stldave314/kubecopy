# kubecopy

`kubecopy` is a robust and resilient shell script for copying files and entire directories between local machines and Kubernetes pods (or directly between two pods). 

Built specifically to side-step connection unreliability and size limits when transferring files over 5MB using `kubectl cp`, it uses standard POSIX commands (`sh`, `tar`, `dd`, `df`) to provide chunking, hash-verification, and retries.

## Features

- **Large File Support**: Files are pre-compressed and split via `dd` into segments (5MB by default) prior to transfer to avoid arbitrary pipe breaks.
- **Fail-Safe Retries**: Uses a robust loop with timeouts. If network congestion drops a chunk, it retries pushing just that 5MB piece.
- **Dynamic Workspaces**: Rather than assuming `/tmp` is accessible or possesses the space required to compress a 10GB database, it scans sequentially through `/dev/shm`, `/tmp`, `/var/tmp`, and root/home dirs on the fly until it finds a writable path with adequate space.
- **Hash Verified Validation**: Automatically hashes `tar.gz` archives locally and remotely (`sha256sum`, `md5sum`, or `md5`), confirming they match bit-for-bit before extraction ever begins.
- **Secure cleanup**: Utilizes a `trap` to ensure all remote pod generated chunks, and local ones, are wiped forcefully on `EXIT` or `Ctrl-C`.
- **POSIX Compliant Architecture**: Written completely in POSIX `sh` syntax to support execution natively against Alpine/Busybox, Debian/Bash, and MacOS/ZSH without dependencies or script modifications.

---

## Installation

```bash
# Download and make executable
curl -O https://raw.githubusercontent.com/<user>/kubecopy/main/kubecopy.sh
chmod +x kubecopy.sh

# Move to bin (optional)
mv kubecopy.sh /usr/local/bin/kubecopy
```

---

## Usage
`kubecopy.sh` uses parameterized flags or interactive prompts. When flags are missing, it asks you dynamically.

### Basic Syntax

```bash
kubecopy [options]
  --version             Show version
  --dry-run             Print steps but don't execute
  --chunk-size BYTES    Size of chunks (default 5242880 / 5MB)
  --retries N           Number of retries for kubectl operations (default 3)

  --origin-type [local|pod]
  --origin-path PATH
  --origin-ns NAMESPACE
  --origin-pod POD
  --origin-kubeconfig KUBECONFIG

  --dest-type [local|pod]
  --dest-path PATH
  --dest-ns NAMESPACE
  --dest-pod POD
  --dest-kubeconfig KUBECONFIG
```

### Examples

**Local to Pod:**
```bash
kubecopy --origin-type local --origin-path /home/user/large-db.sql \
         --dest-type pod --dest-ns production --dest-pod db-worker-0 \
         --dest-path /var/lib/mysql/large-db.sql \
         --dest-kubeconfig ~/.kube/config-prod
```

**Pod to Local:**
```bash
kubecopy --origin-type pod --origin-ns gitea-ext --origin-pod gitea-postresql-1 \
         --origin-path /var/lib/postgresql/data/gitea.dump --origin-kubeconfig ~/Desktop/Git/local-ext.yaml \
         --dest-type local --dest-path /home/user/backups/gitea.dump
```

**Pod to Pod:**
```bash
kubecopy --origin-type pod --origin-ns prod --origin-pod sql-0 \
         --origin-path /var/log/mysql \
         --dest-type pod --dest-ns dev --dest-pod sql-restore \
         --dest-path /var/log/mysql
```

---

## Technical Flow 
1. **Initiation**: Compiles source and destination arguments interactively.
2. **Setup**: Evaluates disk space natively (`df`, `du`) dynamically finding a workspace natively capable of receiving chunks.
3. **Archive**: Wraps `tar` onto directory payloads and `dd` chunks it into `$CHUNK_SIZE` sizes. 
4. **Transfer**: Streams pieces via `kubectl cp` wrapper monitoring for timeouts.
5. **Validation**: Reconstructs inside workspace, performs hash match comparatives.
6. **Extract**: Expands target, drops components safely removing trails on success / interrupt via generic shell signal trap handler.