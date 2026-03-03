#!/usr/bin/env sh
# kubecopy.sh - Robust file copying between local and Kubernetes pods.
# Supports large files (>5MB) via tar & dd split, hashes, and retries.
set -e

VERSION="1.1.0"
CHUNK_SIZE=$((5 * 1024 * 1024)) # 5MB
MAX_RETRIES=3
KUBECTL_TIMEOUT=30 # seconds per operation
DRY_RUN=0

# Config vars
ORIGIN_TYPE=""
DEST_TYPE=""
ORIGIN_PATH=""
ORIGIN_NS=""
ORIGIN_POD=""
ORIGIN_KUBECONFIG=""
DEST_PATH=""
DEST_NS=""
DEST_POD=""
DEST_KUBECONFIG=""
TMP_DIR_LOCAL_ARG=""
TMP_DIR_ORIGIN_ARG=""
TMP_DIR_DEST_ARG=""
PROMPTED=0

LOG_FILE="$HOME/.kubecopy.log"
TMP_DIR_LOCAL=""
TMP_DIR_ORIGIN=""
TMP_DIR_DEST=""

log() {
    local ts
    ts=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$ts] $*" | tee -a "$LOG_FILE"
}

show_help() {
    cat <<EOF
kubecopy.sh [options]
Options:
  --version             Show version
  --dry-run             Print steps but don't execute
  --chunk-size BYTES    Size of chunks (default 5242880)
  --retries N           Number of retries for kubectl operations (default 3)
  --timeout N           Timeout in seconds for kubectl operations (default 30)

  --origin-type [local|pod]
  --origin-path PATH
  --origin-ns NAMESPACE
  --origin-pod POD
  --origin-kubeconfig KUBECONFIG
  --origin-tmp-dir PATH

  --dest-type [local|pod]
  --dest-path PATH
  --dest-ns NAMESPACE
  --dest-pod POD
  --dest-kubeconfig KUBECONFIG
  --dest-tmp-dir PATH
  
  --local-tmp-dir PATH
EOF
    exit 0
}

cleanup() {
    log "Cleaning up temporary directories..."
    [ -n "$TMP_DIR_LOCAL" ] && rm -rf "$TMP_DIR_LOCAL"
    
    if [ "$ORIGIN_TYPE" = "pod" ] && [ -n "$TMP_DIR_ORIGIN" ]; then
        # Kill any lingering tar processes related to our archive
        pod_exec "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "ps | grep '[t]ar -czf' | awk '{print \$1}' | xargs kill -9 2>/dev/null || true" || true
        pod_exec "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "rm -rf '$TMP_DIR_ORIGIN'" || true
    fi
    
    if [ "$DEST_TYPE" = "pod" ] && [ -n "$TMP_DIR_DEST" ]; then
        pod_exec "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "rm -rf '$TMP_DIR_DEST'" || true
    fi
}
trap 'cleanup' EXIT
trap 'log "Interrupted by user."; trap - EXIT; cleanup; exit 130' INT TERM

timeout_cmd() {
    local timeout=$1
    shift
    # A POSIX compliant timeout wrapper if 'timeout' command is not available everywhere
    # But usually 'timeout' is in coreutils or busybox. We will try 'timeout' first.
    if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout" "$@"
    else
        # Fallback for systems without timeout (e.g. strict POSIX)
        "$@" &
        local pid=$!
        ( sleep "$timeout" && kill -HUP $pid 2>/dev/null && sleep 1 && kill -9 $pid 2>/dev/null ) &
        local watcher=$!
        if wait $pid 2>/dev/null; then
            kill $watcher 2>/dev/null || true
            return 0
        else
            kill $watcher 2>/dev/null || true
            return 124
        fi
    fi
}

kubectl_with_retry() {
    local retries=$MAX_RETRIES
    local count=0
    while [ "$count" -le "$retries" ]; do
        if timeout_cmd "$KUBECTL_TIMEOUT" kubectl "$@"; then
            return 0
        fi
        if [ "$count" -eq "$retries" ]; then
            break
        fi
        count=$((count + 1))
        log "kubectl command failed or timed out. Retrying ($count/$retries)..."
        sleep 2
    done
    return 1
}

pod_exec() {
    local ns="$1"
    local pod="$2"
    local kubeconfig="$3"
    local cmd="$4"
    local kc_arg=""
    [ -n "$kubeconfig" ] && kc_arg="--kubeconfig=$kubeconfig"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "[DRY-RUN] kubectl $kc_arg -n $ns exec $pod -- sh -c \"$cmd\""
        return 0
    fi
    kubectl_with_retry $kc_arg -n "$ns" exec "$pod" -- sh -c "$cmd"
}

archive_local() {
    local archive="$1" org_dir="$2" org_base="$3"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "[DRY-RUN] tar -czf '$archive' -C '$org_dir' '$org_base'"
        return 0
    fi
    local count=0
    while [ "$count" -le "$MAX_RETRIES" ]; do
        tar -czf "$archive" -C "$org_dir" "$org_base" &
        local pid=$!
        
        local timer=0
        local is_done=0
        while [ "$timer" -lt "$KUBECTL_TIMEOUT" ]; do
            if ! kill -0 $pid 2>/dev/null; then
                is_done=1
                break
            fi
            sleep 1
            timer=$((timer + 1))
        done
        
        if [ "$is_done" -eq 0 ]; then
            while kill -0 $pid 2>/dev/null; do
                log "tar process is still running locally. Watching instead of respawning..."
                sleep 5
            done
        fi
        
        wait $pid
        if [ $? -eq 0 ]; then
            return 0
        fi
        
        if [ "$count" -eq "$MAX_RETRIES" ]; then
            break
        fi
        count=$((count + 1))
        log "Local tar command failed. Retrying ($count/$MAX_RETRIES)..."
        sleep 2
    done
    return 1
}

archive_pod() {
    local ns="$1" pod="$2" kc="$3" archive="$4" org_dir="$5" org_base="$6"
    local kc_arg=""
    [ -n "$kc" ] && kc_arg="--kubeconfig=$kc"
    
    if [ "$DRY_RUN" -eq 1 ]; then
        log "[DRY-RUN] kubectl $kc_arg -n $ns exec $pod -- sh -c \"tar -czf '$archive' -C '$org_dir' '$org_base'\""
        return 0
    fi
    
    local count=0
    while [ "$count" -le "$MAX_RETRIES" ]; do
        local cmd="tar -czf '$archive' -C '$org_dir' '$org_base'; echo \$? > '${archive}.exit'"
        
        timeout_cmd "$KUBECTL_TIMEOUT" kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "$cmd" || true
        
        while kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "ps aux 2>/dev/null || ps -ef 2>/dev/null || ps 2>/dev/null" | grep '[t]ar -czf' >/dev/null 2>&1; do
            log "tar process is still running remotely. Watching instead of respawning..."
            sleep 5
        done
        
        local exit_code
        exit_code=$(kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "cat '${archive}.exit' 2>/dev/null" || echo "1")
        exit_code=$(echo "$exit_code" | tr -d '\r')
        if [ "$exit_code" = "0" ]; then
            return 0
        fi
        
        if [ "$count" -eq "$MAX_RETRIES" ]; then
            break
        fi
        count=$((count + 1))
        log "Remote tar command failed (exit code $exit_code). Retrying ($count/$MAX_RETRIES)..."
        sleep 2
    done
    return 1
}

find_writable_dir_local() {
    local req_kb="$1"
    local explicit_dir="$2"
    
    if [ -n "$explicit_dir" ]; then
        if touch "$explicit_dir/.kubecopy_test" 2>/dev/null && check_disk_space_local "$explicit_dir" "$req_kb"; then
            rm -f "$explicit_dir/.kubecopy_test"
            echo "$explicit_dir"
            return 0
        fi
        log "Error: Provided local temp dir $explicit_dir is not writable or lacks space."
        read -r -p "Enter a different local temporary directory path: " explicit_dir
        if [ -n "$explicit_dir" ]; then
            find_writable_dir_local "$req_kb" "$explicit_dir"
            return $?
        fi
        return 1
    fi

    for d in "$HOME" "/tmp" "/var/tmp" "$PWD"; do
        if touch "$d/.kubecopy_test" 2>/dev/null && check_disk_space_local "$d" "$req_kb" >/dev/null 2>&1; then
            rm -f "$d/.kubecopy_test"
            echo "$d"
            return 0
        fi
    done
    
    # Fallback to checking active mounts
    if [ -r /proc/mounts ]; then
        while read -r _ mount_point type _; do
            # Skip read-only, network mounts, or obvious non-data paths
            case "$type" in
                nfs|cifs|smb*|proc|sysfs|devpts|tmpfs) continue ;;
            esac
            if touch "$mount_point/.kubecopy_test" 2>/dev/null; then
                rm -f "$mount_point/.kubecopy_test"
                echo "$mount_point"
                return 0
            fi
        done < /proc/mounts
    fi
    return 1
}

find_writable_dir_pod() {
    local ns="$1" pod="$2" kc="$3" req_kb="$4" explicit_dir="$5"
    local kc_arg=""
    [ -n "$kc" ] && kc_arg="--kubeconfig=$kc"
    
    if [ -n "$explicit_dir" ]; then
        local cmd_check_explicit="
            if touch '$explicit_dir/.kubecopy_test' 2>/dev/null; then
                rm -f '$explicit_dir/.kubecopy_test'
                avail_kb=\$(df -k '$explicit_dir' | tail -1 | awk '{print \$(NF-2)}')
                if [ \"\$avail_kb\" -ge \"$req_kb\" ]; then
                    echo \"$explicit_dir\"
                    exit 0
                fi
            fi
            exit 1
        "
        if kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "$cmd_check_explicit" >/dev/null 2>&1; then
            echo "$explicit_dir"
            return 0
        fi
        log "Error: Provided pod temp dir $explicit_dir is not writable or lacks space."
        # Because we can't easily prompt recursively inside a pod wrapper like this without messing up output capturing,
        # we'll prompt locally and recursively call
        read -r -p "Enter a different temporary directory path for pod $pod: " explicit_dir
        if [ -n "$explicit_dir" ]; then
            find_writable_dir_pod "$ns" "$pod" "$kc" "$req_kb" "$explicit_dir"
            return $?
        fi
        return 1
    fi

    local cmd="
        check_space() {
            avail_kb=\$(df -k \"\$1\" | tail -1 | awk '{print \$(NF-2)}')
            if [ -n \"\$avail_kb\" ] && [ \"\$avail_kb\" -ge \"$req_kb\" ]; then return 0; fi
            return 1
        }
        for d in /tmp /var/tmp /home /root /; do 
            if touch \"\$d/.kubecopy_test\" 2>/dev/null && check_space \"\$d\"; then 
                rm -f \"\$d/.kubecopy_test\"; echo \"\$d\"; exit 0; 
            fi; 
        done
        
        if [ -r /proc/mounts ]; then
            while read -r _ mount_point type _; do
                case "$type" in nfs|cifs|smb*|proc|sysfs|devpts|tmpfs) continue ;; esac
                if touch "$mount_point/.kubecopy_test" 2>/dev/null && check_space "$mount_point"; then
                    rm -f "$mount_point/.kubecopy_test"; echo "$mount_point"; exit 0
                fi
            done < /proc/mounts
        fi
        exit 1
    "
    local found
    found=$(kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "$cmd")
    if [ -n "$found" ]; then
        echo "$found"
        return 0
    fi
    # If we got here, defaults failed. Let's prompt.
    log "Error: Default temporary paths on pod $pod lack space ($req_kb KB) or write access."
    local explicit_dir
    read -r -p "Enter a temporary directory path for pod $pod manually: " explicit_dir
    if [ -n "$explicit_dir" ]; then
        find_writable_dir_pod "$ns" "$pod" "$kc" "$req_kb" "$explicit_dir"
        return $?
    fi
    return 1
}

check_disk_space_local() {
    local dir="$1" req_kb="$2"
    local avail_kb
    # use tail -1 to handle very long paths that wrap in df output
    avail_kb=$(df -k "$dir" | tail -1 | awk '{print $(NF-2)}')
    if [ "$avail_kb" -lt "$req_kb" ]; then
        log "Error: Not enough space in $dir. Required: $req_kb KB, Available: $avail_kb KB"
        return 1
    fi
    return 0
}

check_disk_space_pod() {
    local ns="$1" pod="$2" kc="$3" dir="$4" req_kb="$5"
    local kc_arg=""
    [ -n "$kc" ] && kc_arg="--kubeconfig=$kc"
    local avail_kb
    avail_kb=$(kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "df -k $dir | tail -1 | awk '{print \$(NF-2)}'")
    if [ -z "$avail_kb" ] || [ "$avail_kb" -lt "$req_kb" ]; then
        log "Error: Not enough space in pod $pod:$dir. Required: $req_kb KB, Available: ${avail_kb:-0} KB"
        return 1
    fi
    return 0
}

get_size_local() {
    local path="$1"
    du -sk "$path" | awk '{print $1}'
}

get_size_pod() {
    local ns="$1" pod="$2" kc="$3" path="$4"
    local kc_arg=""
    [ -n "$kc" ] && kc_arg="--kubeconfig=$kc"
    kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "du -sk $path | awk '{print \$1}'"
}

split_file_local() {
    local file="$1" prefix="$2" chunk_size="$3"
    local file_size
    file_size=$(wc -c < "$file" | tr -d ' ')
    local total_chunks=$(( (file_size + chunk_size - 1) / chunk_size ))
    local i=0
    while [ "$i" -lt "$total_chunks" ]; do
        dd if="$file" of="${prefix}_${i}" bs="$chunk_size" skip="$i" count=1 2>/dev/null
        i=$((i+1))
    done
    echo "$total_chunks"
}

split_file_pod() {
    local ns="$1" pod="$2" kc="$3" file="$4" prefix="$5" chunk_size="$6"
    local kc_arg=""
    [ -n "$kc" ] && kc_arg="--kubeconfig=$kc"
    
    local cmd="
        file_size=\$(wc -c < '$file' | tr -d ' ')
        total_chunks=\$(( (file_size + $chunk_size - 1) / $chunk_size ))
        i=0
        while [ \"\$i\" -lt \"\$total_chunks\" ]; do
            dd if='$file' of='${prefix}_'\$i bs=$chunk_size skip=\$i count=1 2>/dev/null
            i=\$((i+1))
        done
        echo \"\$total_chunks\"
    "
    kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "$cmd"
}

get_hash_local() {
    local file="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | awk '{print $1}'
    elif command -v md5sum >/dev/null 2>&1; then
        md5sum "$file" | awk '{print $1}'
    elif command -v md5 >/dev/null 2>&1; then
        md5 -q "$file" 2>/dev/null || md5 "$file" | awk '{print $1}'
    else
        # Fallback if no hash tool
        echo "nohash"
    fi
}

get_hash_pod() {
    local ns="$1" pod="$2" kc="$3" file="$4"
    local kc_arg=""
    [ -n "$kc" ] && kc_arg="--kubeconfig=$kc"
    
    local cmd='
        if command -v sha256sum >/dev/null 2>&1; then sha256sum "'$file'" | awk "{print \$1}";
        elif command -v md5sum >/dev/null 2>&1; then md5sum "'$file'" | awk "{print \$1}";
        elif command -v md5 >/dev/null 2>&1; then md5 -q "'$file'" 2>/dev/null || md5 "'$file'" | awk "{print \$1}";
        else echo "nohash"; fi
    '
    kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "$cmd"
}

rebuild_file_local() {
    local prefix="$1" total_chunks="$2" dest_file="$3"
    local i=0
    > "$dest_file"
    while [ "$i" -lt "$total_chunks" ]; do
        cat "${prefix}_${i}" >> "$dest_file"
        i=$((i+1))
    done
}

rebuild_file_pod() {
    local ns="$1" pod="$2" kc="$3" prefix="$4" total_chunks="$5" dest_file="$6"
    local kc_arg=""
    [ -n "$kc" ] && kc_arg="--kubeconfig=$kc"
    
    local cmd="
        i=0
        > '$dest_file'
        while [ \"\$i\" -lt \"$total_chunks\" ]; do
            cat '${prefix}_'\$i >> '$dest_file'
            i=\$((i+1))
        done
    "
    kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "$cmd"
}

prompt_if_empty() {
    local var_name="$1"
    local prompt_text="$2"
    eval "local val=\"\$$var_name\""
    if [ -z "$val" ]; then
        read -r -p "$prompt_text" val
        eval "$var_name=\"\$val\""
        PROMPTED=1
    fi
}

main() {
    # Parse Args
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --help|-h) show_help ;;
            --version) echo "kubecopy $VERSION"; exit 0 ;;
            --dry-run) DRY_RUN=1; shift ;;
            --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
            --retries) MAX_RETRIES="$2"; shift 2 ;;
            --timeout) KUBECTL_TIMEOUT="$2"; shift 2 ;;
            --origin-type) ORIGIN_TYPE="$2"; shift 2 ;;
            --origin-path) ORIGIN_PATH="$2"; shift 2 ;;
            --origin-ns) ORIGIN_NS="$2"; shift 2 ;;
            --origin-pod) ORIGIN_POD="$2"; shift 2 ;;
            --origin-kubeconfig) ORIGIN_KUBECONFIG="$2"; shift 2 ;;
            --origin-tmp-dir) TMP_DIR_ORIGIN_ARG="$2"; shift 2 ;;
            --dest-type) DEST_TYPE="$2"; shift 2 ;;
            --dest-path) DEST_PATH="$2"; shift 2 ;;
            --dest-ns) DEST_NS="$2"; shift 2 ;;
            --dest-pod) DEST_POD="$2"; shift 2 ;;
            --dest-kubeconfig) DEST_KUBECONFIG="$2"; shift 2 ;;
            --dest-tmp-dir) TMP_DIR_DEST_ARG="$2"; shift 2 ;;
            --local-tmp-dir) TMP_DIR_LOCAL_ARG="$2"; shift 2 ;;
            *) echo "Unknown option: $1"; show_help ;;
        esac
    done

    # Interactively prompt for missing inputs
    prompt_if_empty ORIGIN_TYPE "Origin type (local or pod): "
    prompt_if_empty DEST_TYPE "Destination type (local or pod): "
    prompt_if_empty ORIGIN_PATH "Origin path (directory or file): "
    
    if [ "$ORIGIN_TYPE" = "pod" ]; then
        prompt_if_empty ORIGIN_NS "Origin namespace: "
        prompt_if_empty ORIGIN_POD "Origin pod name: "
        prompt_if_empty ORIGIN_KUBECONFIG "Origin kubeconfig file path (leave empty for default): "
    fi

    prompt_if_empty DEST_PATH "Destination path (directory or file): "

    if [ "$DEST_TYPE" = "pod" ]; then
        prompt_if_empty DEST_NS "Destination namespace: "
        prompt_if_empty DEST_POD "Destination pod name: "
        prompt_if_empty DEST_KUBECONFIG "Destination kubeconfig file path (leave empty for default): "
    fi

    # Basic Validations
    if [ "$ORIGIN_TYPE" != "local" ] && [ "$ORIGIN_TYPE" != "pod" ]; then
        log "Error: Origin type must be 'local' or 'pod'."
        exit 1
    fi
    if [ "$DEST_TYPE" != "local" ] && [ "$DEST_TYPE" != "pod" ]; then
        log "Error: Destination type must be 'local' or 'pod'."
        exit 1
    fi
    
    if [ "$PROMPTED" -eq 1 ]; then
        # Build the reproduction command
        local repro_cmd="kubecopy --origin-type \"$ORIGIN_TYPE\" --origin-path \"$ORIGIN_PATH\" --dest-type \"$DEST_TYPE\" --dest-path \"$DEST_PATH\""
        if [ "$ORIGIN_TYPE" = "pod" ]; then
            repro_cmd="$repro_cmd --origin-ns \"$ORIGIN_NS\" --origin-pod \"$ORIGIN_POD\""
            [ -n "$ORIGIN_KUBECONFIG" ] && repro_cmd="$repro_cmd --origin-kubeconfig \"$ORIGIN_KUBECONFIG\""
        fi
        if [ "$DEST_TYPE" = "pod" ]; then
            repro_cmd="$repro_cmd --dest-ns \"$DEST_NS\" --dest-pod \"$DEST_POD\""
            [ -n "$DEST_KUBECONFIG" ] && repro_cmd="$repro_cmd --dest-kubeconfig \"$DEST_KUBECONFIG\""
        fi
        [ "$CHUNK_SIZE" != "$((5 * 1024 * 1024))" ] && repro_cmd="$repro_cmd --chunk-size \"$CHUNK_SIZE\""
        [ "$MAX_RETRIES" != "3" ] && repro_cmd="$repro_cmd --retries \"$MAX_RETRIES\""
        [ "$KUBECTL_TIMEOUT" != "30" ] && repro_cmd="$repro_cmd --timeout \"$KUBECTL_TIMEOUT\""
        [ "$DRY_RUN" -eq 1 ] && repro_cmd="$repro_cmd --dry-run"
        
        echo ""
        echo "==========================================================="
        echo "To repeat this exact operation without prompts, use:"
        echo "$repro_cmd"
        echo "==========================================================="
        echo ""
    fi

    log "Starting kubecopy from $ORIGIN_TYPE:$ORIGIN_PATH to $DEST_TYPE:$DEST_PATH"

    local REQ_KB=0
    # Determine Required KB upfront because we need it to negotiate temporary space
    if [ "$ORIGIN_TYPE" = "local" ]; then
        REQ_KB=$(get_size_local "$ORIGIN_PATH")
    else
        REQ_KB=$(get_size_pod "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "$ORIGIN_PATH")
    fi

    # 1. Setup specific temporary directories
    if [ "$ORIGIN_TYPE" = "local" ] || [ "$DEST_TYPE" = "local" ]; then
        local w_local
        w_local=$(find_writable_dir_local "$REQ_KB" "$TMP_DIR_LOCAL_ARG")
        if [ -z "$w_local" ]; then
            log "Error: Could not find writable directory on local machine with explicit fallback prompted."
            # Final fallback prompt locally if automatic defaults failed entirely.
            read -r -p "Enter a temporary directory path manually for local machine: " TMP_DIR_LOCAL_ARG
            if [ -n "$TMP_DIR_LOCAL_ARG" ]; then
                w_local=$(find_writable_dir_local "$REQ_KB" "$TMP_DIR_LOCAL_ARG")
            fi
            if [ -z "$w_local" ]; then
                exit 1
            fi
        fi
        TMP_DIR_LOCAL="$w_local/kubecopy_local_$$"
        mkdir -p "$TMP_DIR_LOCAL"
    fi

    if [ "$ORIGIN_TYPE" = "pod" ]; then
        local w_pod_org
        w_pod_org=$(find_writable_dir_pod "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "$REQ_KB" "$TMP_DIR_ORIGIN_ARG")
        if [ -z "$w_pod_org" ]; then
            log "Error: Could not find or allocate writable directory on origin pod."
            exit 1
        fi
        TMP_DIR_ORIGIN="$w_pod_org/kubecopy_org_$$"
        pod_exec "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "mkdir -p '$TMP_DIR_ORIGIN'"
    fi

    if [ "$DEST_TYPE" = "pod" ]; then
        local w_pod_dst
        w_pod_dst=$(find_writable_dir_pod "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "$REQ_KB" "$TMP_DIR_DEST_ARG")
        if [ -z "$w_pod_dst" ]; then
            log "Error: Could not find or allocate writable directory on destination pod."
            exit 1
        fi
        TMP_DIR_DEST="$w_pod_dst/kubecopy_dst_$$"
        pod_exec "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "mkdir -p '$TMP_DIR_DEST'"
    fi

    # 2. Check Disk Space & Archive
    local ARCHIVE_NAME="archive.tar.gz"
    local ARCHIVE_LOCAL="$TMP_DIR_LOCAL/$ARCHIVE_NAME"
    local TOTAL_CHUNKS=0
    local ORIGIN_HASH=""

    if [ "$ORIGIN_TYPE" = "local" ]; then
        log "Archiving local path $ORIGIN_PATH..."
        archive_local "$ARCHIVE_LOCAL" "$(dirname "$ORIGIN_PATH")" "$(basename "$ORIGIN_PATH")"
        ORIGIN_HASH=$(get_hash_local "$ARCHIVE_LOCAL")
        log "Origin hash: $ORIGIN_HASH"
        log "Splitting archive into chunks..."
        TOTAL_CHUNKS=$(split_file_local "$ARCHIVE_LOCAL" "$TMP_DIR_LOCAL/chunk" "$CHUNK_SIZE")
        log "Total chunks: $TOTAL_CHUNKS"
    else
        log "Archiving origin pod path $ORIGIN_PATH..."
        local org_dir org_base
        org_dir="$(dirname "$ORIGIN_PATH")"
        org_base="$(basename "$ORIGIN_PATH")"
        archive_pod "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "$TMP_DIR_ORIGIN/$ARCHIVE_NAME" "$org_dir" "$org_base"
        ORIGIN_HASH=$(get_hash_pod "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "$TMP_DIR_ORIGIN/$ARCHIVE_NAME")
        log "Origin hash: $ORIGIN_HASH"
        log "Splitting archive into chunks on pod..."
        TOTAL_CHUNKS=$(split_file_pod "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "$TMP_DIR_ORIGIN/$ARCHIVE_NAME" "$TMP_DIR_ORIGIN/chunk" "$CHUNK_SIZE")
        log "Total chunks: $TOTAL_CHUNKS"
    fi

    # 3. Transfer & Reassemble
    local i=0
    local DEST_HASH=""

    if [ "$DEST_TYPE" = "local" ]; then
        while [ "$i" -lt "$TOTAL_CHUNKS" ]; do
            log "Transferring chunk $i..."
            if [ "$ORIGIN_TYPE" = "pod" ]; then
                local kc_arg=""
                [ -n "$ORIGIN_KUBECONFIG" ] && kc_arg="--kubeconfig=$ORIGIN_KUBECONFIG"
                if [ "$DRY_RUN" -eq 1 ]; then
                    log "[DRY-RUN] kubectl $kc_arg cp $ORIGIN_NS/$ORIGIN_POD:$TMP_DIR_ORIGIN/chunk_$i $TMP_DIR_LOCAL/chunk_$i"
                else
                    kubectl_with_retry $kc_arg cp "$ORIGIN_NS/$ORIGIN_POD:$TMP_DIR_ORIGIN/chunk_$i" "$TMP_DIR_LOCAL/chunk_$i"
                fi
            else
                if [ "$DRY_RUN" -eq 1 ]; then
                    log "[DRY-RUN] mv $TMP_DIR_LOCAL/chunk_$i (local-to-local simulated)"
                fi
            fi
            i=$((i+1))
        done

        if [ "$DRY_RUN" -eq 0 ]; then
            log "Reassembling chunks locally..."
            rebuild_file_local "$TMP_DIR_LOCAL/chunk" "$TOTAL_CHUNKS" "$TMP_DIR_LOCAL/$ARCHIVE_NAME"
            DEST_HASH=$(get_hash_local "$TMP_DIR_LOCAL/$ARCHIVE_NAME")
            if [ "$ORIGIN_HASH" != "$DEST_HASH" ]; then
                log "Error: Hash mismatch (Origin: $ORIGIN_HASH, Dest: $DEST_HASH)"
                exit 1
            fi
            log "Hashes match. Extracting..."
            mkdir -p "$(dirname "$DEST_PATH")"
            tar -xzf "$TMP_DIR_LOCAL/$ARCHIVE_NAME" -C "$(dirname "$DEST_PATH")"
        fi

    else
        while [ "$i" -lt "$TOTAL_CHUNKS" ]; do
            log "Transferring chunk $i..."
            
            if [ "$ORIGIN_TYPE" = "local" ]; then
                local kc_arg=""
                [ -n "$DEST_KUBECONFIG" ] && kc_arg="--kubeconfig=$DEST_KUBECONFIG"
                if [ "$DRY_RUN" -eq 1 ]; then
                    log "[DRY-RUN] kubectl $kc_arg cp $TMP_DIR_LOCAL/chunk_$i $DEST_NS/$DEST_POD:$TMP_DIR_DEST/chunk_$i"
                else
                    kubectl_with_retry $kc_arg cp "$TMP_DIR_LOCAL/chunk_$i" "$DEST_NS/$DEST_POD:$TMP_DIR_DEST/chunk_$i"
                fi
            else
                # Pod to Pod: fetch to local tmp, then push to dest
                local w_local
                w_local=$(find_writable_dir_local "$REQ_KB" "$TMP_DIR_LOCAL_ARG")
                TMP_DIR_LOCAL="$w_local/kubecopy_local_$$"
                mkdir -p "$TMP_DIR_LOCAL"
                
                local okc_arg=""
                [ -n "$ORIGIN_KUBECONFIG" ] && okc_arg="--kubeconfig=$ORIGIN_KUBECONFIG"
                local dkc_arg=""
                [ -n "$DEST_KUBECONFIG" ] && dkc_arg="--kubeconfig=$DEST_KUBECONFIG"

                if [ "$DRY_RUN" -eq 1 ]; then
                    log "[DRY-RUN] kubectl $okc_arg cp $ORIGIN_NS/$ORIGIN_POD:$TMP_DIR_ORIGIN/chunk_$i $TMP_DIR_LOCAL/chunk_$i"
                    log "[DRY-RUN] kubectl $dkc_arg cp $TMP_DIR_LOCAL/chunk_$i $DEST_NS/$DEST_POD:$TMP_DIR_DEST/chunk_$i"
                else
                    kubectl_with_retry $okc_arg cp "$ORIGIN_NS/$ORIGIN_POD:$TMP_DIR_ORIGIN/chunk_$i" "$TMP_DIR_LOCAL/chunk_$i"
                    kubectl_with_retry $dkc_arg cp "$TMP_DIR_LOCAL/chunk_$i" "$DEST_NS/$DEST_POD:$TMP_DIR_DEST/chunk_$i"
                    rm -f "$TMP_DIR_LOCAL/chunk_$i"
                fi
            fi
            i=$((i+1))
        done

        if [ "$DRY_RUN" -eq 0 ]; then
            log "Reassembling chunks on destination pod..."
            rebuild_file_pod "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "$TMP_DIR_DEST/chunk" "$TOTAL_CHUNKS" "$TMP_DIR_DEST/$ARCHIVE_NAME"
            DEST_HASH=$(get_hash_pod "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "$TMP_DIR_DEST/$ARCHIVE_NAME")
            if [ "$ORIGIN_HASH" != "$DEST_HASH" ]; then
                log "Error: Hash mismatch (Origin: $ORIGIN_HASH, Dest: $DEST_HASH)"
                exit 1
            fi
            log "Hashes match. Extracting..."
            local dst_dir
            dst_dir="$(dirname "$DEST_PATH")"
            pod_exec "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "mkdir -p '$dst_dir'"
            pod_exec "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "tar -xzf '$TMP_DIR_DEST/$ARCHIVE_NAME' -C '$dst_dir'"
        fi
    fi

    log "Transfer completed successfully."
}

# Execute main
main "$@"

