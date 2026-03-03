#!/usr/bin/env sh
# kubecopy.sh - Robust file copying between local and Kubernetes pods.
# Supports large files (>5MB) via tar & dd split, hashes, and retries.
set -e

VERSION="1.0.0"
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
EOF
    exit 0
}

cleanup() {
    log "Cleaning up temporary directories..."
    [ -n "$TMP_DIR_LOCAL" ] && rm -rf "$TMP_DIR_LOCAL"
    
    if [ "$ORIGIN_TYPE" = "pod" ] && [ -n "$TMP_DIR_ORIGIN" ]; then
        pod_exec "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "rm -rf '$TMP_DIR_ORIGIN'" || true
    fi
    
    if [ "$DEST_TYPE" = "pod" ] && [ -n "$TMP_DIR_DEST" ]; then
        pod_exec "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "rm -rf '$TMP_DIR_DEST'" || true
    fi
}
trap cleanup EXIT INT TERM

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
    while [ $count -le "$retries" ]; step_count=$((count+1)); count=$step_count; do
        if timeout_cmd "$KUBECTL_TIMEOUT" kubectl "$@"; then
            return 0
        fi
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

find_writable_dir_local() {
    for d in "/dev/shm" "$HOME" "/tmp" "/var/tmp" "$PWD"; do
        if touch "$d/.kubecopy_test" 2>/dev/null; then
            rm -f "$d/.kubecopy_test"
            echo "$d"
            return 0
        fi
    done
    return 1
}

find_writable_dir_pod() {
    local ns="$1" pod="$2" kc="$3"
    local cmd='for d in /dev/shm /tmp /var/tmp /home /root /; do if touch "$d/.kubecopy_test" 2>/dev/null; then rm -f "$d/.kubecopy_test"; echo "$d"; exit 0; fi; done; exit 1'
    local kc_arg=""
    [ -n "$kc" ] && kc_arg="--kubeconfig=$kc"
    kubectl $kc_arg -n "$ns" exec "$pod" -- sh -c "$cmd"
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
            --origin-type) ORIGIN_TYPE="$2"; shift 2 ;;
            --origin-path) ORIGIN_PATH="$2"; shift 2 ;;
            --origin-ns) ORIGIN_NS="$2"; shift 2 ;;
            --origin-pod) ORIGIN_POD="$2"; shift 2 ;;
            --origin-kubeconfig) ORIGIN_KUBECONFIG="$2"; shift 2 ;;
            --dest-type) DEST_TYPE="$2"; shift 2 ;;
            --dest-path) DEST_PATH="$2"; shift 2 ;;
            --dest-ns) DEST_NS="$2"; shift 2 ;;
            --dest-pod) DEST_POD="$2"; shift 2 ;;
            --dest-kubeconfig) DEST_KUBECONFIG="$2"; shift 2 ;;
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

    log "Starting kubecopy from $ORIGIN_TYPE:$ORIGIN_PATH to $DEST_TYPE:$DEST_PATH"

    # 1. Setup specific temporary directories
    if [ "$ORIGIN_TYPE" = "local" ] || [ "$DEST_TYPE" = "local" ]; then
        local w_local
        w_local=$(find_writable_dir_local)
        if [ -z "$w_local" ]; then
            log "Error: Could not find writable directory on local machine."
            exit 1
        fi
        TMP_DIR_LOCAL="$w_local/kubecopy_local_$$"
        mkdir -p "$TMP_DIR_LOCAL"
    fi

    if [ "$ORIGIN_TYPE" = "pod" ]; then
        local w_pod_org
        w_pod_org=$(find_writable_dir_pod "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG")
        if [ -z "$w_pod_org" ]; then
            log "Error: Could not find writable directory on origin pod."
            exit 1
        fi
        TMP_DIR_ORIGIN="$w_pod_org/kubecopy_org_$$"
        pod_exec "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "mkdir -p '$TMP_DIR_ORIGIN'"
    fi

    if [ "$DEST_TYPE" = "pod" ]; then
        local w_pod_dst
        w_pod_dst=$(find_writable_dir_pod "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG")
        if [ -z "$w_pod_dst" ]; then
            log "Error: Could not find writable directory on destination pod."
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
    local REQ_KB=0

    if [ "$ORIGIN_TYPE" = "local" ]; then
        REQ_KB=$(get_size_local "$ORIGIN_PATH")
        check_disk_space_local "$TMP_DIR_LOCAL" "$REQ_KB" || exit 1
        log "Archiving local path $ORIGIN_PATH..."
        tar -czf "$ARCHIVE_LOCAL" -C "$(dirname "$ORIGIN_PATH")" "$(basename "$ORIGIN_PATH")"
        ORIGIN_HASH=$(get_hash_local "$ARCHIVE_LOCAL")
        log "Origin hash: $ORIGIN_HASH"
        log "Splitting archive into chunks..."
        TOTAL_CHUNKS=$(split_file_local "$ARCHIVE_LOCAL" "$TMP_DIR_LOCAL/chunk" "$CHUNK_SIZE")
        log "Total chunks: $TOTAL_CHUNKS"
    else
        REQ_KB=$(get_size_pod "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "$ORIGIN_PATH")
        check_disk_space_pod "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "${TMP_DIR_ORIGIN%/*}" "$REQ_KB" || exit 1
        log "Archiving origin pod path $ORIGIN_PATH..."
        local org_dir org_base
        org_dir="$(dirname "$ORIGIN_PATH")"
        org_base="$(basename "$ORIGIN_PATH")"
        pod_exec "$ORIGIN_NS" "$ORIGIN_POD" "$ORIGIN_KUBECONFIG" "tar -czf '$TMP_DIR_ORIGIN/$ARCHIVE_NAME' -C '$org_dir' '$org_base'"
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
        check_disk_space_local "$TMP_DIR_LOCAL" "$REQ_KB" || exit 1
        
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
        check_disk_space_pod "$DEST_NS" "$DEST_POD" "$DEST_KUBECONFIG" "${TMP_DIR_DEST%/*}" "$REQ_KB" || exit 1

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
                w_local=$(find_writable_dir_local)
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

