#!/bin/sh
# Generate manifest JSON for backed up files
# Usage: generate_manifest.sh <output_json> <backup_type> <dest_dir> [maxdepth]

OUTPUT="$1"
TYPE="$2"
DEST_DIR="$3"
MAXDEPTH="$4"

echo "{\"backup_type\": \"$TYPE\", \"files\": [ " > "$OUTPUT"

if [ -n "$MAXDEPTH" ]; then
    FIND_CMD="find \"$DEST_DIR\" -type f -maxdepth $MAXDEPTH -print0"
else
    FIND_CMD="find \"$DEST_DIR\" -type f -print0"
fi

$FIND_CMD | xargs -0 md5sum | awk 'NR>1{printf ","} {printf "{\"checksum\": \"%s\", \"path\": \"%s\"}", $1, $2}' >> "$OUTPUT"

if [ -n "$MAXDEPTH" ]; then
    COUNT=$($FIND_CMD | wc -l)
else
    COUNT=$(find "$DEST_DIR" -type f | wc -l)
fi

echo " ], \"total_files\": $COUNT}" >> "$OUTPUT"
