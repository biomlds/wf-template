#!/usr/bin/env nextflow

import groovy.json.JsonBuilder
nextflow.enable.dsl = 2

include {
    getParams;
} from './lib/common'


process buildRsyncContainer {
    output:
        val true, emit: built

    script:
    """
    docker build -t wf-rsync:latest -f ${projectDir}/docker/rsync/Dockerfile ${projectDir}/docker/rsync/
    """
}


process getVersions {
    label "wfbackup"
    input:
        val build_done
    publishDir "${params.out_dir}", mode: 'copy', pattern: "versions.txt"
    cpus 1

    output:
        path "versions.txt"
    script:
    """
    RSYNC_VER=\$(rsync --version 2>&1 | head -1)
    echo "rsync,\$RSYNC_VER" >> versions.txt
    """
}


process backupOntData {
    label "wfbackup"

    containerOptions { "-v ${source_path}:/home/source -v ${dest_path}:/home/dest" }

    input:
        val build_done
        val source_path
        val dest_path
        val delete_source

    publishDir "${params.out_dir}", mode: 'copy', pattern: "manifest_ont_data.json"
    cpus 1
    memory "1 GB"

    output:
        path "manifest_ont_data.json", emit: manifest
        path "backup_ont.log", emit: log
        val true, emit: success

    script:
    String src = "/home/source"
    String dst = "/home/dest/ont_data"
    """
    mkdir -p "$dst"

    echo "Starting ONT data backup..." > backup_ont.log
    echo "Source: $source_path" >> backup_ont.log
    echo "Destination: $dst" >> backup_ont.log
    echo "" >> backup_ont.log

    echo "Step 1: Initial rsync copy (excluding pod5)..." >> backup_ont.log
    rsync -av --no-owner --no-group --numeric-ids --exclude='pod5' "$src/" "$dst/" >> backup_ont.log 2>&1
    RSYNC_INIT_EXIT=\$?

    if [ \$RSYNC_INIT_EXIT -ne 0 ]; then
        echo "ERROR: Initial rsync failed with exit code \$RSYNC_INIT_EXIT" >> backup_ont.log
        exit 1
    fi
    echo "Initial copy completed successfully." >> backup_ont.log
    echo "" >> backup_ont.log

    echo "Step 2: Verification rsync with checksum..." >> backup_ont.log
    rsync -avc --no-owner --no-group --numeric-ids --checksum "$src/" "$dst/" >> backup_ont.log 2>&1
    RSYNC_VERIFY_EXIT=\$?

    if [ \$RSYNC_VERIFY_EXIT -ne 0 ]; then
        echo "ERROR: Verification rsync failed with exit code \$RSYNC_VERIFY_EXIT" >> backup_ont.log
        exit 1
    fi
    echo "Verification completed successfully." >> backup_ont.log
    echo "" >> backup_ont.log

    echo "Step 3: Generating manifest..." >> backup_ont.log
    generate_manifest.sh manifest_ont_data.json "ont_data" "$dst"
    echo "Manifest created." >> backup_ont.log
    echo "" >> backup_ont.log

    if [ "$delete_source" = "true" ]; then
        echo "Step 4: Deleting source files (backup verified)..." >> backup_ont.log
        rsync -av --no-owner --no-group --numeric-ids --exclude='pod5' --delete "$src/" /tmp/ont_backup_temp/ >> backup_ont.log 2>&1
        rm -rf "$src"
        echo "Source files deleted." >> backup_ont.log
    else
        echo "Step 4: Skipping source deletion (delete_source=false)." >> backup_ont.log
    fi

    echo "ONT backup completed successfully!" >> backup_ont.log
    """
}


process backupEpi2meData {
    label "wfbackup"

    containerOptions { "-v ${source_path}:/home/source -v ${dest_path}:/home/dest" }

    input:
        val build_done
        val source_path
        val dest_path
        val delete_source

    publishDir "${params.out_dir}", mode: 'copy', pattern: "manifest_epi2me_data.json"
    cpus 1
    memory "1 GB"

    output:
        path "manifest_epi2me_data.json", emit: manifest
        path "backup_epi2me.log", emit: log
        val true, emit: success

    script:
    String src = "/home/source"
    String dst = "/home/dest/epi2me_data"
    """
    mkdir -p "$dst"

    echo "Starting EPI2ME data backup..." > backup_epi2me.log
    echo "Source: $source_path" >> backup_epi2me.log
    echo "Destination: $dst" >> backup_epi2me.log
    echo "" >> backup_epi2me.log

    echo "Step 1: Initial rsync copy (first-level files only)..." >> backup_epi2me.log
    rsync -av --no-owner --no-group --numeric-ids --include='*' --exclude='*/*' "$src/" "$dst/" >> backup_epi2me.log 2>&1
    RSYNC_INIT_EXIT=\$?

    if [ \$RSYNC_INIT_EXIT -ne 0 ]; then
        echo "ERROR: Initial rsync failed with exit code \$RSYNC_INIT_EXIT" >> backup_epi2me.log
        exit 1
    fi
    echo "Initial copy completed successfully." >> backup_epi2me.log
    echo "" >> backup_epi2me.log

    echo "Step 2: Verification rsync with checksum..." >> backup_epi2me.log
    rsync -avc --no-owner --no-group --numeric-ids --checksum --include='*' --exclude='*/*' "$src/" "$dst/" >> backup_epi2me.log 2>&1
    RSYNC_VERIFY_EXIT=\$?

    if [ \$RSYNC_VERIFY_EXIT -ne 0 ]; then
        echo "ERROR: Verification rsync failed with exit code \$RSYNC_VERIFY_EXIT" >> backup_epi2me.log
        exit 1
    fi
    echo "Verification completed successfully." >> backup_epi2me.log
    echo "" >> backup_epi2me.log

    echo "Step 3: Generating manifest..." >> backup_epi2me.log
    generate_manifest.sh manifest_epi2me_data.json "epi2me_data" "$dst" 1
    echo "Manifest created." >> backup_epi2me.log
    echo "" >> backup_epi2me.log

    if [ "$delete_source" = "true" ]; then
        echo "Step 4: Deleting source files (backup verified)..." >> backup_epi2me.log
        rsync -av --no-owner --no-group --numeric-ids --include='*' --exclude='*/*' --delete "$src/" /tmp/epi2me_backup_temp/ >> backup_epi2me.log 2>&1
        rm -rf "$src"
        echo "Source files deleted." >> backup_epi2me.log
    else
        echo "Step 4: Skipping source deletion (delete_source=false)." >> backup_epi2me.log
    fi

    echo "EPI2ME backup completed successfully!" >> backup_epi2me.log
    """
}


process makeReport {
    label "wf_common"
    publishDir "${params.out_dir}", mode: 'copy', pattern: "wf-backup-report.html"
    input:
        path ont_manifest
        path epi2me_manifest
        path ont_log
        path epi2me_log
        path "versions/*"
        path "params.json"
        val wf_version

    output:
        path "wf-backup-report.html"

    script:
    String ont_manifest_arg = ont_manifest.exists() ? "--ont_manifest $ont_manifest" : ""
    String epi2me_manifest_arg = epi2me_manifest.exists() ? "--epi2me_manifest $epi2me_manifest" : ""
    String ont_log_arg = ont_log.exists() ? "--ont_log $ont_log" : ""
    String epi2me_log_arg = epi2me_log.exists() ? "--epi2me_log $epi2me_log" : ""
    """
    workflow-glue report wf-backup-report.html \
        $ont_manifest_arg \
        $epi2me_manifest_arg \
        $ont_log_arg \
        $epi2me_log_arg \
        --versions versions \
        --params params.json \
        --wf_version $wf_version
    """
}


workflow pipeline {
    take:
        ont_data_input
        epi2me_data_input

    main:
        // Build container - returns channel
        build_done = buildRsyncContainer()

        // These receive the build_done channel - creates dependency
        software_versions = getVersions(build_done)
        workflow_params = getParams()

        ont_results = null
        epi2me_results = null

        if (ont_data_input) {
            ont_results = backupOntData(
                build_done,
                ont_data_input.source,
                ont_data_input.dest,
                params.delete_source
            )
        }

        if (epi2me_data_input) {
            epi2me_results = backupEpi2meData(
                build_done,
                epi2me_data_input.source,
                epi2me_data_input.dest,
                params.delete_source
            )
        }

        ont_manifest = ont_results ? ont_results.manifest : file("$projectDir/data/OPTIONAL_FILE")
        epi2me_manifest = epi2me_results ? epi2me_results.manifest : file("$projectDir/data/OPTIONAL_FILE")
        ont_log = ont_results ? ont_results.log : file("$projectDir/data/OPTIONAL_FILE")
        epi2me_log = epi2me_results ? epi2me_results.log : file("$projectDir/data/OPTIONAL_FILE")

        report = makeReport(
            ont_manifest,
            epi2me_manifest,
            ont_log,
            epi2me_log,
            software_versions,
            workflow_params,
            workflow.manifest.version
        )

    emit:
        report
        telemetry = workflow_params
}


WorkflowMain.initialise(workflow, params, log)

workflow {
    Pinguscript.ping_start(nextflow, workflow, params)

    def ont_input = null
    def epi2me_input = null

    if (params.ont_data) {
        ont_input = [
            source: params.ont_data,
            dest: params.ont_data_dest
        ]
    }

    if (params.epi2me_data) {
        epi2me_input = [
            source: params.epi2me_data,
            dest: params.epi2me_data_dest
        ]
    }

    if (!ont_input && !epi2me_input) {
        log.error "No input data specified. Please provide --ont_data and/or --epi2me_data"
        exit 1
    }

    if (ont_input && !params.ont_data_dest) {
        log.error "ONT data destination not specified. Please provide --ont_data_dest"
        exit 1
    }

    if (epi2me_input && !params.epi2me_data_dest) {
        log.error "EPI2ME data destination not specified. Please provide --epi2me_data_dest"
        exit 1
    }

    pipeline(ont_input, epi2me_input)
}

workflow.onComplete {
    Pinguscript.ping_complete(nextflow, workflow, params)
}
workflow.onError {
    Pinguscript.ping_error(nextflow, workflow, params)
}
