version 1.0

import "../../structs/Structs.wdl"

task FindSequencingSummaryFiles {

    meta {
        description: "Find sequencing summary files in an ONT basecall directory."
    }

    parameter_meta {
        gcs_input_dir: "GCS directory containing sequencing summary files."
        runtime_attr_override: "Override default runtime attributes."
    }

    input {
        String gcs_input_dir

        RuntimeAttr? runtime_attr_override
    }

    String indir = sub(gcs_input_dir, "/$", "")

    command <<<
        for summary_file in $(gsutil ls "~{indir}/**sequencing_summary*.txt*")
        do
            DIR=$(dirname $summary_file)
            echo ${DIR}

            gsutil ls "${DIR}" | grep fastq_pass && gsutil ls "${DIR}" | grep fast5_pass

            if [ $? -eq 0 ]; then
                FASTQ_COUNT=$(gsutil ls "${DIR}/fastq_pass/*.fastq*" | wc -l)
                FAST5_COUNT=$(gsutil ls "${DIR}/fast5_pass/*.fast5*" | wc -l)

                echo "${FASTQ_COUNT} ${FAST5_COUNT}"

                if [ ${FASTQ_COUNT} -eq ${FAST5_COUNT} ]; then
                    echo $summary_file >> summaries.txt
                else
                    echo "# fastq != # fast5.  Skipped."
                fi
            else
                echo "No passing fastq and fast5 files.  Skipped."
            fi

            echo ""
        done
    >>>

    output {
        Array[String] summary_files = read_lines("summaries.txt")
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             1,
        disk_gb:            1,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-utils:0.1.8"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task GetRunInfo {

    meta{
        description: "Get ONT run info from a final summary file."
    }

    parameter_meta {
        final_summary: "Sequencing summary file."
        runtime_attr_override: "Override default runtime attributes."
    }

    input {
        String final_summary

        RuntimeAttr? runtime_attr_override
    }

    command <<<
        set -euxo pipefail

        gsutil cat "~{final_summary}" | sed 's/=[[:space:]]*$/=unknown/' | sed 's/=/\t/g' > run_info.txt
    >>>

    output {
        Map[String, String] run_info = read_map("run_info.txt")
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             1,
        disk_gb:            1,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-utils:0.1.8"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task ListFiles {

    meta {
        description: "List files in a GCS directory."
    }

    parameter_meta {
        sequencing_summary: "Sequencing summary file."
        suffix: "Suffix of files to list."
        runtime_attr_override: "Override default runtime attributes."
    }

    input {
        String sequencing_summary
        String suffix

        RuntimeAttr? runtime_attr_override
    }

    String indir = sub(sub(sequencing_summary, basename(sequencing_summary), ""), "/$", "")

    command <<<
        set -euxo pipefail

        gsutil ls "~{indir}/**.~{suffix}*" | grep -v fail > files.txt
        cat files.txt | wc -l > lc.txt
    >>>

    output {
        File manifest = "files.txt"
        Int count = read_int("lc.txt")
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             1,
        disk_gb:            1,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-utils:0.1.8"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task PartitionManifest {

    meta {
        description: "Partition a manifest into chunks."
    }

    parameter_meta {
        manifest: "Manifest to partition."
        N: "Number of chunks to partition into."
        runtime_attr_override: "Override default runtime attributes."
    }

    input {
        File manifest
        Int N

        RuntimeAttr? runtime_attr_override
    }

    command <<<
        set -euxo pipefail

        split -a 5 -d --additional-suffix=.txt -e -n l/~{N} ~{manifest} manifest_chunk_
    >>>

    output {
        Array[File] manifest_chunks = glob("manifest_chunk_*.txt")
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             1,
        disk_gb:            1,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-utils:0.1.8"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task GetDuplicateReadNamesInQnameSortedAlignedONTBam {
    meta {
        desciption: "Get read names from a queryname sorted, unaligned ONT bam, where such reads are duplicate records"
    }
    parameter_meta {
        qs_uBAM: {
            localization_optional: true
        }
    }
    input {
        File qs_uBAM
    }

    output {
        File dup_names_txt = "dup_read_names.txt"
    }

    command <<<
        # the way this works is the following:
        # 0) relying on the re-auth.sh script to export the credentials
        # 1) perform the remote sam-view subsetting in the background
        # 2) listen to the PID of the background process, while re-auth every 1200 seconds
        source /opt/re-auth.sh
        set -euxo pipefail

        # assumption
        sort_order=$(samtools view -H ~{qs_uBAM} | grep "^@HD" | tr '\t' '\n' | grep "^SO:" | awk -F ':' '{print $2}')
        if [[ "queryname" != "${sort_order}"  ]]; then echo -e "Sort order ${sort_oder} isn't the expected 'queryname' " && exit 1; fi

        # check the input isn't aligned
        set +e
        mode=$(samtools view ~{qs_uBAM} | head | cut -f 2-6 | sort | uniq | tr '\t' ' ')
        # flag, ref-contig, ref-pos, mapq, cigar
        if [[ "4 * 0 0 *" != "${mode}" ]]; then echo "Input BAM might be aligned, not unaligned as expected." && exit 1; fi
        set -e

        # remote grab read names
        echo "false" > samtools.failed.txt
        samtools view ~{qs_uBAM} | awk -F '\t' '{print $1}' | uniq -d  > "dup_read_names.txt" || { echo "true" > samtools.failed.txt; exit 77; } &
        pid=$!
        set +e
        count=0
        while true; do
            sleep 1200 && date && source /opt/re-auth.sh
            count=$(( count+1 ))
            if [[ ${count} -gt 6 ]]; then exit 0; fi
            if ! pgrep -x -P $pid; then exit 0; fi
        done
    >>>

    runtime {
        cpu:            2
        memory:         "8 GiB"
        disks:          "local-disk 10 HDD"
        preemptible:    2
        maxRetries:     1
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.2"
    }

}

task DeduplicateReadNamesInQnameSortedAlignedONTBam {

    meta {
        description: "Utility to drop (occationally happening) literal duplicate records in a queryname sorted, unaligned ONT bam"
    }

    parameter_meta {
        same_name_as_input: "if true, output BAM will have the same name as input BAM, otherwise it will have the input basename with .dedup suffix"
    }

    input {
        File qs_uBAM

        Boolean same_name_as_input

        RuntimeAttr? runtime_attr_override
    }

    output {
        File corrected_bam = "~{prefix}.bam"
        File duplicate_querynames = "~{prefix}.dup_querynames.txt"
    }

    Int disk_size = 10 + 3 * ceil(size(qs_uBAM, "GB"))

    String base = basename(qs_uBAM, ".bam")
    String prefix = if (same_name_as_input) then base else (base + ".dedup")

    command <<<
        set -eux

        samtools view -H "~{qs_uBAM}" | grep "@HD" > hd.line
        if ! grep -qF "SO:queryname" hd.line; then
            echo "BAM must be queryname sorted!" && echo && cat hd.line && exit 1
        fi

        # check the input isn't aligned
        set +e
        mode=$(samtools view ~{qs_uBAM} | head | cut -f 2-6 | sort | uniq | tr '\t' ' ')
        # flag, ref-contig, ref-pos, mapq, cigar
        if [[ "4 * 0 0 *" != "${mode}" ]]; then echo "Input BAM might be aligned, not unaligned as expected." && exit 1; fi
        set -e

        echo "==========================================================="
        echo "de-duplicating"
        time python3 /opt/remove_duplicate_ont_namesorted_unaligned.py \
            "~{qs_uBAM}" \
            --prefix "~{prefix}.dedup" \
            --qnames "~{prefix}.dup_querynames.txt"
        echo "==========================================================="
        echo "DONE"
        rm "~{qs_uBAM}" && mv "~{prefix}.dedup.bam" "~{prefix}.bam"
    >>>

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          4,
        mem_gb:             16,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-bam-dedup:0.1.1"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

