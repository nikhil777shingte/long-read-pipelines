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

task GetBasecallModel {
    meta {
        desciption: "Getting the basecall model string of an ONT BAM"
    }
    parameter_meta {
        bam: {
            desciption: "BAM to operate on",
            localization_optional: true
        }
        runid_2_model: "The basecall model for each run."
    }
    input {
        File bam
    }
    output {
        Map[String, String] runid_2_model = read_map("results.tsv")
    }

    command <<<
        set -eux

        export GCS_OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
        samtools view -H ~{bam} | grep "^@RG" > one_rg_per_line.txt

        while IFS= read -r line
        do
            echo "$line" | tr '\t' '\n' | grep "^DS:" | sed "s/^DS://" | tr ' ' '\n' > tmp.txt
            runid=$(grep "^runid=" tmp.txt | awk -F '=' '{print $2}')
            model=$(grep "^basecall_model=" tmp.txt | awk -F '=' '{print $2}')
            echo -e "${runid}\t${model}" >> results.tsv
        done < one_rg_per_line.txt
    >>>

    runtime {
        cpu:            1
        memory:         "4 GiB"
        disks:          "local-disk 100 HDD"
        preemptible:    2
        maxRetries:     1
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.2"
    }
}

task DeduplicateONTAlignedBam {

    meta {
        description: "Utility to drop (occationally happening) literal duplicate records in input BAM."
    }

    parameter_meta {
        aligned_bam: {
            localization_optional: true,
            description: "input BAM file (must be coordinate sorted)."
        }
        aligned_bai: "input BAM index file"
        same_name_as_input: "if true, output BAM will have the same name as input BAM, otherwise it will have the input basename with .dedup suffix"
        runtime_attr_override: "override default runtime attributes"
    }

    input {
        File  aligned_bam
        File? aligned_bai

        Boolean same_name_as_input = true

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 3 * ceil(size(aligned_bam, "GB"))

    String base = basename(aligned_bam, ".bam")
    String prefix = if (same_name_as_input) then base else (base + ".dedup")

    String local_bam = "/cromwell_root/~{base}.bam"
    String local_bai = "/cromwell_root/~{base}.bam.bai"

    command <<<
        set -eux

        # here we use an optimization, that is, in stead of relying on the slow Cromwell localization,
        # we explicity localize the bam in the with gcloud storage cp
        time gcloud storage cp ~{aligned_bam} ~{local_bam}

        echo "==========================================================="
        echo "verify input bam is sorted by coordinate"
        samtools view -H ~{local_bam} | grep "@HD" > hd.line
        if ! grep -qF "SO:coordinate" hd.line;
        then
            echo "BAM must be coordinate sorted!" && echo && cat hd.line && exit 1
        fi

        echo "index if bai not provided"
        if ~{defined(aligned_bai)}; then
            mv ~{aligned_bai} ~{local_bai}
        else
            time samtools index -@3 "~{local_bam}"
        fi
        echo "==========================================================="
        echo "collecting duplicate information"
        time \
            samtools view -@ 1 "~{local_bam}" | \
            awk -F '\t' 'BEGIN{OFS="\t"} {print $1, $2, $3, $4, $5, $6}' | \
            sort | uniq -d \
            > "~{base}".duplicates.txt

        cnt=$(wc -l "~{base}".duplicates.txt | awk '{print $1}')
        if [[ ${cnt} -eq 0 ]];
        then
            echo "No duplicates found"
            if ! ~{same_name_as_input} ;
            then
                mv "~{local_bam}" "~{prefix}.bam"
                mv "~{local_bai}" "~{prefix}.bam.bai"  # when input bai isn't provided, an explicit index run happened above, so we're safe here
            fi
            exit 0
        else
            echo "Amongst the mapped reads, ${cnt} unique duplicate signates found."
        fi
        echo "==========================================================="
        echo "DE-DUPLICATION STARTED"
        # here we treat the de-duplication per-chrmosome (unmapped reads are treated separately too)

        echo "######################################"
        # unmapped deduplication
        samtools view -@3 -f4 -o unmapped.bam "~{local_bam}"
        samtools view unmapped.bam | awk -F '\t' '{print $1}' | sort | uniq -d > duplicated.unmapped.reads.txt
        touch duplicated.unmapped.reads.txt
        cat duplicated.unmapped.reads.txt
        cnt=$(wc -l duplicated.unmapped.reads.txt | awk '{print $1}')
        if [[ ${cnt} -eq 0 ]]; then
            echo "No duplicates found in the unmapped reads"
            mv unmapped.bam unmapped.dedup.bam
        else
            # sort by queryname (note that natural-order or ascii-order doesn't matter, as long as it's self-consistent)
            samtools sort -@1 -n -o unmapped.qname-sort.bam unmapped.bam
            python3 /opt/remove_duplicate_ont_namesorted_unaligned.py \
                -p unmapped.dedup.to-be-sorted \
                -q unmapped.dup-reads-by-python.txt \
                unmapped.qname-sort.bam
            cat unmapped.dup-reads-by-python.txt
            samtools sort -@1 -o unmapped.dedup.bam unmapped.dedup.to-be-sorted.bam

            rm unmapped.bam unmapped.qname-sort.bam unmapped.dedup.to-be-sorted.bam  # save disk space
        fi
        echo "######################################"
        # per-chr de-duplication
        ##########
        # first, see which chromosomes needs deduplication
        awk -F '\t' '{print $3}' "~{base}".duplicates.txt | sort | uniq > contigs.with.dups.txt
        cat contigs.with.dups.txt
        samtools view -H "~{local_bam}" | grep "^@SQ" | awk -F '\t' '{print $2}' | awk -F ':' '{print $2}' > all.contigs.in.reference.txt
        comm -23 <(sort all.contigs.in.reference.txt) contigs.with.dups.txt | sort -V > contigs.without.dups.txt
        cat contigs.without.dups.txt
        ##########
        # split the bam into those don't need dedup, and those that need
        date
        samtools view -@1 -bh \
            -o no.duplicates.bam \
            --regions-file contigs.without.dups.txt \
            "~{local_bam}"  &
        while IFS= read -r chromosome; do
            samtools view -@1 -bh -o "${chromosome}".with.duplicates.bam "~{local_bam}" "${chromosome}" &
        done < contigs.with.dups.txt
        wait
        date
        ##########
        # create per-chr duplicate signatures files
        while IFS= read -r chromosome; do
            grep -E "^${chromosome}\t" "~{base}".duplicates.txt > "${chromosome}.per-chr.duplicates.txt"
        done < contigs.with.dups.txt
        ls ./*.per-chr.duplicates.txt
        ##########
        # deduplicate those that need actions
        date
        while IFS= read -r chromosome; do
            python3 /opt/remove_duplicate_ont_aln.py \
                "${chromosome}".with.duplicates.bam \
                --prefix "${chromosome}".dedup \
                --annotations "${chromosome}.per-chr.duplicates.txt" &
        done < contigs.with.dups.txt
        wait
        date
        echo "######################################"
        # merge, including those unmapped reads
        date
        samtools view -H "~{local_bam}" > original.header
        rm "~{local_bam}" "~{local_bai}"
        rm ./*.with.duplicates.bam

        echo "no.duplicates.bam" > to.merge.list
        ls ./*.dedup.bam >> to.merge.list

        samtools merge \
            -@5 \
            -o "~{prefix}.bam" \
            -h original.header \
            -c \
            -b to.merge.list

        date
        echo "==========================================================="
        echo "DONE"
        samtools index -@3 "~{prefix}.bam"
        cat ./*.per-chr.duplicates.txt > "~{prefix}.duplicate.signatures.txt"
    >>>

    output {
        File corrected_bam = "~{prefix}.bam"
        File corrected_bai = "~{prefix}.bam.bai"
        File duplicate_record_signatures = "~{prefix}.duplicate.signatures.txt"
        File duplicate_unmapped_readnames  =  "duplicated.unmapped.reads.txt"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          8,
        mem_gb:             32,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-bam-dedup:0.1.2"
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
