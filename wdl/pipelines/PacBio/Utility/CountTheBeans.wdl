version 1.0

import "../../../tasks/Utility/Finalize.wdl" as FF

workflow CountTheBeans {
    meta {
        desciption: "For PacBio files, gather information about reads with and without the ML/MM tags for methylation."
    }
    input {
        File  bam
        File? bai
        String gcs_out_root_dir
        String bam_descriptor
        Boolean use_local_ssd = false
    }

    String out_dir = "RecordsWithout5mcMethylTags/" + basename(bam, ".bam") + "." + bam_descriptor

    call Count { input: bam = bam, bai = bai, disk_type = if(use_local_ssd) then "LOCAL" else "SSD"}

    call GatherBitter { input: bam = bam, bai = bai, disk_type = if(use_local_ssd) then "LOCAL" else "SSD"}
    call FF.FinalizeToDir {
        input:
            files = [GatherBitter.no_ml_reads, GatherBitter.no_mm_reads,
                     GatherBitter.names_missing_only_one_tag, GatherBitter.names_missing_both_tags],
            outdir = sub(gcs_out_root_dir, "/$", "") + "/~{out_dir}"
    }

    output {
        Map[String, String] methyl_tag_simple_stats = {
                                        'raw_record_cnt': Count.raw_count,
                                        'raw_record_with-mm-ml_cnt': Count.bean_count,
                                        'primary_record_cnt': Count.non_2304_count,
                                        'primary_record_with-mm-ml_cnt': Count.non_2304_bean_count,
                                        'files_holding_reads_without_tags': FinalizeToDir.gcs_dir
        }
    }
}

task Count {
    meta {
        desciption: "Count the numbers of records in the bam with and without the ML & MM tags"
    }
    input {
        File  bam
        File? bai
        String disk_type
    }

    output {
        Int raw_count  = read_int("raw_count.txt")
        Int bean_count = read_int("bean_count.txt")
        Int non_2304_count = read_int("non_2304_count.txt")
        Int non_2304_bean_count = read_int("non_2304_bean_count.txt")
    }

    command <<<
        set -eux

        samtools view -@1 -c ~{bam} > raw_count.txt &
        samtools view -@1 -c -F 2304 ~{bam} > non_2304_count.txt &

        samtools view -@1 ~{bam} | grep "ML:B:C" | grep -c "MM:Z" > bean_count.txt &
        samtools view -@1 -F 2304  ~{bam} | grep "ML:B:C" | grep -c "MM:Z" > non_2304_bean_count.txt &

        wait
    >>>

    runtime {
        cpu:            10
        memory:         "40 GiB"
        disks:          "local-disk 375 ~{disk_type}"
        preemptible:    2
        maxRetries:     1
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
    }
}

task GatherBitter {
    meta {
        desciption: "Collect records in the bam without the ML & MM tags"
    }

    input {
        File  bam
        File? bai
        String disk_type
    }

    String p = basename(bam, ".bam")

    output {
        File no_ml_reads = "~{p}.no_ML.bam"
        File no_mm_reads = "~{p}.no_MM.bam"

        File names_missing_only_one_tag = "missing_only_one_tag.read_names.txt"
        File names_missing_both_tags    = "no_mm_and_ml.read_names.txt"
    }

    Int disk_size = if (size(bam, "GB")>200) then 6000 else 375

    command <<<
        set -eux

        samtools view -@3 -h ~{bam} \
            | grep -v "ML:B:C" \
            | samtools view -bh \
            -o "~{p}.no_ML.bam" &

        samtools view -@3 -h ~{bam} \
            | grep -v "MM:Z" \
            | samtools view -bh \
            -o "~{p}.no_MM.bam" &

        wait

        samtools view "~{p}.no_ML.bam" | awk -F '\t' '{print $1}' | sort > no_ml.txt
        samtools view "~{p}.no_MM.bam" | awk -F '\t' '{print $1}' | sort > no_mm.txt
        comm -3 \
            no_ml.txt \
            no_mm.txt \
        > "missing_only_one_tag.read_names.txt"
        comm -12 \
            no_ml.txt \
            no_mm.txt \
        > "no_mm_and_ml.read_names.txt"
    >>>

    runtime {
        cpu:            10
        memory:         "40 GiB"
        disks:          "local-disk ~{disk_size} ~{disk_type}"
        preemptible:    2
        maxRetries:     1
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
    }
}
