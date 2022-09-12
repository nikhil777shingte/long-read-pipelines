version 1.0

task GetReadGroupInfo {
    meta {
        desciption:
        "Get some read group information Given a single-readgroup BAM. Will fail if the information isn't present."
    }

    input {
        String uBAM  # not using file as call-caching brings not much benefit

        Array[String] keys
    }

    parameter_meta {
        keys: "A list of requested fields in the RG line, e.g. ID, SM, LB."
    }

    command <<<
        set -eux

        export GCS_OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
        samtools view -H ~{uBAM} | grep "^@RG" | tr '\t' '\n' > rh_header.txt

        for attribute in ~{sep=' ' keys}; do
            value=$(grep "^${attribute}" rh_header.txt | awk -F ':' '{print $2}')
            echo -e "${attribute}\t${value}" >> "result.txt"
        done
    >>>

    output {
        Map[String, String] read_group_info = read_map("result.txt")
    }

    runtime {
        cpu:            1
        memory:         "4 GiB"
        disks:          "local-disk 100 HDD"
        bootDiskSizeGb: 10
        preemptible:    2
        maxRetries:     1
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
    }
}

task GetPileup {
    meta {
        desciption:
        "Get pileup information with samtools mpileup. Current cmdline options are '-a -s -q 1 [-E|-B]' "
    }

    input {
        File bam
        File bai
        Boolean disable_baq
        String prefix
        File ref_fasta
    }

    String baq_option = if disable_baq then '-B' else '-E'

    command <<<
        set -eux

        samtools mpileup \
            ~{baq_option} \
            -a \
            -s \
            -q 1 \
            -f ~{ref_fasta} \
            -o ~{prefix}.mpileup \
            ~{bam}
    >>>

    output {
        File pileup = "~{prefix}.mpileup"
    }

    runtime {
        cpu:            1
        memory:         "4 GiB"
        disks:          "local-disk 100 HDD"
        bootDiskSizeGb: 10
        preemptible:    2
        maxRetries:     1
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
    }
}
