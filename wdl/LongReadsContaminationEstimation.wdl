version 1.0

import "tasks/Utils.wdl"

workflow LongReadsContaminationEstimation {
    meta {
        desciption:
        "Estimate the cross-individual contamination level of a GRCh38 bam."
    }

    input {
        File bam
        File bai
        File ref_map_file

        File gt_sites_bed
        Boolean is_hgdp_sites
        Boolean is_100k_sites

        Boolean disable_baq

        Array[String] random_gcp_zones = ["us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"]
    }

    parameter_meta {
        # input:
        gt_sites_bed:     "Bed file holding the genotyping sites."
        is_hgdp_sites:    "Provided BED is HGDP genotyping sites."
        is_100k_sites:    "Provided BED is 100k genotyping sites, not 10k sites."
        disable_baq:      "If turned on, BAQ computation will be disabled (faster operation)."
        random_gcp_zones: "An parameter for rush processing: if provided, compute will happen in the requested zones"
    }

    # quickly change to pileup
    Map[String, String] ref_map = read_map(ref_map_file)
    call CollapseStrings {input: whatever = random_gcp_zones}
    String collapsed_zones = CollapseStrings.collapsed

    Int scaleup_factor = 20
    call Utils.ComputeAllowedLocalSSD {input: intended_gb = floor(scaleup_factor * size(bam, "GB")) + 1}
    call BamToRelevantPileup as Pileup {
        input:
            bam = bam,
            bai = bai,
            bed = gt_sites_bed,
            ref_fasta = ref_map['fasta'],
            disable_baq = disable_baq,
            local_ssd_cnt = ComputeAllowedLocalSSD.numb_of_local_ssd,
            zones = collapsed_zones
    }

    call VerifyBamID {
        input: pileup = Pileup.pileups, ref_fasta = ref_map['fasta'], is_hgdp_sites = is_hgdp_sites, is_100k_sites = is_100k_sites, zones = collapsed_zones
    }

    output {
        Float contamination_est = VerifyBamID.contamination_est
    }
}

task CollapseStrings {
    input {
        Array[String] whatever
    }

    command <<<
        echo ~{sep=' ' whatever}
    >>>

    output {
        String collapsed = read_string(stdout())
    }

    runtime {
        disks: "local-disk 50 HDD"
        docker: "gcr.io/cloud-marketplace/google/ubuntu2004:latest"
    }
}

task BamToRelevantPileup {
    meta {
        desciption:
        "Chop up a GRCh38 BAM by chromosome and further subset into requested genotyping sites; then convert to pileup format"
    }
    input {
        File bam
        File bai
        File bed
        File ref_fasta
        Boolean disable_baq
        Int local_ssd_cnt
        String zones
    }

    String baq_option = if disable_baq then '-B' else '-E'

    String result_dir = "scattered"

    Int ssd_sz = local_ssd_cnt * 375

    Int cores = 24

    command <<<
        set -eux

        set +e
        for i in `seq 1 22`;
        do
            grep -w "chr${i}" ~{bed} > "chr${i}.bed";
        done
        grep -w "chrX" ~{bed} > "chrX.bed"
        grep -w "chrY" ~{bed} > "chrY.bed"
        set -e
        rm ~{bed}
        touch ~{bai}
        cnt=0
        for bed in `ls chr*.bed | sort -V`; do
            if [[ ! -s ${bed} ]] ; then rm ${bed} && continue; fi
            prefix=$(echo "${bed}" | awk -F '.' '{print $1}')
            samtools view -h \
                --region-file ${bed} \
                --write-index \
                -o "${prefix}.bam##idx##${prefix}.bam.bai" \
                ~{bam} && \
            samtools mpileup \
                ~{baq_option} \
                -s \
                -q 1 \
                -f ~{ref_fasta} \
                -o ${prefix}.mpileup \
                "${prefix}.bam" &
            cnt=$((cnt + 1))
            if [[ $cnt -eq ~{cores} ]]; then wait; cnt=0; fi
        done
        wait

        rm -f chr*bam chr*bai
        cat *.mpileup > pileup.mpileup
    >>>

    output {
        File pileups = "pileup.mpileup"
    }

    runtime {
        cpu:            "~{cores}"
        memory:         "56 GiB"
        disks:          "local-disk ~{ssd_sz} LOCAL"
        preemptible:    1
        maxRetries:     1
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
        zones: zones
    }
}

task VerifyBamID {
    meta {
        desciption: "Uses VerifyBamID2 for human cross-individual contamination estimation. Assumes GRCh38."
    }

    input {
        File pileup
        File ref_fasta
        Boolean is_hgdp_sites
        Boolean is_100k_sites
        String zones
    }

    String a = if is_hgdp_sites then 'hgdp' else '1000g.phase3'
    String b = if is_100k_sites then '100k' else  '10k'
    String resource_prefix = '~{a}.~{b}.b38.vcf.gz.dat'

    command <<<
        set -eux

        export VERIFY_BAM_ID_HOME='/VerifyBamID'

        time \
        ${VERIFY_BAM_ID_HOME}/bin/VerifyBamID \
            --SVDPrefix ${VERIFY_BAM_ID_HOME}/resource/~{resource_prefix} \
            --Reference ~{ref_fasta} \
            --PileupFile ~{pileup} \
            --NumThread 4 \
        > vbid2.out \
        2> vbid2.log

        cat vbid2.out
        tail -1 vbid2.out | awk -F ':' '{print $2}' | awk '{$1=$1};1' > "est_contam.txt"
    >>>

    output {
        File vbid2_log = "vbid2.log"
        File vbid2_out = "vbid2.out"
        Float contamination_est = read_float("est_contam.txt")
    }

    runtime {
        cpu: 4
        memory: "16 GiB"
        disks: "local-disk 375 LOCAL"
        docker: "us.gcr.io/broad-dsp-lrma/verifybamid2:v2.0.1"
        zones: zones
    }
}
