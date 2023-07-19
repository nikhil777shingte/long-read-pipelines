version 1.0

workflow TestRequesterPay {
    input {
        File rp_bam_path
    }
    call test_task { input: rp_bam_path = rp_bam_path }
}

task test_task {
    input {
        File rp_bam_path
    }

    Int disk_size = 10 + 2*round(size(rp_bam_path, 'GB'))

    command <<<
        set -eu

        samtools view -c rp_bam_path
    >>>
    runtime {
        cpu: 2
        memory: "8 GiB"
        disks: "local-disk ~{disk_size} HDD"
        preemptible: 1
        docker: "gcr.io/cloud-marketplace/google/ubuntu2004:latest"
    }
}
