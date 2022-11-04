version 1.0

######################################################################################
## A pipeline for running the clipper script
######################################################################################

import "tasks/Structs.wdl"

workflow RunClipper {
    input {
        File aligned_bam
        String prefix
        Int? min_dist
        Int? min_cluster_count
        Int? max_unique_breakends
    }

    parameter_meta {
        aligned_bam:            "aligned bam"
        prefix:                 "e.g. sample name"
        min_dist:               "Minimum distance between where read splits align on the reference (bp) (default=1)"
        min_cluster_count:      "Minimum number of reads in a cluster (default=5)"
        max_unique_breakends:   "Maximum number of unique breakends for a cluster (default=10)"
    }

    call ClipperCluster { input: aligned_bam = aligned_bam, prefix=prefix }

    call ClipperProcess {
      input:
        clusterfile = ClipperCluster.clusterfile,
        prefix = prefix,
        min_dist = select_first([min_dist, 1]),
        min_cluster_count = select_first([min_cluster_count, 5]),
        max_unique_breakends = select_first([max_unique_breakends, 10])
    }

    output {
        File clipped_vcf = ClipperProcess.clustervcf
    }
}

task ClipperCluster {
    input {
        File aligned_bam
        String prefix
        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 2*ceil(size(aligned_bam, "GB"))+20

    command <<<
        set -euxo pipefail

        python split_reads.py ~{aligned_bam} > ~{prefix}_clipped_reads.bed
        awk '$9~"chr"' ~{prefix}_clipped_reads.bed | sort -k1,1 -k2,2n > ~{prefix}_clipped_reads_filtered.bed
        bedtools cluster -d 5 -i ~{prefix}_clipped_reads_filtered.bed > ~{prefix}_clipped_reads_filtered_clustered.bed
    >>>

    output {
        File clusterfile = "~{prefix}_clipped_reads_filtered_clustered.bed"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          8,
        mem_gb:             32,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  3,
        max_retries:        0,
        docker:             "quay.io/ymostovoy/clipper:1.0"
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

task ClipperProcess {
    input {
        File clusterfile
        String prefix
        Int min_dist
        Int min_cluster_count
        Int max_unique_breakends
        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = ceil(size(clusterfile, "GB"))+50

    command <<<
        python split_reads_process_clusters.py ~{clusterfile} -d ~{min_dist} -c ~{min_cluster_count} -u ~{max_unique_breakends} > ~{prefix}_clipped_reads_d~{min_dist}_c~{min_cluster_count}_u~{max_unique_breakends}.vcf
    >>>

    output { File clustervcf = "~{prefix}_clipped_reads_d~{min_dist}_c~{min_cluster_count}_u~{max_unique_breakends}.vcf" }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          4,
        mem_gb:             16,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  3,
        max_retries:        0,
        docker:             "quay.io/ymostovoy/clipper:1.0"
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
