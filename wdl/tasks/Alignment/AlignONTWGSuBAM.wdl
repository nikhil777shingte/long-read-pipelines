version 1.0

import "../Utility/Utils.wdl"
import "../Utility/GeneralUtils.wdl" as GU
import "../Utility/BAMutils.wdl" as BU

import "AlignReads.wdl" as AR

import "../QC/AlignedMetrics.wdl" as AM
import "../Visualization/NanoPlot.wdl" as NP

workflow AlignONTWGSuBAM {
    meta {
        desciption:
        "Given an unaligned ONT BAM for a sample, align and verify fingerprint."
    }

    input {
        File uBAM
        Array[String] uBAM_tags_to_preserve # please be careful with this: methylation tags MM, ML are usually what you want to keep, in addition to the RG tag.
        String bam_sample_name
        String flowcell

        File ref_map_file
    }

    parameter_meta {
        bam_sample_name:       "Name of the sample to use in the aligned BAM."
        uBAM_tags_to_preserve: "Please be careful with this: methylation tags MM, ML are usually what you want to keep at minimum, in addition to the RG tag."
        flowcell:              "Name of the flowcell."

        # outputs
        wgs_cov: "whole genome mean coverage"
        aln_summary: "summary on alignment metrics"
        alignment_metrics_tar_gz : "A tar.gz file holding the custom alignment metrics."
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    ###################################################################################
    # alignment

    # for transferring the read group line
    call BU.GetReadGroupLines { input: bam = uBAM }
    if (1!=length(GetReadGroupLines.read_group_ids)) {
        call Utils.StopWorkflow { input: reason = "This workflow only works for single read-group inputs." }
    }

    # if the intended sample id isn't the same as that in the input, change the read group line
    call BU.GetReadGroupInfo { input: uBAM = uBAM, keys = ['SM']}
    if (bam_sample_name != GetReadGroupInfo.read_group_info['SM']) {
        call OneOffReplaceSampleID { input:
            orig_rg_line = GetReadGroupLines.read_group_lines[0],
            old_sample_name = GetReadGroupInfo.read_group_info['SM'],
            new_sample_name = bam_sample_name
        }
    }

    String fix_this_rg_line = select_first([OneOffReplaceSampleID.fixed_rg_line, GetReadGroupLines.read_group_lines[0]])

    # some surgery on the RG line
    call OneOffHandleSpacesInRGLine { input: orig_rg_line = fix_this_rg_line }
    call OneOffHandleTabInRGLine { input: orig_rg_line = OneOffHandleSpacesInRGLine.fixed_rg_line } # this MUST be the last fix

    if (20 < ceil(size(uBAM, "GiB"))) { # 20 is purely emperical
        call BU.SplitNameSortedUbam {  # 288K reads is emperical, takes roughly 2 hours for minimap2 task
            input: uBAM = uBAM, n_reads = 288000
        }
        Int num_shards = length(SplitNameSortedUbam.split)
        scatter (i in range(num_shards)) {
            File shard_ubam = SplitNameSortedUbam.split[i]
            call AR.Minimap2 as MapShard {
                input:
                    reads = [shard_ubam],
                    ref_fasta = ref_map['fasta'],
                    map_preset = 'map-ont',

                    RG = OneOffHandleTabInRGLine.fixed_rg_line,

                    tags_to_preserve = uBAM_tags_to_preserve,

                    prefix = bam_sample_name + "." + flowcell + "." + "~{i}"
            }
        }
        call Utils.MergeBams { input: bams = MapShard.aligned_bam, prefix = bam_sample_name + "." + flowcell }
    }
    if (20 >= ceil(size(uBAM, "GiB"))) { # 20 is purely emperical
        call AR.Minimap2 {
        input:
            reads = [uBAM],
            ref_fasta = ref_map['fasta'],
            map_preset = 'map-ont',

            RG = OneOffHandleTabInRGLine.fixed_rg_line,

            tags_to_preserve = uBAM_tags_to_preserve,

            prefix = bam_sample_name + "." + flowcell
        }
    }
    File aBAM = select_first([Minimap2.aligned_bam, MergeBams.merged_bam])
    File aBAI = select_first([Minimap2.aligned_bai, MergeBams.merged_bai])

    ###################################################################################
    # alignment metrics collection
    call AM.MosDepthWGS { input: bam = aBAM, bai = aBAI }

    call NP.NanoPlotFromBam { input: bam = aBAM, bai = aBAI }
    call GU.TarGZFiles as PackAlnMetrics {
        input: files = NanoPlotFromBam.plots, name = "alignment.metrics"
    }

    output {
        File aligned_bam = aBAM
        File aligned_bai = aBAI

        Float wgs_cov = MosDepthWGS.wgs_cov
        Map[String, Float] aln_summary = NanoPlotFromBam.stats_map
        File alignment_metrics_tar_gz = PackAlnMetrics.you_got_it
    }
}

task OneOffReplaceSampleID {
    meta {
        desciption: "Fixing sample ID in a BAM's read group line (because what's desired in the final BAM and what's in the input aren't the same, and reheader is expensive.)"
    }
    input {
        String orig_rg_line
        String old_sample_name
        String new_sample_name
    }

    output {
        String fixed_rg_line = read_lines("result.txt")[0]
    }
    command <<<
        set -eux

        echo -e "~{orig_rg_line}" | \
            sed "s/SM:~{old_sample_name}/SM:~{new_sample_name}/g" \
        > "result.txt"
    >>>
    runtime {
        disks: "local-disk 10 HDD"
        docker: "gcr.io/cloud-marketplace/google/ubuntu2004:latest"
    }
}

task OneOffHandleTabInRGLine {
    meta {
        desciption: "RG input to minimap needs \\t, not just \t"
    }
    input {
        String orig_rg_line
    }

    output {
        String fixed_rg_line = read_lines("result.txt")[0]
    }
    command <<<
        set -eux

        echo -e "~{orig_rg_line}" | \
            sed 's#\t#\\t#g' \
        > "result.txt"
    >>>
    runtime {
        disks: "local-disk 10 HDD"
        docker: "gcr.io/cloud-marketplace/google/ubuntu2004:latest"
    }
}

task OneOffHandleSpacesInRGLine {
    meta {
        desciption: "RG input sometimes contrain spaces (in DS field), re-format as ';' here. Otherwise this causes troubles in downstream WDLs because spaces may be used as indicating new args to commands."
    }
    input {
        String orig_rg_line
    }

    output {
        String fixed_rg_line = read_lines("result.txt")[0]
    }
    command <<<
        set -eux

        # replace any sequence of one or more spaces with ";"
        SPACE=" "
        echo -e "~{orig_rg_line}" | \
            sed "s/${SPACE}${SPACE}*/;/g" \
        > "result.txt"
    >>>
    runtime {
        disks: "local-disk 10 HDD"
        docker: "gcr.io/cloud-marketplace/google/ubuntu2004:latest"
    }
}