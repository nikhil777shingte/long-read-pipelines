version 1.0

import "../../TechAgnostic/Utility/SplitBamByReadgroup.wdl" as Major

import "../../../tasks/Utility/Utils.wdl"
import "../../../tasks/Utility/BAMutils.wdl" as BU
import "../../../tasks/Utility/ONTUtils.wdl" as OU

workflow RemoveDuplicateFromMergedONTBamAndSplitByReadgroup {

    input {
        File  input_bam
        File? input_bai

        Boolean unmap_bam
        Boolean fix_bam_header

        String gcs_out_root_dir
    }

    output {
        Map[String, String]? runid_2_ont_basecall_model = GetBasecallModel.runid_2_model

        Map[String, String] rgid_2_bam = WORKHORSE.rgid_2_bam
        Map[String, String] rgid_2_PU  = WORKHORSE.rgid_2_PU
        Map[String, String]? rgid_2_ubam_emptyness = WORKHORSE.rgid_2_ubam_emptyness
        Boolean rgid_2_bam_are_aligned = WORKHORSE.rgid_2_bam_are_aligned

        String last_processing_date = WORKHORSE.last_processing_date
    }

    ##############################################################################################################################
    # input file validation and fixing
    call BU.GatherBamMetadata {
        input: bam = input_bam
    }
    if ('coordinate' != GatherBamMetadata.sort_order) {
        call Utils.StopWorkflow { input: reason = "Input bam isn't coordinate-sorted, but rather sorted by ~{GatherBamMetadata.sort_order}"  }
    }

    # reality of life--submitted files sometimes need fixings in their headers
    if (fix_bam_header) {
        call FixParticularBamHeaderIssue { input: bam = input_bam }
    }

    # de-duplicate, another reality of life
    call OU.DeduplicateONTAlignedBam as Dedup { input :
        aligned_bam = select_first([FixParticularBamHeaderIssue.fixed, input_bam]), aligned_bai = input_bai, same_name_as_input = true
    }

    File ok_input_bam = Dedup.corrected_bam
    File ok_input_bai = Dedup.corrected_bai

    # Picard check
    call BU.ValidateSamFile { input: bam = ok_input_bam }

    ##############################################################################################################################
    # delegate
    call Major.SplitBamByReadgroup as WORKHORSE {
        input:
            input_bam = ok_input_bam,
            input_bai = ok_input_bai,

            unmap_bam = unmap_bam,
            convert_to_fq = false,

            validate_input_bam = false,

            gcs_out_root_dir = gcs_out_root_dir,
            debug_mode = false
    }

    call OU.GetBasecallModel { input: bam = ok_input_bam }
}

task FixParticularBamHeaderIssue {
    meta {
        description: "Someone submitted to us BAMs with irregular headers. Fix that here."
    }
    input {
        File bam
    }
    output {
        File fixed = "~{prefix}.rg.fixed.bam"
    }

    Int disk_size = 100 + 2 * ceil(size(bam, 'GiB'))

    String prefix = basename(bam, '.bam')

    command <<<
        set -eux

        samtools view -H ~{bam} > original.header.txt
        cat original.header.txt

        # strip away the unwanted @RG line
        grep -vE "^@RG[[:space:]]SM:" original.header.txt > to.add.sample.name.header.txt
        diff original.header.txt to.add.sample.name.header.txt || true

        # add back the sample name to the correct @RG lines
        formatted_sample_name=$(grep -E "^@RG[[:space:]]SM:" original.header.txt | tr '\t' '\n' | grep "^SM:")
        TAB=$'\t'  # sed doesn't officially recoganize \t as tab
        for line_num in `grep -n "^@RG" to.add.sample.name.header.txt | awk -F ':' '{print $1}'`
        do
            echo "${line_num}"
            sed -i.bak "${line_num}s/$/""${TAB}""${formatted_sample_name}""/" to.add.sample.name.header.txt
        done
        cat to.add.sample.name.header.txt
        mv to.add.sample.name.header.txt fixed.header.txt

        samtools reheader fixed.header.txt ~{bam} > ~{prefix}.rg.fixed.bam
    >>>

    runtime {
        cpu:            1
        memory:         "4 GiB"
        disks:          "local-disk ~{disk_size} LOCAL"
        preemptible:    2
        maxRetries:     1
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.2"
    }
}