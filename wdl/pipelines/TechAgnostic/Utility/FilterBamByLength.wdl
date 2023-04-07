version 1.0

import "../../../tasks/Utility/BAMutils.wdl" as BU
import "../../../tasks/Utility/Finalize.wdl" as FF

workflow FilterBamByLength {
    meta {
        desciption:
        "Filter a BAM (mapped or not) by sequence length."
    }
    parameter_meta {
        len_threshold_inclusive: "Reads longer than or equal to this length will be included."
    }
    input {
        File bam
        File? bai
        Int len_threshold_inclusive
        String gcs_out_root_dir
    }

    String workflow_name = "FilterBamByLength"

    call BU.InferSampleName { input: bam = bam, bai = bai }
    String outdir = sub(gcs_out_root_dir, "/$", "") + "/~{workflow_name}/~{InferSampleName.sample_name}"

    call BU.FilterBamByLen { input: bam = bam, bai = bai, len_threshold_inclusive = len_threshold_inclusive }
    call FF.FinalizeToFile as FinalizeBam { input: outdir = outdir, file = FilterBamByLen.fBAM }
    if (defined(bai)) {
        call FF.FinalizeToFile as FinalizeBai { input: outdir = outdir, file = select_first([FilterBamByLen.fBAI]) }
    }

    output {
        File  filtered_bam = FinalizeBam.gcs_path
        File? filtered_bai = FinalizeBai.gcs_path
    }
}
