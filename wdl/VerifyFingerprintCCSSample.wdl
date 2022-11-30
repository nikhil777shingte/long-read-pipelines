version 1.0

import "tasks/utils/qc/FPCheckAoU.wdl" as FP

workflow VerifyFingerprintCCSSample {
    input {
        File aligned_bam
        File aligned_bai

        String fp_store
        String sample_id_at_store

        File ref_specific_haplotype_map

        Float lod_pass_threshold =  6.0
        Float lod_fail_threshold = -3.0
    }

    call FP.FPCheckAoU as core {
        input:
            aligned_bam = aligned_bam,
            aligned_bai = aligned_bai,
            fp_store = fp_store,
            sample_id_at_store = sample_id_at_store,
            ref_specific_haplotype_map = ref_specific_haplotype_map,
            lod_pass_threshold = lod_pass_threshold,
            lod_fail_threshold = lod_fail_threshold
    }

    Map[String, String] result = {"status": core.FP_status, "LOD": core.lod_expected_sample}

    output {
        Map[String, String] fingerprint_check = result
    }
}