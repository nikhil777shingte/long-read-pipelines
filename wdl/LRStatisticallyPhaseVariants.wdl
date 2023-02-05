version 1.0

#######################################################################
## A workflow that performs local ancestry inference in long read data.
#######################################################################

import "tasks/IntervalUtils.wdl"
import "tasks/VariantUtils.wdl"
import "tasks/SHAPEIT5.wdl"
import "tasks/Finalize.wdl" as FF

workflow LRStatisticallyPhaseVariants {
    input {
        File gvcf
        File tbi
        File ref_map_file

        String prefix

        String gcs_out_root_dir
    }

    parameter_meta {
        gvcf:             "GCS path to gVCF file"
        tbi:              "GCS path to gVCF tbi file"
        ref_map_file:     "table indicating reference sequence and auxillary file locations"
        prefix:           "prefix for output joint-called gVCF and tabix index"
        gcs_out_root_dir: "GCS bucket to store the reads, variants, and metrics files"
    }

    String outdir = sub(gcs_out_root_dir, "/$", "") + "/LRStatisticallyPhaseVariants/~{prefix}"

    Map[String, String] ref_map = read_map(ref_map_file)

    call IntervalUtils.GetContigNames { input: ref_dict = ref_map['dict'] }

    scatter (contig_name in GetContigNames.contig_names) {
        call IntervalUtils.GenerateIntervals as GenerateCommonVariantIntervals {
            input:
                ref_dict        = ref_map['dict'],
                selected_contig = contig_name,
                chunk_bp        = 40000000,
                stride_bp       = 35000000,
                buffer_bp       = 0,
        }

        call VariantUtils.SubsetVCF { input: vcf_gz = gvcf, vcf_tbi = tbi, locus = contig_name }

        call VariantUtils.FillTags { input: vcf = SubsetVCF.subset_vcf, tags = [ 'AC', 'AN' ] }

        scatter (interval in GenerateCommonVariantIntervals.intervals) {
            call VariantUtils.CountVariants {
                input:
                    vcf = FillTags.filled_vcf,
                    tbi = FillTags.filled_tbi,
                    locus = interval
            }

            if (CountVariants.num_variants > 0) {
                call SHAPEIT5.PhaseCommonVariants {
                    input:
                        input_vcf  = FillTags.filled_vcf,
                        input_tbi  = FillTags.filled_tbi,
                        filter_maf = 0.005,
                        interval   = interval,
                }
            }
        }

        if (length(select_all(PhaseCommonVariants.common_phased_shard_bcf)) > 0) {
            call SHAPEIT5.LigatePhasedCommonVariants {
                input:
                    phased_shard_bcfs = select_all(PhaseCommonVariants.common_phased_shard_bcf),
                    phased_shard_csis = select_all(PhaseCommonVariants.common_phased_shard_csi),
                    prefix = "out",
            }

            call IntervalUtils.GenerateIntervals as GenerateRareVariantIntervals {
                input:
                    ref_dict        = ref_map['dict'],
                    selected_contig = contig_name,
                    chunk_bp        = 40000000,
                    stride_bp       = 35000000,
                    buffer_bp       = 17500000,
            }

            scatter (p in zip(GenerateRareVariantIntervals.intervals, GenerateCommonVariantIntervals.intervals)) {
                call SHAPEIT5.PhaseRareVariants {
                    input:
                        input_vcf             = FillTags.filled_vcf,
                        input_tbi             = FillTags.filled_tbi,
                        scaffold_bcf          = LigatePhasedCommonVariants.scaffold_bcf,
                        scaffold_csi          = LigatePhasedCommonVariants.scaffold_csi,
                        rare_variant_region   = p.left,
                        common_variant_region = p.right,
                }
            }

            call SHAPEIT5.ConcatenateVariants as ConcatenateContigVariants {
                input:
                    shard_bcfs = PhaseRareVariants.rare_phased_shard_bcf,
                    shard_csis = PhaseRareVariants.rare_phased_shard_csi,
                    prefix = contig_name
            }
        }
    }

    call SHAPEIT5.ConcatenateVariants as ConcatenateAllVariants {
        input:
            shard_bcfs = select_all(ConcatenateContigVariants.full_bcf),
            shard_csis = select_all(ConcatenateContigVariants.full_csi),
            prefix = prefix
    }

    # Finalize
    call FF.FinalizeToFile as FinalizeBCF { input: outdir = outdir, file = ConcatenateAllVariants.full_bcf }
    call FF.FinalizeToFile as FinalizeCSI { input: outdir = outdir, file = ConcatenateAllVariants.full_csi }

    ##########
    # store the results into designated bucket
    ##########

    output {
        File phased_bcf = FinalizeBCF.gcs_path
        File phased_csi = FinalizeCSI.gcs_path
    }
}
