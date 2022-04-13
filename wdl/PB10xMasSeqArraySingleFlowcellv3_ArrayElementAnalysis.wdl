version 1.0

import "tasks/PBUtils.wdl" as PB
import "tasks/Utils.wdl" as Utils
import "tasks/Finalize.wdl" as FF
import "tasks/AlignReads.wdl" as AR
import "tasks/Cartographer.wdl" as CART
import "tasks/TranscriptAnalysis/Flair_Tasks.wdl" as ISO
import "tasks/ReadsMetrics.wdl" as RM
import "tasks/AlignedMetrics.wdl" as AM
import "tasks/Ten_X_Tool.wdl" as TENX
import "tasks/JupyterNotebooks.wdl" as JUPYTER
import "tasks/Longbow.wdl" as LONGBOW

import "tasks/StringTie2.wdl"

import "tasks/TranscriptAnalysis/UMI_Tools.wdl" as UMI_TOOLS
import "tasks/TranscriptAnalysis/Postprocessing_Tasks.wdl" as TX_POST
import "tasks/TranscriptAnalysis/Preprocessing_Tasks.wdl" as TX_PRE

workflow PB10xMasSeqArraySingleFlowcellv3_ArrayElementAnalysis {

    meta {
        description : "This workflow is designed to process data from the MASSeq v2 protocol and produce aligned reads that are ready for downstream analysis (e.g. transcript isoform identification).  It takes in a raw PacBio run folder location on GCS and produces a folder containing the aligned reads and other processed data."
        author : "Jonn Smith"
        email : "jonn@broadinstitute.org"
    }

    input {
        String input_array_element_bam

        String gcs_out_root_dir = "gs://broad-dsde-methods-long-reads-outgoing/PB10xMasSeqArraySingleFlowcellv3_ArrayElementAnalysis"

        File cell_barcode_whitelist = "gs://broad-dsde-methods-long-reads/resources/MASseq_0.0.2/737K-august-2016.txt"

        # NOTE: Reference for un-split CCS reads:
        File ref_fasta =  "gs://broad-dsde-methods-long-reads/resources/references/grch38_noalt/GCA_000001405.15_GRCh38_no_alt_analysis_set.fa"
        File ref_fasta_index = "gs://broad-dsde-methods-long-reads/resources/references/grch38_noalt/GCA_000001405.15_GRCh38_no_alt_analysis_set.fa.fai"
        File ref_fasta_dict = "gs://broad-dsde-methods-long-reads/resources/references/grch38_noalt/GCA_000001405.15_GRCh38_no_alt_analysis_set.dict"

        # NOTE: Reference for array elements:
        File transcriptome_ref_fasta =  "gs://broad-dsde-methods-long-reads/resources/gencode_v37/gencode.v37.pc_transcripts.fa"
        File transcriptome_ref_fasta_index = "gs://broad-dsde-methods-long-reads/resources/gencode_v37/gencode.v37.pc_transcripts.fa.fai"
        File transcriptome_ref_fasta_dict = "gs://broad-dsde-methods-long-reads/resources/gencode_v37/gencode.v37.pc_transcripts.dict"

        File genome_annotation_gtf = "gs://broad-dsde-methods-long-reads/resources/gencode_v37/gencode.v37.primary_assembly.annotation.gtf"

        File jupyter_template_static = "gs://broad-dsde-methods-long-reads/resources/MASseq_0.0.2/MAS-seq_QC_report_template-static.ipynb"
        File workflow_dot_file = "gs://broad-dsde-methods-long-reads/resources/MASseq_0.0.2/PB10xMasSeqArraySingleFlowcellv2.dot"

        File intervals_of_interest = "gs://broad-dsde-methods-long-reads/resources/MASseq_0.0.2/gencode.37.TCR_intervals.tsv"
        String interval_overlap_name = "is_tcr_overlapping"

        File short_read_umis_tsv

        String starcode_extra_params = "--dist 2 --sphere"

        String expanded_cbc_tag = "CR"

        File? cell_barcode_freq_tsv

        # Default here is 0 because ccs uncorrected reads all seem to have RQ = -1.
        # All pathologically long reads also have RQ = -1.
        # This way we preserve the vast majority of the data, even if it has low quality.
        # We can filter it out at later steps.
        Float min_read_quality = 0.0
        Int max_reclamation_length = 60000

        Boolean is_SIRV_data = false
        String mas_seq_model = "mas15v2"

        Int ccs_lev_dist = 2
        Int clr_lev_dist = 3

        Int ccs_umi_padding = 2
        Int clr_umi_padding = 2

        Int ccs_cbc_padding = 3
        Int clr_cbc_padding = 3

        # Add a suffix here for our out directory so we can label runs:
        String out_dir_suffix = ""

        String sample_name
        String sample_id
    }

    parameter_meta {
        input_array_element_bam : "Input bam file containing pre-segmented MAS-ISO-seq / Longbow array elements."
        gcs_out_root_dir : "Root output GCS folder in which to place results of this workflow."

        cell_barcode_whitelist : "Text file containing a whitelist of cell barcodes for the single-cell library prep."

        ref_fasta : "FASTA file containing the reference sequence to which the input data should be aligned before splitting into array elements."
        ref_fasta_index : "FASTA index file for the given ref_fasta file."
        ref_fasta_dict : "Sequence dictionary file for the given ref_fasta file."

        transcriptome_ref_fasta : "FASTA file containing the reference sequence to which the array elements should be aligned."
        transcriptome_ref_fasta_index : "FASTA index file for the given transcriptome_ref_fasta file."
        transcriptome_ref_fasta_dict : "Sequence dictionary file for the given transcriptome_ref_fasta file."

        genome_annotation_gtf : "Gencode GTF file containing genome annotations for the organism under study (usually humans).  This must match the given reference version and transcriptiome reference (usually hg38)."

        jupyter_template_static : "Jupyter notebook / ipynb file containing a template for the QC report which will contain static plots.  This should contain the same information as the jupyter_template_interactive file, but with static images."
        workflow_dot_file : "DOT file containing the representation of this WDL to be included in the QC reports.  This can be generated with womtool."

        intervals_of_interest : "[optional] An interval list file containing intervals to mark in the final anndata object as overlapping the transcripts.  Defaults to a T-cell receptor interval list."
        interval_overlap_name : "[optional] The name of the annotation to add to the final anndata object for the column containing the overlap flag for transcripts that overlap intervals in the given intervals_of_interest file.  Default: is_tcr_overlapping"

        min_read_quality : "[optional] Minimum read quality for reads to have to be included in our data (Default: 0.0)."
        max_reclamation_length : "[optional] Maximum length (in bases) that a read can be to attempt to reclaim from CCS rejection (Default: 60000)."

        is_SIRV_data : "[optional] true if and only if the data in this sample are from the SIRV library prep.  false otherwise (Default: false)"
        mas_seq_model : "[optional] built-in mas-seq model to use (Default: mas15)"

        sample_name : "[optional] The name of the sample to associate with the data in this workflow."
    }

    # Create some runtime attributes that will force google to do network transfers really fast:
    RuntimeAttr fast_network_attrs = object {
        cpu_cores:  4,
        mem_gb:     32,
        disk_type:  "LOCAL",
        preemptible_tries:  0
    }
    RuntimeAttr fast_network_attrs_preemptible = object {
        cpu_cores:  4,
        mem_gb:     32,
        disk_type:  "LOCAL",
        preemptible_tries:  1
    }

    ## No more preemption on this sharding - takes too long otherwise.
    RuntimeAttr disable_preemption_runtime_attrs = object {
        preemptible_tries: 0
    }

    # Call our timestamp so we can store outputs without clobbering previous runs:
    call Utils.GetCurrentTimestampString as t_01_WdlExecutionStartTimestamp { input: }

    String outdir = sub(gcs_out_root_dir, "/$", "")

    String SM  = sample_name
    String ID  = sample_id
    String DIR = SM + "." + ID

    String fbmrq_prefix = basename(input_array_element_bam, ".bam")

    ###############

    # Now align the array elements with their respective alignment presets.
    # NOTE: We use the non-truncated reads because we only want the good stuff.

    # 1 - filter the reads by the minimum read quality:
    call Utils.Bamtools as t_02_GetCcsReads {
        input:
            bamfile = input_array_element_bam,
            prefix = fbmrq_prefix + "_ccs_reads",
            cmd = "filter",
            args = '-tag "rq":">=' + min_read_quality + '"',
            runtime_attr_override = disable_preemption_runtime_attrs
    }

    call Utils.Bamtools as t_03_GetClrReads {
        input:
            bamfile = input_array_element_bam,
            prefix = fbmrq_prefix + "_rejected_reads",
            cmd = "filter",
            args = '-tag "rq":"<' + min_read_quality + '"',
            runtime_attr_override = disable_preemption_runtime_attrs
    }

    # Align CCS reads to the genome:
    call AR.Minimap2 as t_04_AlignCCSArrayElementsToGenome {
        input:
            reads      = [ t_02_GetCcsReads.bam_out ],
            ref_fasta  = ref_fasta,
            map_preset = "splice:hq"
    }

    # Align Reclaimed reads to the genome:
    call AR.Minimap2 as t_05_AlignReclaimedArrayElementsToGenome {
        input:
            reads      = [ t_03_GetClrReads.bam_out ],
            ref_fasta  = ref_fasta,
            map_preset = "splice"
    }

    ##############

    # Now restore the tags to the aligned bam files:
    call TENX.RestoreAnnotationstoAlignedBam as t_06_RestoreAnnotationsToGenomeAlignedCCSBam {
        input:
            annotated_bam_file = t_02_GetCcsReads.bam_out,
            aligned_bam_file = t_04_AlignCCSArrayElementsToGenome.aligned_bam,
            tags_to_ignore = [],
            mem_gb = 8,
    }

    call TENX.RestoreAnnotationstoAlignedBam as t_07_RestoreAnnotationsToGenomeAlignedReclaimedBam {
        input:
            annotated_bam_file = t_03_GetClrReads.bam_out,
            aligned_bam_file = t_05_AlignReclaimedArrayElementsToGenome.aligned_bam,
            tags_to_ignore = [],
            mem_gb = 8,
    }

    # Merge Aligned CCS array elements together:
    call Utils.MergeBams as t_08_MergeAlignedCCSArrayElements {
        input:
            bams = [t_06_RestoreAnnotationsToGenomeAlignedCCSBam.output_bam],
            prefix = SM + "_ccs_array_elements_aligned"
    }

    # Merge Aligned CCS Reclaimed array elements together:
    call Utils.MergeBams as t_09_MergeAlignedCCSReclaimedArrayElements {
        input:
            bams = [t_07_RestoreAnnotationsToGenomeAlignedReclaimedBam.output_bam],
            prefix = SM + "_ccs_reclaimed_array_elements_aligned"
    }

    call Utils.MergeBams as t_10_MergeAllAlignedArrayElementsNonTruncated {
        input:
            bams = [t_06_RestoreAnnotationsToGenomeAlignedCCSBam.output_bam, t_07_RestoreAnnotationsToGenomeAlignedReclaimedBam.output_bam],
            prefix = SM + "_array_elements_non_truncated_aligned"
    }

    ##########################################################################################################################
    #############################################################
    #
    # FILTER THE READS HERE!!!!!!!!!!!!!!!!111!1!11
    #
    ############################################################
    ##########################################################################################################################

#    	• Post-Alignment filters:
#		○ Remove Flags:
#			§ MQ0
#			§ Supplementary
#			§ Secondary
#			§ Unmapped
#		○ Read length
#			§ Keep: <15Kb
#		○ Spread on the reference
#			§ Some fraciton of ref length
#			§ Or max N (splice gap)
#			§ Overlapping multiple genes - remove it.
#				□ Use funcotate segments or similar
#				□ Whitelist genes with overlapping exons
#				□ Bedtools intersection
#					® THOUGH, occasionally it gives different results
#					® Might not be a problem.
#		○ End soft/hard Clipping
#			§ L or R end of read
#               1000?

    RuntimeAttr filterReadsAttrs = object {
        preemptible_tries: 0
    }

    # Remove unmapped, secondary, supplementary, mq0, length > 15kb, end clips > 1kb
    call Utils.FilterMasSeqReadsWithGatk as t_11_AlignmentFilterForCcsArrayElements {
        input:
            bam_file = t_08_MergeAlignedCCSArrayElements.merged_bam,
            bam_index = t_08_MergeAlignedCCSArrayElements.merged_bai,
            prefix = SM + "_CCS_ArrayElements_Annotated_Aligned_PrimaryOnly",
            runtime_attr_override = filterReadsAttrs
    }

    call Utils.FilterMasSeqReadsWithGatk as t_12_AlignmentFilterForReclaimedArrayElements {
        input:
            bam_file = t_09_MergeAlignedCCSReclaimedArrayElements.merged_bam,
            bam_index = t_09_MergeAlignedCCSReclaimedArrayElements.merged_bai,
            prefix = SM + "_Reclaimed_ArrayElements_Annotated_Aligned_PrimaryOnly",
            runtime_attr_override = filterReadsAttrs
    }

    #########################################################################################################################
    ##########################################################################################################################

    # Mehrtash's suggestion for paper
    # 		• run stringtie2
    #		• filter stringtie2 annotations, get rid of super low TPM annotations
    #		polish stringtie2 annotations (e.g. if a transcript is an extension of a GENCODE transcript, propagate the name, if a “novel” stringtie2 gene overlaps with a previously annotated gene > 95%, propagate the name; otherwise, ignore)
    #		run TALON w/ polished stringtie2 annotations
    #       ignore NIC and NNC TALON transcripts (which should be VERY few), only focus on exact matches and ISM

    # Mehrtash's latest suggestion for paper:
    # Running StringTie2 on our sample w/ GENCODE as reference
    # Running TALON on GENCODE "reads" (we need to cook up a bam file from the GENCODE gtf) with a database initialized with the StringTie2 gtf
    # Running TALON on our sample with a darabase initialized with the StringTie2 gtf

    # Merge all alignments together:
    call Utils.MergeBams as t_13_MergeAllAlignedAndFilteredArrayElements {
        input:
            bams = [t_11_AlignmentFilterForCcsArrayElements.bam, t_12_AlignmentFilterForReclaimedArrayElements.bam],
            prefix = SM + "_all_array_elements_aligned_for_txome_discovery"
    }

    call StringTie2.Quantify as t_14_ST2_Quant {
        input:
            aligned_bam = t_13_MergeAllAlignedAndFilteredArrayElements.merged_bam,
            aligned_bai = t_13_MergeAllAlignedAndFilteredArrayElements.merged_bai,
            gtf = genome_annotation_gtf,
            keep_retained_introns = false,
            prefix = SM + "_StringTie2_Quantify",
    }

    call StringTie2.ExtractTranscriptSequences as t_15_ST2_ExtractTranscriptSequences  {
        input:
            ref_fasta = ref_fasta,
            ref_fasta_fai = ref_fasta_index,
            gtf = t_14_ST2_Quant.st_gtf,
            prefix = SM + "_StringTie2_ExtractTranscriptSequences",
    }

    call StringTie2.CompareTranscriptomes as t_16_ST2_CompareTranscriptomes {
        input:
            guide_gtf = genome_annotation_gtf,
            new_gtf = t_14_ST2_Quant.st_gtf,
            prefix = SM + "_StringTie2_CompareTranscriptome",
    }

    ##########################################################################################################################
    ##########################################################################################################################

    # Now we pad our barcodes and correct them:
    Int num_array_element_shards = 50

    # CCS
    call Utils.ShardReads as t_17_ShardS2ECcsArrayElements {
        input:
            bam = t_11_AlignmentFilterForCcsArrayElements.bam,
            bam_index = t_11_AlignmentFilterForCcsArrayElements.bai,
            prefix = SM + "_ccs_array_elements_subshard",
            num_shards = num_array_element_shards,
    }
    scatter (ccsi in range(length(t_17_ShardS2ECcsArrayElements.shards))) {
        File ccs_array_element_shard = t_17_ShardS2ECcsArrayElements.shards[ccsi]

        call Utils.IndexBam as t_18_IndexCcsArrayElementShard {
            input:
                bam = ccs_array_element_shard
        }

        # Now that we've annotated the reads, we can pad the UMIs by a couple of bases to aid in the deduping:
        call LONGBOW.Pad as t_19_LongbowPadCCSArrayElementUMIs {
            input:
                reads = ccs_array_element_shard,
                model = mas_seq_model,
                tag_to_expand = "ZU",
                padding = ccs_umi_padding,
                prefix = SM + "_ccs_array_elements_aligned_annotated_umi_padded_shard_" + ccsi
        }

        call LONGBOW.Pad as t_20_LongbowPadCCSArrayElementCBCs {
            input:
                reads = t_19_LongbowPadCCSArrayElementUMIs.padded_tag_bam,
                model = mas_seq_model,
                tag_to_expand = "CR",
                new_tag_dest = expanded_cbc_tag,
                padding = ccs_cbc_padding,
                prefix = SM + "_ccs_array_elements_aligned_annotated_cbc_padded_shard_" + ccsi
        }

        # Now we should correct our barcodes based on the whitelist:
        call LONGBOW.Correct as t_21_LongbowCorrectCCSCorrectedArrayElementCBCs {
            input:
                reads = t_20_LongbowPadCCSArrayElementCBCs.padded_tag_bam,
                barcode_allow_list = cell_barcode_whitelist,
                barcode_freq_list = cell_barcode_freq_tsv,
                model = mas_seq_model,
                ccs_lev_dist_threshold = ccs_lev_dist,
                clr_lev_dist_threshold = clr_lev_dist,
                prefix = SM + "_ccs_array_elements_aligned_annotated_padded_cbc_corrected_shard_" + ccsi,
                raw_barcode_tag = expanded_cbc_tag,
                corrected_barcode_tag = "CB",
        }

        call TENX.AdjustUmiSequenceWithAdapterAlignment as t_22_AdjustCCSUMIs {
            input:
                bam = t_21_LongbowCorrectCCSCorrectedArrayElementCBCs.corrected_barcodes_bam,
                short_read_umis = short_read_umis_tsv,
                prefix = SM + "_ccs_array_elements_aligned_annotated_padded_cbc_corrected_UMI_adjusted_shard_" + ccsi,
        }
    }
    # Merge Aligned CCS reads together:
    call Utils.MergeBams as t_23_MergeLongbowPaddedCBCCorrectedCCSArrayElements {
        input:
            bams = t_22_AdjustCCSUMIs.output_bam,
            prefix = SM + "_ccs_array_elements_aligned_annotated_padded_CBC_corrected"
    }
    call Utils.MergeBams as t_24_MergeLongbowPaddedCBCUncorrectableCCSArrayElements {
        input:
            bams = t_21_LongbowCorrectCCSCorrectedArrayElementCBCs.uncorrected_barcodes_bam,
            prefix = SM + "_ccs_array_elements_aligned_annotated_padded_CBC_uncorrectable"
    }

    # RECLAIMED
    call Utils.ShardReads as t_25_ShardS2ECcsReclaimedArrayElements {
        input:
            bam = t_12_AlignmentFilterForReclaimedArrayElements.bam,
            bam_index = t_12_AlignmentFilterForReclaimedArrayElements.bai,
            prefix = SM + "_ccs_reclaimed_array_elements_subshard",
            num_shards = num_array_element_shards,
    }
    scatter (cri in range(length(t_25_ShardS2ECcsReclaimedArrayElements.shards))) {
        File ccs_reclaimed_array_element_shard = t_25_ShardS2ECcsReclaimedArrayElements.shards[cri]

        call Utils.IndexBam as t_26_IndexCcsReclaimedArrayElementShard {
            input:
                bam = ccs_reclaimed_array_element_shard
        }

        # Now that we've annotated the reads, we can pad the CBCs and UMIs by a couple of bases to aid in the deduping:
        call LONGBOW.Pad as t_27_LongbowPadCCSReclaimedArrayElementUMIs {
            input:
                reads = ccs_reclaimed_array_element_shard,
                model = mas_seq_model,
                tag_to_expand = "ZU",
                padding = clr_umi_padding,
                prefix = SM + "_ccs_reclaimed_array_elements_aligned_annotated_umi_padded_shard_" + cri
        }

        call LONGBOW.Pad as t_28_LongbowPadCCSReclaimedArrayElementCBCs {
            input:
                reads = t_27_LongbowPadCCSReclaimedArrayElementUMIs.padded_tag_bam,
                model = mas_seq_model,
                tag_to_expand = "CR",
                new_tag_dest = expanded_cbc_tag,
                padding = clr_cbc_padding,
                prefix = SM + "_ccs_reclaimed_array_elements_aligned_annotated_cbc_padded_shard_" + cri
        }

        # Now we should correct our barcodes based on the whitelist:
        call LONGBOW.Correct as t_29_LongbowCorrectCCSReclaimedArrayElementCBCs {
            input:
                reads = t_28_LongbowPadCCSReclaimedArrayElementCBCs.padded_tag_bam,
                barcode_allow_list = cell_barcode_whitelist,
                barcode_freq_list = cell_barcode_freq_tsv,
                model = mas_seq_model,
                ccs_lev_dist_threshold = ccs_lev_dist,
                clr_lev_dist_threshold = clr_lev_dist,
                prefix = SM + "_ccs_reclaimed_array_elements_aligned_annotated_padded_cbc_corrected_shard_" + cri,
                raw_barcode_tag = expanded_cbc_tag,
                corrected_barcode_tag = "CB",
        }

        call TENX.AdjustUmiSequenceWithAdapterAlignment as t_30_AdjustCCSReclaimedUMIs {
            input:
                bam = t_29_LongbowCorrectCCSReclaimedArrayElementCBCs.corrected_barcodes_bam,
                short_read_umis = short_read_umis_tsv,
                prefix = SM + "_ccs_reclaimed_array_elements_aligned_annotated_padded_cbc_corrected_UMI_adjusted_shard_" + cri,
        }
    }
    # Merge Aligned CCS Reclaimed reads together:
    call Utils.MergeBams as t_31_MergeLongbowPaddedCBCCorrectedCCSReclaimedArrayElements {
        input:
            bams = t_30_AdjustCCSReclaimedUMIs.output_bam,
            prefix = SM + "_ccs_reclaimed_array_elements_aligned_annotated_padded_CBC_corrected"
    }
    call Utils.MergeBams as t_32_MergeLongbowPaddedCBCUncorrectableCCSReclaimedArrayElements {
        input:
            bams = t_29_LongbowCorrectCCSReclaimedArrayElementCBCs.uncorrected_barcodes_bam,
            prefix = SM + "_ccs_array_elements_aligned_annotated_padded_CBC_uncorrectable"
    }

    #################
    # Here we restore the original read names to the bam because we're hashing them with Longbow.segment:

    # Restore original read names to CCS reads:
    call TX_PRE.RestoreOriginalReadNames as t_33_RestoreCcsOriginalReadNames {
        input:
            bam = t_23_MergeLongbowPaddedCBCCorrectedCCSArrayElements.merged_bam,
            prefix =  SM + "_CCS_cbc_annotated_array_elements_padded_original_names"
    }

    # Restore original read names to CLR reads:
    call TX_PRE.RestoreOriginalReadNames as t_34_RestoreClrOriginalReadNames {
        input:
            bam = t_31_MergeLongbowPaddedCBCCorrectedCCSReclaimedArrayElements.merged_bam,
            prefix =  SM + "_CLR_cbc_annotated_array_elements_padded_original_names"
    }

    # Merge Aligned CCS and Reclaimed reads together:
    call Utils.MergeBams as t_35_MergeAllAnnotatedArrayElementsWithOriginalNames {
        input:
            bams = [t_33_RestoreCcsOriginalReadNames.bam_out, t_34_RestoreClrOriginalReadNames.bam_out],
            prefix = SM + "_all_cbc_annotated_array_elements_padded_original_names"
    }

    #################
    # Now we have to split the reads again, process them into gff files, run gffcompare and then aggregate the results in a graph

    # We can actually compare the references without needing to scatter:
    call TX_PRE.GffCompare as t_36_GffCompareStringtie2toGencode {
        input:
            gff_ref = t_14_ST2_Quant.st_gtf,
            gff_query = genome_annotation_gtf,
            ref_fasta = ref_fasta,
            ref_fasta_index = ref_fasta_index,
    }
    call TX_PRE.GffCompare as t_37_GffCompareGencodetoStringtie2 {
        input:
            gff_ref = genome_annotation_gtf,
            gff_query = t_14_ST2_Quant.st_gtf,
            ref_fasta = ref_fasta,
            ref_fasta_index = ref_fasta_index,
    }

    # Split by contig:
    call TX_PRE.SplitBamByContig as t_38_SplitArrayElementsByContig {
        input:
            bam = t_35_MergeAllAnnotatedArrayElementsWithOriginalNames.merged_bam,
            prefix = SM + "_all_cbc_annotated_array_elements_padded_original_names"
    }

    # For each contig:
    scatter (i in range(length(t_38_SplitArrayElementsByContig.contig_bams))) {

        File contig_bam = t_38_SplitArrayElementsByContig.contig_bams[i]
        String contig_name = t_38_SplitArrayElementsByContig.contig_names[i]

        # Create a GFF file:
        call TX_PRE.ConvertSplicedBamToGff as t_39_ConvertSplicedBamToGff {
            input:
                bam = contig_bam
        }

        # Compare GFF files:
        call TX_PRE.GffCompare as t_40_GffCompareStringtie2toMasSeqReads {
            input:
                gff_ref = t_14_ST2_Quant.st_gtf,
                gff_query = t_39_ConvertSplicedBamToGff.gff,
                ref_fasta = ref_fasta,
                ref_fasta_index = ref_fasta_index,
        }

        call TX_PRE.GffCompare as t_41_GffCompareGencodetoMasSeqReads {
            input:
                gff_ref = genome_annotation_gtf,
                gff_query = t_39_ConvertSplicedBamToGff.gff,
                ref_fasta = ref_fasta,
                ref_fasta_index = ref_fasta_index,
        }

        # Create the comparison graph and tsv files:
        call TX_POST.QuantifyGffComparison as t_42_QuantifyGffComparison {
            input:
                genome_gtf = genome_annotation_gtf,
                st2_gencode_refmap = t_36_GffCompareStringtie2toGencode.refmap,
                st2_gencode_tmap = t_36_GffCompareStringtie2toGencode.tmap,
                st2_read_refmap = t_40_GffCompareStringtie2toMasSeqReads.refmap,
                st2_read_tmap = t_40_GffCompareStringtie2toMasSeqReads.tmap,
                gencode_st2_refmap = t_37_GffCompareGencodetoStringtie2.refmap,
                gencode_st2_tmap = t_37_GffCompareGencodetoStringtie2.tmap,
                gencode_read_refmap = t_41_GffCompareGencodetoMasSeqReads.refmap,
                gencode_read_tmap = t_41_GffCompareGencodetoMasSeqReads.tmap,
                prefix = SM + "_all_cbc_annotated_array_elements_padded_" + contig_name
        }
    }

    # Merge our tx equivalance classes assignments and eq classes:
    call TX_POST.CombineEqClassFiles as t_43_CombineEqClassFiles {
        input:
            gene_eq_class_definitions = t_42_QuantifyGffComparison.gene_eq_class_labels_file,
            gene_assignment_files = t_42_QuantifyGffComparison.gene_assignments_file,
            equivalence_class_definitions = t_42_QuantifyGffComparison.tx_equivalence_class_labels_file,
            equivalence_classes = t_42_QuantifyGffComparison.tx_equivalence_class_file,
            prefix = SM + "_all_cbc_annotated_array_elements_padded"
    }

    ############################################################
    # Quantify Transcripts:
    ##########

    # Use old quant method here as a baseline for comparison:
    call TX_POST.CopyEqClassInfoToTag as t_44_CopyEqClassInfoToTag {
        input:
            bam = t_35_MergeAllAnnotatedArrayElementsWithOriginalNames.merged_bam,
            eq_class_file = t_43_CombineEqClassFiles.combined_tx_eq_class_assignments,
            prefix = SM + "_annotated_array_elements_for_quant_with_gene_names"
    }

    call TX_PRE.CorrectUmisWithSetCover as t_45_CorrectUmisWithSetCover {
        input:
            bam = t_44_CopyEqClassInfoToTag.bam_out,
            prefix = SM + "_annotated_array_elements_for_quant_with_gene_names"
    }

    # Because of how we're doing things, we need to pull out the CCS and CCS Reclaimed reads from the output of the
    # set cover correction:
    call Utils.Bamtools as t_46_GetCcsCorrectedReadsWithCorrectedUmis {
        input:
            bamfile = t_45_CorrectUmisWithSetCover.corrected_umi_reads,
            prefix = SM + "_annotated_array_elements_for_quant_with_gene_names.corrected_umis.CCS",
            cmd = "filter",
            args = '-tag "rq":">=' + min_read_quality + '"',
            runtime_attr_override = disable_preemption_runtime_attrs
        }

    call Utils.Bamtools as t_47_GetCcsReclaimedReadsWithCorrectedUmis {
            input:
                bamfile =t_45_CorrectUmisWithSetCover.corrected_umi_reads,
                prefix = SM + "_annotated_array_elements_for_quant_with_gene_names.corrected_umis.CCS_Reclaimed",
                cmd = "filter",
                args = '-tag "rq":"<' + min_read_quality + '"',
                runtime_attr_override = disable_preemption_runtime_attrs
        }

    call UMI_TOOLS.Run_Group as t_48_UMIToolsGroup {
        input:
            aligned_transcriptome_reads = t_45_CorrectUmisWithSetCover.corrected_umi_reads,
            aligned_transcriptome_reads_index = t_45_CorrectUmisWithSetCover.corrected_umi_reads_index,
            do_per_cell = true,
            prefix = SM + "_annotated_array_elements_with_gene_names_with_umi_tools_group_correction"
    }

    # Create CCS count matrix and anndata:
    call TX_POST.CreateCountMatrixFromAnnotatedBam as t_49_CreateCCSCountMatrixFromAnnotatedBam {
        input:
            annotated_transcriptome_bam = t_46_GetCcsCorrectedReadsWithCorrectedUmis.bam_out,
            tx_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_tx_eq_class_assignments,
            umi_tag = "BX",
            prefix = SM + "_ccs_gene_tx_expression_count_matrix"
    }

    call TX_POST.CreateCountMatrixAnndataFromEquivalenceClasses as t_50_CreateCCSCountMatrixAnndataFromEqClasses {
        input:
            count_matrix_tsv = t_49_CreateCCSCountMatrixFromAnnotatedBam.count_matrix,
            genome_annotation_gtf_file = t_14_ST2_Quant.st_gtf,
            gencode_reference_gtf_file = genome_annotation_gtf,
            overlap_intervals = intervals_of_interest,
            overlap_interval_label = interval_overlap_name,
            tx_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_tx_eq_class_assignments,
            tx_equivalence_class_definitions = t_43_CombineEqClassFiles.combined_tx_eq_class_defs,
            gene_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_gene_eq_class_assignments,
            gene_equivalence_class_definitions = t_43_CombineEqClassFiles.combined_gene_eq_class_defs,
            prefix = SM + "_ccs_gene_tx_expression_count_matrix",

            runtime_attr_override = object {mem_gb: 64}
    }

    # Create CLR count matrix and anndata:
    call TX_POST.CreateCountMatrixFromAnnotatedBam as t_51_CreateCLRCountMatrixFromAnnotatedBam {
        input:
            annotated_transcriptome_bam = t_47_GetCcsReclaimedReadsWithCorrectedUmis.bam_out,
            tx_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_tx_eq_class_assignments,
            umi_tag = "BX",
            prefix = SM + "_clr_gene_tx_expression_count_matrix"
    }

    call TX_POST.CreateCountMatrixAnndataFromEquivalenceClasses as t_52_CreateCLRCountMatrixAnndataFromEqClasses {
        input:
            count_matrix_tsv = t_51_CreateCLRCountMatrixFromAnnotatedBam.count_matrix,
            genome_annotation_gtf_file = t_14_ST2_Quant.st_gtf,
            gencode_reference_gtf_file = genome_annotation_gtf,
            overlap_intervals = intervals_of_interest,
            overlap_interval_label = interval_overlap_name,
            tx_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_tx_eq_class_assignments,
            tx_equivalence_class_definitions = t_43_CombineEqClassFiles.combined_tx_eq_class_defs,
            gene_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_gene_eq_class_assignments,
            gene_equivalence_class_definitions = t_43_CombineEqClassFiles.combined_gene_eq_class_defs,
            prefix = SM + "_clr_gene_tx_expression_count_matrix",

            runtime_attr_override = object {mem_gb: 64}
    }

    # Create overall count matrix and anndata:
    call TX_POST.CreateCountMatrixFromAnnotatedBam as t_53_CreateOverallCountMatrixFromAnnotatedBam {
        input:
            annotated_transcriptome_bam = t_45_CorrectUmisWithSetCover.corrected_umi_reads,
            tx_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_tx_eq_class_assignments,
            umi_tag = "BX",
            prefix = SM + "_overall_gene_tx_expression_count_matrix"
    }

    call TX_POST.CreateCountMatrixAnndataFromEquivalenceClasses as t_54_CreateOverallCountMatrixAnndataFromEqClasses {
        input:
            count_matrix_tsv = t_53_CreateOverallCountMatrixFromAnnotatedBam.count_matrix,
            genome_annotation_gtf_file = t_14_ST2_Quant.st_gtf,
            gencode_reference_gtf_file = genome_annotation_gtf,
            overlap_intervals = intervals_of_interest,
            overlap_interval_label = interval_overlap_name,
            tx_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_tx_eq_class_assignments,
            tx_equivalence_class_definitions = t_43_CombineEqClassFiles.combined_tx_eq_class_defs,
            gene_equivalence_class_assignments = t_43_CombineEqClassFiles.combined_gene_eq_class_assignments,
            gene_equivalence_class_definitions = t_43_CombineEqClassFiles.combined_gene_eq_class_defs,
            prefix = SM + "_overall_gene_tx_expression_count_matrix",

            runtime_attr_override = object {mem_gb: 64}
    }


    #################################################
    #     ___   ____      __  __  __ _____ _____ ____  ___ ____ ____
    #    / _ \ / ___|    / / |  \/  | ____|_   _|  _ \|_ _/ ___/ ___|
    #   | | | | |       / /  | |\/| |  _|   | | | |_) || | |   \___ \
    #   | |_| | |___   / /   | |  | | |___  | | |  _ < | | |___ ___) |
    #    \__\_\\____| /_/    |_|  |_|_____| |_| |_| \_\___\____|____/
    #
    #################################################

    call AM.SamtoolsStats as t_55_AlignedArrayElementStats {
        input:
            bam = t_10_MergeAllAlignedArrayElementsNonTruncated.merged_bam
    }

    call AM.SamtoolsStats as t_56_AlignedFilteredArrayElementStats {
        input:
            bam = t_13_MergeAllAlignedAndFilteredArrayElements.merged_bam
    }

    call AM.SamtoolsStats as t_57_AlignedAnnotatedArrayElementsForQuantStats {
        input:
            bam = t_45_CorrectUmisWithSetCover.corrected_umi_reads
    }

    call LONGBOW.AggregateCorrectLogStats as t_58_AggregateLongbowCorrectStats {
        input:
            longbow_correct_log_files = flatten([t_29_LongbowCorrectCCSReclaimedArrayElementCBCs.log, t_21_LongbowCorrectCCSCorrectedArrayElementCBCs.log]),
            out_name = SM + "_longbow_correct_stats.txt"
    }

    ######################################################################
    #             _____ _             _ _
    #            |  ___(_)_ __   __ _| (_)_______
    #            | |_  | | '_ \ / _` | | |_  / _ \
    #            |  _| | | | | | (_| | | |/ /  __/
    #            |_|   |_|_| |_|\__,_|_|_/___\___|
    #
    ######################################################################

    # NOTE: We key all finalization steps on the static report.
    #       This will prevent incomplete runs from being placed in the output folders.

#    File keyfile = t_54_CreateOverallCountMatrixAnndataFromEqClasses.transcript_gene_count_anndata_h5ad

    # This seems to take longer to get to:
    File keyfile = t_48_UMIToolsGroup.output_tsv

    String base_out_dir = outdir + "/" + DIR + out_dir_suffix + "/" + t_01_WdlExecutionStartTimestamp.timestamp_string
    String stats_out_dir = base_out_dir + "/stats"
    String array_element_dir = base_out_dir + "/annotated_array_elements"
    String intermediate_reads_dir = base_out_dir + "/intermediate_reads"

    String meta_files_dir = base_out_dir + "/meta_files"

    String intermediate_array_reads_dir = intermediate_reads_dir + "/array_reads"
    String intermediate_array_elements_dir = intermediate_reads_dir + "/array_elements"

    String quant_dir = base_out_dir + "/quant"

    ##############################################################################################################
    # Finalize gene / tx assignments:
    call FF.FinalizeToDir as t_59_FinalizeEqClasses {
        input:
            files = [
                t_43_CombineEqClassFiles.combined_gene_eq_class_defs,
                t_43_CombineEqClassFiles.combined_gene_eq_class_assignments,
                t_43_CombineEqClassFiles.combined_tx_eq_class_defs,
                t_43_CombineEqClassFiles.combined_tx_eq_class_assignments,
            ],
            outdir = quant_dir + "/eqivalence_classes",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_60_FinalizeUmiToolsOutputs {
        input:
            files = [
                t_48_UMIToolsGroup.output_bam,
                t_48_UMIToolsGroup.output_tsv,
            ],
            outdir = quant_dir + "/UMITools",
            keyfile = keyfile
    }

    # CCS:
    call FF.FinalizeToDir as t_61_FinalizeCCSTxAndGeneAssignments {
        input:
            files = [
                t_49_CreateCCSCountMatrixFromAnnotatedBam.count_matrix,
                t_50_CreateCCSCountMatrixAnndataFromEqClasses.transcript_gene_count_anndata_h5ad,
            ],
            outdir = quant_dir + "/CCS",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_62_FinalizeCCSRawQuantPickles {
        input:
            files = t_50_CreateCCSCountMatrixAnndataFromEqClasses.pickles,
            outdir = quant_dir + "/CCS",
            keyfile = keyfile
    }

    # CLR:
    call FF.FinalizeToDir as t_63_FinalizeCLRTxAndGeneAssignments {
        input:
            files = [
                t_51_CreateCLRCountMatrixFromAnnotatedBam.count_matrix,
                t_52_CreateCLRCountMatrixAnndataFromEqClasses.transcript_gene_count_anndata_h5ad,
            ],
            outdir = quant_dir + "/CLR",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_64_FinalizeCLRRawQuantPickles {
        input:
            files = t_52_CreateCLRCountMatrixAnndataFromEqClasses.pickles,
            outdir = quant_dir + "/CLR",
            keyfile = keyfile
    }

    # Overall:
    call FF.FinalizeToDir as t_65_FinalizeOverallTxAndGeneAssignments {
        input:
            files = [
                t_53_CreateOverallCountMatrixFromAnnotatedBam.count_matrix,
                t_54_CreateOverallCountMatrixAnndataFromEqClasses.transcript_gene_count_anndata_h5ad,
            ],
            outdir = quant_dir + "/Overall",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_66_FinalizeOverallRawQuantPickles {
        input:
            files = t_54_CreateOverallCountMatrixAnndataFromEqClasses.pickles,
            outdir = quant_dir + "/Overall",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_67_FinalizeRefAndSt2Comparisons {
        input:
            files = [
                t_36_GffCompareStringtie2toGencode.refmap,
                t_36_GffCompareStringtie2toGencode.tmap,
                t_36_GffCompareStringtie2toGencode.tracking,
                t_36_GffCompareStringtie2toGencode.loci,
                t_36_GffCompareStringtie2toGencode.annotated_gtf,
                t_36_GffCompareStringtie2toGencode.stats,
                t_36_GffCompareStringtie2toGencode.log,

                t_37_GffCompareGencodetoStringtie2.refmap,
                t_37_GffCompareGencodetoStringtie2.tmap,
                t_37_GffCompareGencodetoStringtie2.tracking,
                t_37_GffCompareGencodetoStringtie2.loci,
                t_37_GffCompareGencodetoStringtie2.annotated_gtf,
                t_37_GffCompareGencodetoStringtie2.stats,
                t_37_GffCompareGencodetoStringtie2.log,
            ],
            outdir = quant_dir + "/gencode_and_stringtie2",
            keyfile = keyfile
    }

    # Finalize gene / tx assignment by contig:
    # NOTE: According to the scatter/gather documentation in the WDL spec, this will work correctly
    #       (https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#scatter--gather)
    scatter (i in range(length(t_38_SplitArrayElementsByContig.contig_bams))) {
        String contig = t_38_SplitArrayElementsByContig.contig_names[i]

        call FF.FinalizeToDir as t_68_FinalizeTxAndGeneAssignmentsByContig {
            input:
                files = [
                    t_40_GffCompareStringtie2toMasSeqReads.refmap[i],
                    t_40_GffCompareStringtie2toMasSeqReads.tmap[i],
                    t_40_GffCompareStringtie2toMasSeqReads.tracking[i],
                    t_40_GffCompareStringtie2toMasSeqReads.loci[i],
                    t_40_GffCompareStringtie2toMasSeqReads.annotated_gtf[i],
                    t_40_GffCompareStringtie2toMasSeqReads.stats[i],
                    t_40_GffCompareStringtie2toMasSeqReads.log[i],

                    t_41_GffCompareGencodetoMasSeqReads.refmap[i],
                    t_41_GffCompareGencodetoMasSeqReads.tmap[i],
                    t_41_GffCompareGencodetoMasSeqReads.tracking[i],
                    t_41_GffCompareGencodetoMasSeqReads.loci[i],
                    t_41_GffCompareGencodetoMasSeqReads.annotated_gtf[i],
                    t_41_GffCompareGencodetoMasSeqReads.stats[i],
                    t_41_GffCompareGencodetoMasSeqReads.log[i],

                    t_42_QuantifyGffComparison.gene_assignments_file[i],
                    t_42_QuantifyGffComparison.tx_equivalence_class_labels_file[i],
                    t_42_QuantifyGffComparison.tx_equivalence_class_file[i],
                    t_42_QuantifyGffComparison.graph_gpickle[i],
                ],
                outdir = quant_dir + "/by_contig/" + contig,
                keyfile = keyfile
        }
    }

    ##############################################################################################################
    # Finalize annotated, aligned array elements:
    call FF.FinalizeToDir as t_69_FinalizeIntermediateAnnotatedArrayElements {
        input:
            files = [
                t_10_MergeAllAlignedArrayElementsNonTruncated.merged_bam,
                t_10_MergeAllAlignedArrayElementsNonTruncated.merged_bai,

                t_13_MergeAllAlignedAndFilteredArrayElements.merged_bam,
                t_13_MergeAllAlignedAndFilteredArrayElements.merged_bai,

                t_23_MergeLongbowPaddedCBCCorrectedCCSArrayElements.merged_bam,
                t_23_MergeLongbowPaddedCBCCorrectedCCSArrayElements.merged_bai,
                t_24_MergeLongbowPaddedCBCUncorrectableCCSArrayElements.merged_bam,
                t_24_MergeLongbowPaddedCBCUncorrectableCCSArrayElements.merged_bai,
                t_31_MergeLongbowPaddedCBCCorrectedCCSReclaimedArrayElements.merged_bam,
                t_31_MergeLongbowPaddedCBCCorrectedCCSReclaimedArrayElements.merged_bai,
                t_32_MergeLongbowPaddedCBCUncorrectableCCSReclaimedArrayElements.merged_bam,
                t_32_MergeLongbowPaddedCBCUncorrectableCCSReclaimedArrayElements.merged_bai,

                t_33_RestoreCcsOriginalReadNames.bam_out,
                t_34_RestoreClrOriginalReadNames.bam_out,

                t_35_MergeAllAnnotatedArrayElementsWithOriginalNames.merged_bam,
                t_35_MergeAllAnnotatedArrayElementsWithOriginalNames.merged_bai,

                t_45_CorrectUmisWithSetCover.uncorrected_umi_reads
            ],
            outdir = intermediate_array_elements_dir,
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_70_FinalizeAnnotatedArrayElements {
        input:
            files = [
                t_45_CorrectUmisWithSetCover.corrected_umi_reads,
                t_45_CorrectUmisWithSetCover.corrected_umi_reads_index,

                t_46_GetCcsCorrectedReadsWithCorrectedUmis.bam_out,
                t_47_GetCcsReclaimedReadsWithCorrectedUmis.bam_out,

                t_13_MergeAllAlignedAndFilteredArrayElements.merged_bam,
                t_13_MergeAllAlignedAndFilteredArrayElements.merged_bai
            ],
            outdir = array_element_dir,
            keyfile = keyfile
    }

    ##############################################################################################################
    # Finalize meta files:
    call FF.FinalizeToDir as t_71_FinalizeMeta {
        input:
            files = [
                cell_barcode_whitelist,
            ],
            outdir = meta_files_dir,
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_72_FinalizeCCSCBCcorrectionLogsToMeta {
        input:
            files = t_21_LongbowCorrectCCSCorrectedArrayElementCBCs.log,
            outdir = meta_files_dir + "/" + "ccs_cbc_correction_logs",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_73_FinalizeCCSRejectedCBCcorrectionLogsToMeta {
        input:
            files = t_29_LongbowCorrectCCSReclaimedArrayElementCBCs.log,
            outdir = meta_files_dir + "/" + "ccs_rejected_cbc_correction_logs",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_74_FinalizeCCSUmiAdjustmentLogs {
        input:
            files = t_22_AdjustCCSUMIs.log,
            outdir = meta_files_dir + "/" + "umi_adjustment_logs",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_75_FinalizeCCSReclaimedUmiAdjustmentLogs {
        input:
            files = t_30_AdjustCCSReclaimedUMIs.log,
            outdir = meta_files_dir + "/" + "umi_adjustment_logs",
            keyfile = keyfile
    }

    ##############################################################################################################
    # Finalize the discovered transcriptome:
    if ( !is_SIRV_data ) {
        call FF.FinalizeToDir as t_76_FinalizeDiscoveredTranscriptome {
            input:
                files = [
                    t_14_ST2_Quant.st_gtf,
                    t_15_ST2_ExtractTranscriptSequences.transcripts_fa,
                    t_15_ST2_ExtractTranscriptSequences.transcripts_fai,
                    t_15_ST2_ExtractTranscriptSequences.transcripts_dict,
                    t_16_ST2_CompareTranscriptomes.annotated_gtf,
                    t_16_ST2_CompareTranscriptomes.loci,
                    t_16_ST2_CompareTranscriptomes.stats,
                    t_16_ST2_CompareTranscriptomes.tracking,
                    t_16_ST2_CompareTranscriptomes.refmap,
                    t_16_ST2_CompareTranscriptomes.tmap,
                ],
                outdir = base_out_dir + "/discovered_transcriptome",
                keyfile = keyfile
        }
    }

    ##############################################################################################################
    # Finalize Stats:

    call FF.FinalizeToDir as t_77_FinalizeQuantArrayElementStats {
        input:
            files = [
                t_57_AlignedAnnotatedArrayElementsForQuantStats.raw_stats,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.summary_stats,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.first_frag_qual,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.last_frag_qual,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.first_frag_gc_content,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.last_frag_gc_content,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.acgt_content_per_cycle,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.insert_size,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.read_length_dist,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.indel_distribution,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.indels_per_cycle,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.coverage_distribution,
                t_57_AlignedAnnotatedArrayElementsForQuantStats.gc_depth,
            ],
            outdir = stats_out_dir + "/array_elements_for_quant/",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_78_FinalizeTxomeDiscoveryArrayElementStats {
        input:
            files = [
                t_56_AlignedFilteredArrayElementStats.raw_stats,
                t_56_AlignedFilteredArrayElementStats.summary_stats,
                t_56_AlignedFilteredArrayElementStats.first_frag_qual,
                t_56_AlignedFilteredArrayElementStats.last_frag_qual,
                t_56_AlignedFilteredArrayElementStats.first_frag_gc_content,
                t_56_AlignedFilteredArrayElementStats.last_frag_gc_content,
                t_56_AlignedFilteredArrayElementStats.acgt_content_per_cycle,
                t_56_AlignedFilteredArrayElementStats.insert_size,
                t_56_AlignedFilteredArrayElementStats.read_length_dist,
                t_56_AlignedFilteredArrayElementStats.indel_distribution,
                t_56_AlignedFilteredArrayElementStats.indels_per_cycle,
                t_56_AlignedFilteredArrayElementStats.coverage_distribution,
                t_56_AlignedFilteredArrayElementStats.gc_depth,
            ],
            outdir = stats_out_dir + "/array_elements_for_transcriptome_discovery/",
            keyfile = keyfile
    }

    call FF.FinalizeToDir as t_79_FinalizeAlignedArrayElementStats {
        input:
            files = [
                t_55_AlignedArrayElementStats.raw_stats,
                t_55_AlignedArrayElementStats.summary_stats,
                t_55_AlignedArrayElementStats.first_frag_qual,
                t_55_AlignedArrayElementStats.last_frag_qual,
                t_55_AlignedArrayElementStats.first_frag_gc_content,
                t_55_AlignedArrayElementStats.last_frag_gc_content,
                t_55_AlignedArrayElementStats.acgt_content_per_cycle,
                t_55_AlignedArrayElementStats.insert_size,
                t_55_AlignedArrayElementStats.read_length_dist,
                t_55_AlignedArrayElementStats.indel_distribution,
                t_55_AlignedArrayElementStats.indels_per_cycle,
                t_55_AlignedArrayElementStats.coverage_distribution,
                t_55_AlignedArrayElementStats.gc_depth,
            ],
            outdir = stats_out_dir + "/aligned_array_elements/",
            keyfile = keyfile
    }

    ##############################################################################################################
    # Write out completion file so in the future we can be 100% sure that this run was good:
    call FF.WriteCompletionFile as t_80_WriteCompletionFile {
        input:
            outdir = base_out_dir + "/",
            keyfile = keyfile
    }
}