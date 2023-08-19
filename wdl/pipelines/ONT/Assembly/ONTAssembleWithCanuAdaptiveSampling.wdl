version 1.0

import "../../../tasks/Utility/Utils.wdl" as Utils
import "../../../tasks/Assembly/Canu.wdl" as Canu
import "../../../tasks/Alignment/AlignReads.wdl" as Minimap2
import "../../../tasks/Preprocessing/Medaka.wdl" as Medaka
import "../../../tasks/VariantCalling/CallAssemblyVariants.wdl" as AV
import "../../../tasks/QC/Quast.wdl" as Quast
import "../../../tasks/Utility/Finalize.wdl" as FF

workflow ONTAssembleWithCanuAdaptiveSampling {
    meta {
        description: "A workflow that performs single sample genome assembly on ONT reads from one or more flow cells. The workflow merges multiple samples into a single BAM prior to genome assembly and variant calling."
    }
    parameter_meta {
        gcs_fastq_dir:       "GCS path to unaligned CCS BAM files"
        ref_map_file:        "table indicating reference sequence and auxillary file locations"

        genome_size:        "Genome size String for Canu"
        correct_error_rate:  "stringency for overlaps in Canu's correction step"
        trim_error_rate:     "stringency for overlaps in Canu's trim step"
        assemble_error_rate: "stringency for overlaps in Canu's assemble step"
        medaka_model:        "Medaka polishing model name"

        participant_name:    "name of the participant from whom these samples were obtained"
        prefix:              "prefix for output files"

        gcs_out_root_dir:    "GCS bucket to store the reads, variants, and metrics files"

        minimap_on_target_flanks_bed:        "on_target_flanks_bed file for bedtools to select region of interest"
        minimap_RG:               "read group information to be supplied to parameter '-R' (note that tabs should be input as '\t')"
        minimap_map_preset:       "preset to be used for minimap2 parameter '-x'"
        minimap_tags_to_preserve: "sam tags to carry over to aligned bam file"
        minimap_prefix:           "[default-valued] prefix for output BAM"
        crispy_script_file:           "Crispy python script file"
        crispy_ref_seq_file:           "File containing Reference sequence for Crispy"
        crispy_seq_start: "Start equence for Crispy"
        crispy_seq_end: "End sequence for Crispy"
        crispy_test_list: "WT or MUT sequence for Crispy e.g. MUT|ATCGTA|WT|TGACATC..."
        crispy_outdir: "File containing Reference sequence for Crispy"
    }

    input {
        String gcs_fastq_dir

        File ref_map_file

        Float correct_error_rate = 0.15
        Float trim_error_rate = 0.15
        Float assemble_error_rate = 0.15
        String medaka_model = "r941_prom_high_g360"

        String participant_name
        String prefix

        String gcs_out_root_dir
        String genome_size
        String minimap_on_target_flanks_bed
        String minimap_RG
        String minimap_map_preset
        Array[String] minimap_tags_to_preserve
        String minimap_prefix
        String crispy_script_file
        String crispy_ref_seq_file
        String crispy_seq_start
        String crispy_seq_end
        String crispy_test_list
        String crispy_outdir
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    String outdir = sub(gcs_out_root_dir, "/$", "") + "/ONTAssembleWithCanu/~{prefix}"

    call Utils.ComputeGenomeLength { input: fasta = ref_map['fasta'] }

    call Utils.ListFilesOfType { input: gcs_dir = gcs_fastq_dir, suffixes = [".fastq", ".fq", ".fastq.gz", ".fq.gz"] }
    call Utils.MergeFastqs { input: fastqs = ListFilesOfType.files }

    call Minimap2.Minimap2 {
        input:
            reads = ListFilesOfType.files,
            ref_fasta = ref_map['fasta'],
            on_target_flanks_bed = minimap_on_target_flanks_bed,
            RG = minimap_RG,
            map_preset = minimap_map_preset,
            tags_to_preserve = minimap_tags_to_preserve,
            prefix = prefix + ".minimap2"
    }

    call Utils.SelectReadsSeqkit {
        input:
            merged_fastq =  MergeFastqs.merged_fastq,
            reads_full =  Minimap2.full_reads_txt
    }

    call Utils.Crispy {
        input:
            merged_fastq =  MergeFastqs.merged_fastq,
            crispy = crispy_script_file,
            ref_seq_file =  crispy_ref_seq_file,
            seq_start = crispy_seq_start,
            seq_end = crispy_seq_end,
            test_list = crispy_test_list,
            outdir = crispy_outdir
    }

    call Canu.Canu {
        input:
            reads = SelectReadsSeqkit.filtered_full_reads_fastq,
            prefix = prefix,
            genome_size = genome_size,
            correct_error_rate = correct_error_rate,
            trim_error_rate = trim_error_rate,
            assemble_error_rate = assemble_error_rate
    }

    call Medaka.MedakaPolish {
        input:
            basecalled_reads = MergeFastqs.merged_fastq,
            draft_assembly = Canu.fa,
            model = medaka_model,
            prefix = basename(Canu.fa, ".fasta") + ".polished",
            n_rounds = 2
    }

    call Quast.Quast {
        input:
            ref = ref_map['fasta'],
            assemblies = [ MedakaPolish.polished_assembly ]
    }

    call AV.CallAssemblyVariants {
        input:
            asm_fasta = MedakaPolish.polished_assembly,
            ref_fasta = ref_map['fasta'],
            participant_name = participant_name,
            prefix = prefix + ".canu"
    }

    # Finalize data
    String dir = outdir + "/assembly"

    call FF.FinalizeToFile as FinalizeAsmUnpolished   { input: outdir = dir, file = Canu.fa }
    call FF.FinalizeToFile as FinalizeAsmPolished     { input: outdir = dir, file = MedakaPolish.polished_assembly }
    call FF.FinalizeToFile as FinalizeQuastReportHtml { input: outdir = dir, file = Quast.report_html }
    call FF.FinalizeToFile as FinalizeQuastReportTxt  { input: outdir = dir, file = Quast.report_txt }

    call Quast.SummarizeQuastReport as summaryQ {input: quast_report_txt = Quast.report_txt}
    Map[String, String] q_metrics = read_map(summaryQ.quast_metrics[0])

    output {
        File asm_unpolished = FinalizeAsmUnpolished.gcs_path
        File asm_polished = FinalizeAsmPolished.gcs_path

        File paf = CallAssemblyVariants.paf
        File paftools_vcf = CallAssemblyVariants.paftools_vcf

        File quast_report_html = FinalizeQuastReportHtml.gcs_path
        File quast_report_txt = FinalizeQuastReportTxt.gcs_path

        Map[String, String] quast_summary = q_metrics
    }
}
