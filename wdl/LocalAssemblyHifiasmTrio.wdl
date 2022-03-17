version 1.0

import "tasks/Utils.wdl" as Utils
import "tasks/Hifiasm.wdl" as Hifiasm
import "tasks/CallAssemblyVariants.wdl" as Align

workflow LocalAssembly {
    input {
        Array[String]+ loci
        String aligned_bam
        File   aligned_bai
        String prefix
#        Boolean add_unaligned_reads = false
        File mat_yak
        File pat_yak

        File ref_map_file
    }

    parameter_meta {
        loci:          "Loci to assemble. At least one is required. Reads from all loci will be merged for assembly. Format: [\"chr1:1000-2000\", \"chr1:5000-10000\"]"
        aligned_bam:   "aligned file"
        aligned_bai:   "index file"
        prefix:        "prefix for output files"

#        add_unaligned_reads: "set to true to include unaligned reads in the assembly (default: false)"

        ref_map_file:  "table indicating reference sequence and auxillary file locations"
    }

    Map[String, String] ref_map = read_map(ref_map_file)

    scatter (locus in loci) {
        call Utils.SubsetBam {
            input:
                bam = aligned_bam,
                bai = aligned_bai,
                locus = locus
        }
    }

    if (length(loci) > 1)  {
        call Utils.MergeBams {
            input:
                bams = SubsetBam.subset_bam,
                prefix = "merged"
        }
    }

    File subset_bam = select_first([MergeBams.merged_bam, SubsetBam.subset_bam[0]])

    call Utils.BamToFastq {
        input:
            bam = subset_bam,
            prefix = prefix
    }

    call Hifiasm.Assemble_trio {
        input:
            reads = BamToFastq.reads_fq,
            prefix = prefix,
            mat_yak = mat_yak,
            pat_yak = pat_yak
    }

    call Align.AlignAsPAF as align_hap1 {
        input:
            asm_fasta = Assemble_trio.h1_fa,
            ref_fasta = ref_map['fasta'],
            prefix = "~{prefix}_h1"
    }

        call Align.AlignAsPAF as align_hap2 {
        input:
            asm_fasta = Assemble_trio.h2_fa,
            ref_fasta = ref_map['fasta'],
            prefix = "~{prefix}_h2"
    }

    output {
        File h1_fa = Assemble_trio.h1_fa
        File h2_fa = Assemble_trio.h2_fa
        File h1_aligned = align_hap1.paf
        File h2_aligned = align_hap2.paf
    }
}