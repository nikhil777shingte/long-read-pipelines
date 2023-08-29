version 1.0

import "../../../tasks/Utility/Utils.wdl" as Utils

workflow RunCrispy {
    meta {
        description: "A workflow that runs Cris.py on fastq files."
    }
    parameter_meta {
        script_file:           "Crispy python script file"
        fastq_file:           "fastq input file"
        ref_seq_file:           "File containing Reference sequence for Crispy"
        seq_start: "Start equence for Crispy"
        seq_end: "End sequence for Crispy"
        test_list: "WT or MUT sequence for Crispy e.g. MUT|ATCGTA|WT|TGACATC..."
        outdir: "File containing Reference sequence for Crispy"
    }

    input {
        String script_file
        String fastq_file
        String ref_seq_file
        String seq_start
        String seq_end
        String test_list
        String outdir
    }

    call Utils.Crispy {
        input:
            merged_fastq =  fastq_file,
            crispy = script_file,
            ref_seq_file =  ref_seq_file,
            seq_start = seq_start,
            seq_end = seq_end,
            test_list = test_list,
            outdir = outdir
    }
}
