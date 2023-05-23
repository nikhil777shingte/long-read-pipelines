version 1.0

import "tasks/Finalize.wdl" as FF

workflow DystPeaker {
    meta {
        description: "Collect read length information from a long reads BAM."
    }
    input {
        File input_file
        Boolean input_is_bam
        String id
        Int short_reads_threshold

        String gcs_out_root_dir
    }
    parameter_meta {
        gcs_out_root_dir: "Cloud storage output directory"
        id: "A distinguishing ID that's going to impact how the files are named and where they are placed in the directories."
        short_reads_threshold: "A threshold below which the reads will be classified as short"

        read_lengths_hist: "Read length histogram"
        peaks: "Estimated peaks in the read length distritbution"
        reverse_yield: "A lenth-9 array of lengths at which a certain fraction of reads are shorter than. The fraction bins are 10% to 90% with 10% increments."
        read_len_summaries: "A summary on some other metrics related to read length"
    }

    String relative_dir = "ReadLengthMetrics"
    String output_dir = sub(gcs_out_root_dir, "/$", "") + "/" + relative_dir + "/" + id

    # collect
    if (input_is_bam) {
        Float bam_sz = size(input_file, "GB")
        Int magic_streaming_file_sz_threshold = 25
        if (bam_sz < magic_streaming_file_sz_threshold) {
            call GetLengthsStreamFromBam { input: bam = input_file }
        }
        if (! (bam_sz < magic_streaming_file_sz_threshold)) {
            call GetLengthsFromBam       { input: bam = input_file }
        }
    }
    if ( !input_is_bam ) {
        call GetLengthsFromFastq { input: fastq = input_file }
    }
    File rl_file = select_first([GetLengthsStreamFromBam.read_lengths, GetLengthsFromBam.read_lengths, GetLengthsFromFastq.read_lengths])

    # stats
    call Dyst { input: read_lengths_txt = rl_file }
    call Peaker { input: dyst_histogram = Dyst.histogram }
    call ReverseYield { input: read_lengths_txt = rl_file }
    call Skewness { input: read_lengths_txt = rl_file }
    call GetNumReadsAndShors { input: read_lengths_txt = rl_file, short_threshold = short_reads_threshold }
    String raw_pct = round(100 * GetNumReadsAndShors.num_shorts/GetNumReadsAndShors.num_seqs)

    call FF.FinalizeToFile { input: outdir = output_dir, file = GetNumReadsAndShors.rl_bz2, name = id + ".readLen.txt.bz2",}

    output {
        File read_lengths_hist = Dyst.histogram
        Array[Int] peaks = Peaker.peaks
        Array[Int] reverse_yield = ReverseYield.reverse_yield

        Map[String, String] read_len_summaries = {'shortie_pct': raw_pct + "%",
                                                  'skew': Skewness.skew,
                                                  'raw_rl_file': FinalizeToFile.gcs_path}
    }
}

task GetLengthsStreamFromBam {
    meta {
        description:
        "Stream from a BAM version"
    }
    input {
        String bam
    }

    command <<<
        set -eux
        export GCS_OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
        samtools view \
            -F 256 \
            -F 2048 \
            ~{bam} | \
            awk -F '\t' '{print $10}' | \
            awk '{print length}' \
            > "lengths.txt"
    >>>

    output {
        File read_lengths = "lengths.txt"
    }

    runtime {
        cpu: 2
        memory: "8 GiB"
        disks: "local-disk 100 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
    }
}

task GetLengthsFromBam {
    meta {
        description:
        "From a downloaded-BAM version"
    }
    input {
        File bam
    }

    command <<<
        set -eux
        samtools view \
            -F 256 \
            -F 2048 \
            ~{bam} | \
            awk -F '\t' '{print $10}' | \
            awk '{print length}' \
            > "lengths.txt"
    >>>

    output {
        File read_lengths = "lengths.txt"
    }

    runtime {
        cpu: 2
        memory: "8 GiB"
        disks: "local-disk 100 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
    }
}

task GetLengthsFromFastq {
    meta {
        description:
        "From a downloaded-BAM version"
    }
    input {
        File fastq
    }

    Int disk_size = 2 * ceil(size(fastq, "GiB"))
    command <<<
        set -eux

        zcat ~{fastq} | awk '{if(NR%4==2) print length}' > "lengths.txt"
    >>>

    output {
        File read_lengths = "lengths.txt"
    }

    runtime {
        cpu: 2
        memory: "8 GiB"
        disks: "local-disk ~{disk_size} HDD"
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
    }
}

task GetNumReadsAndShors {
    meta {
        desciption:
        "Get number of reads and those that are too short. Also compress."
    }
    input {
        File read_lengths_txt
        Int short_threshold
    }

    String prefix = basename(read_lengths_txt, ".txt")
    command <<<
        set -eux

        wc -l ~{read_lengths_txt} | awk '{print $1}' > "total.txt"

        awk -v thesh=~{short_threshold} \
               '{if ($1<thesh) print}' ~{read_lengths_txt} \
            | wc -l \
            | awk '{print $1}' \
        > "shorts.txt"

        mv ~{read_lengths_txt} ~{prefix}.txt
        bzip2 -v9 ~{prefix}.txt
    >>>

    output {
        Float num_seqs = read_float("total.txt")
        Float num_shorts = read_float("shorts.txt")
        File rl_bz2 = "~{prefix}.txt.bz2"
    }
    runtime {disks: "local-disk 100 HDD" docker: "gcr.io/cloud-marketplace/google/ubuntu2004:latest"}
}

task Dyst {
    input {
        File read_lengths_txt
    }

    command <<<
        set -eux

        mv ~{read_lengths_txt} "read_lengths.txt"
        dyst -h
        dyst -n -b 100 -i "read_lengths.txt" \
            > "read_lengths.hist.txt"
        cat "read_lengths.hist.txt"
    >>>

    output {
        File histogram = "read_lengths.hist.txt"
    }

    runtime {
        cpu: 4
        memory: "20 GiB"
        disks: "local-disk 100 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/lr-dyst-peaker:0.0.1"
    }
}

task Peaker {
    input {
        File dyst_histogram
    }

    command <<<
        set -eux

        grep -v "^#" ~{dyst_histogram} | awk -F ':' '{print $1}' \
            > "prepped_dyst_plain.hist"

        python3 /opt/find_peaks.py \
            -i "prepped_dyst_plain.hist" \
            -o "peaks.txt"
    >>>

    output {
        Array[Int] peaks = read_lines("peaks.txt")
    }

    runtime {
        disks: "local-disk 100 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/lr-dyst-peaker:0.0.1"
    }
}

task ReverseYield {
    input {
        File read_lengths_txt
    }

    command <<<
        python3 /opt/reverse_yield.py \
            -i ~{read_lengths_txt} \
            -o "reverse_yield.txt"
    >>>

    output {
        Array[Int] reverse_yield = read_lines("reverse_yield.txt")
    }

    runtime {
        disks: "local-disk 10 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/lr-dyst-peaker:0.0.2"
    }
}

task Skewness {
    input {
        File read_lengths_txt
    }

    command <<<
        python3 /opt/measure_g1_skew.py \
            -i ~{read_lengths_txt} \
            -o "skew.txt"
    >>>

    output {
        Float skew = read_float("skew.txt")
    }

    runtime {
        disks: "local-disk 10 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/lr-dyst-peaker:0.0.2"
    }
}
