version 1.0

workflow DystPeaker {
    input {
        String bam
        Int short_reads_threshold
    }

    call GetLengths { input: bam = bam, threashold = short_reads_threshold }
    call Dyst { input: read_lengths_txt = GetLengths.read_lengths }
    call Peaker { input: dyst_histogram = Dyst.histogram }
    call ReverseYield { input: read_lengths_txt = GetLengths.read_lengths }

    String raw_pct = round(100 * GetLengths.shorts/GetLengths.total_sequences)

    output {
        File read_lengths_hist = Dyst.histogram
        Array[Int] peaks = Peaker.peaks
        String pct_too_short = raw_pct + "%"
        Array[Int] reverse_yield = ReverseYield.reverse_yield
    }
}

task GetLengths {
    input {
        String bam
        Int threashold
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
        wc -l "lengths.txt" | awk '{print $1}' > "total.txt"
        awk -v thesh=~{threashold} '{if ($1<thesh) print}' "lengths.txt" | wc -l | awk '{print $1}' > "shorts.txt"
    >>>

    output {
        File read_lengths = "lengths.txt"
        Float total_sequences = read_float("total.txt")
        Float shorts = read_float("shorts.txt")
    }

    runtime {
        cpu: 2
        memory: "8 GiB"
        disks: "local-disk 100 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/lr-basic:0.1.1"
    }
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
