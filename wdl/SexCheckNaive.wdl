version 1.0

import "tasks/AlignedMetrics.wdl"

workflow SexCheckNaive {
    input {
        File bam
        File bai
        File chr1_bed
        File chrX_bed
        File chrY_bed
        String expected_sex_type
    }

    call AlignedMetrics.MosDepthWGS {input: bam = bam, bai = bai}
    call SummarizeCoverages {input: mosdepth_summary_txt = MosDepthWGS.summary_txt}

    call MakeACall {
        input:
            cov_chr1 = SummarizeCoverages.cov_chr1,
            cov_chrX = SummarizeCoverages.cov_chrX,
            cov_chrY = SummarizeCoverages.cov_chrY,
            expected_sex_type = expected_sex_type
    }

    output {
        Map[String, String] inferred_sex_info = MakeACall.inferred_sex_info
    }
}

task SummarizeCoverages {
    input {
        File mosdepth_summary_txt
    }

    command <<<
        set -eux

        cat ~{mosdepth_summary_txt}

        grep -w "chr1" ~{mosdepth_summary_txt} | \
            awk -F '\t' '{print $4}' | \
            xargs printf "%0.2f\n" > cov.chr1.txt

        grep -w "chrX" ~{mosdepth_summary_txt} | \
            awk -F '\t' '{print $4}' | \
            xargs printf "%0.2f\n" > cov.chrX.txt
        grep -w "chrY" ~{mosdepth_summary_txt} | \
            awk -F '\t' '{print $4}' | \
            xargs printf "%0.2f\n" > cov.chrY.txt
    >>>

    output {
        Float cov_chr1 = read_float("cov.chr1.txt")
        Float cov_chrX = read_float("cov.chrX.txt")
        Float cov_chrY = read_float("cov.chrY.txt")
    }

    runtime {
        disks: "local-disk 100 HDD"
        docker: "gcr.io/cloud-marketplace/google/ubuntu2004:latest"
    }
}

task MakeACall {
    input {
        Float cov_chr1
        Float cov_chrX
        Float cov_chrY

        String expected_sex_type
    }

    Int expected_sex_code = if expected_sex_type == 'F' then 2 else if expected_sex_type == 'M' then 1 else 0

    command <<<
        set -eux

        scaled_x_dp_mean=$(echo "scale=2; 2*~{cov_chrX}/~{cov_chr1}" | bc)
        scaled_y_dp_mean=$(echo "scale=2; 2*~{cov_chrY}/~{cov_chr1}" | bc)

        touch my_call.tsv
        echo -e "scaled_x_dp_mean\t${scaled_x_dp_mean}" >> my_call.tsv
        echo -e "scaled_y_dp_mean\t${scaled_y_dp_mean}" >> my_call.tsv

        nx=$(echo "${scaled_x_dp_mean}" | awk '{print int($1+0.5)}')
        ny=$(echo "${scaled_y_dp_mean}" | awk '{print int($1+0.5)}')
        export nx
        xchar=$(perl -e "print 'X' x ${nx}")
        export ny
        if [[ ${ny} -eq 0 ]]; then ychar=''; else ychar=$(perl -e "print 'Y' x ${ny}"); fi
        echo -e "sex_call\t${xchar}${ychar}" >> my_call.tsv

        # unsure if this is correct
        if [[ ~{expected_sex_code} -eq 1 ]];
        then
            if [[ ${ny} -ge 1 ]];
            then
                echo -e "is_sex_concordant\ttrue" >> my_call.tsv
            else
                echo -e "is_sex_concordant\tfalse" >> my_call.tsv
            fi
        elif [[ ~{expected_sex_code} -eq 2 ]];
        then
            if [[ ${nx} -ge 2 ]];
            then
                echo -e "is_sex_concordant\ttrue" >> my_call.tsv;
            else
                echo -e "is_sex_concordant\tfalse" >> my_call.tsv
            fi
        else
            echo -e "is_sex_concordant\ttrue" >> my_call.tsv
        fi
    >>>

    output {
        Map[String, String] inferred_sex_info = read_map("my_call.tsv")
    }

    runtime {
        disks: "local-disk 100 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/somalier:v0.2.15" # need bc for floating point arith.
    }
}
