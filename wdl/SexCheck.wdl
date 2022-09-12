version 1.0

import "tasks/utils/BAMutils.wdl"

workflow SexCheck {
    input {
        File bam
        File bai
        String expected_sex_type

        File sites_vcf
        File ref_map_file
        Int? min_gt_depth
    }

    call BAMutils.GetReadGroupInfo {input: uBAM = bam, keys = ['SM']}

    Map[String, String] ref_map = read_map(ref_map_file)
    call SomalierSexCheck {
        input:
            bam = bam,
            bai = bai,
            sample_name_in_bam = GetReadGroupInfo.read_group_info['SM'],
            expected_sex_type = expected_sex_type,
            sites_vcf = sites_vcf,
            ref_fasta = ref_map['fasta'],
            min_gt_depth = min_gt_depth
    }

    call MakeACall { input: somalier_samples_tsv = SomalierSexCheck.somalier_samples_tsv, expected_sex_type = expected_sex_type}

    output {
        Map[String, String] inferred_sex_info = MakeACall.inferred_sex_info
    }
}

task SomalierSexCheck {
    input {
        File bam
        File bai
        String sample_name_in_bam
        String expected_sex_type

        File sites_vcf
        File ref_fasta

        Int? min_gt_depth
    }

    Int sex_code = if expected_sex_type == 'F' then 2 else if expected_sex_type == 'M' then 1 else 0
    String depth_arg = if defined(min_gt_depth) then "--min-depth=~{min_gt_depth}" else " "

    command <<<
        set -eux

        touch ~{bai}

        extraction_dir='extracted'
        somalier extract \
            -d "${extraction_dir}" \
            --sites ~{sites_vcf} \
            -f ~{ref_fasta} \
            ~{bam}

        echo -e "family\t~{sample_name_in_bam}\tpaternal\tmaternal\t~{sex_code}\t-9" > semidummy_ped_file.ped

        somalier relate \
            ~{depth_arg} \
            --ped semidummy_ped_file.ped \
            -o ~{sample_name_in_bam}.somalier \
            "${extraction_dir}"/*.somalier
    >>>

    output {
        File semidummy_ped_file = "semidummy_ped_file.ped"
        File somalier_samples_tsv = "~{sample_name_in_bam}.somalier.samples.tsv"
        File somalier_samples_html = "~{sample_name_in_bam}.somalier.html"
    }

    runtime {
        disks: "local-disk 100 HDD"
        docker: "us.gcr.io/broad-dsp-lrma/somalier:v0.2.15"  # mirroring official version onto GCR for efficient pulling
    }
}

task MakeACall {
    input {
        File somalier_samples_tsv
        String expected_sex_type
    }

    Int expected_sex_code = if expected_sex_type == 'F' then 2 else if expected_sex_type == 'M' then 1 else 0

    command <<<
        set -eux

        col_m=$(head -1 ~{somalier_samples_tsv} | tr '\t' '\n' | grep -nF 'X_depth_mean')
        x_dp_mean=$(tail -1 ~{somalier_samples_tsv} | awk -v mm="${col_m}" '{print $mm}')
        col_n=$(head -1 ~{somalier_samples_tsv} | tr '\t' '\n' | grep -nF 'Y_depth_mean')
        y_dp_mean=$(tail -1 ~{somalier_samples_tsv} | awk -v nn="${col_n}" '{print $nn}')

        col_x=$(head -1 ~{somalier_samples_tsv} | tr '\t' '\n' | grep -nF 'gt_depth_mean')
        gt_dp_mean=$(tail -1 ~{somalier_samples_tsv} |  awk -v xx="${col_x}" '{print $xx}')

        scaled_x_dp_mean=$(echo "scale=2; 2*${x_dp_mean}/${gt_dp_mean}" | bc)
        scaled_y_dp_mean=$(echo "scale=2; 2*${y_dp_mean}/${gt_dp_mean}" | bc)

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
            echo -e "is_sex_concordant\tfalse" >> my_call.tsv
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
