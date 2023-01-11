version 1.0

import "Structs.wdl"

task MergePerChrCalls {
    input {
        Array[File] vcfs
        File ref_dict
        String prefix

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 2*ceil(size(vcfs, "GB")) + 1

    command <<<
        set -euxo pipefail

        VCF_WITH_HEADER=~{vcfs[0]}
        GREPCMD="grep"
        if [[ ~{vcfs[0]} =~ \.gz$ ]]; then
            GREPCMD="zgrep"
        fi

        $GREPCMD '^#' $VCF_WITH_HEADER | grep -v -e '^##contig' -e CHROM > header
        grep '^@SQ' ~{ref_dict} | awk '{ print "##contig=<ID=" $2 ",length=" $3 ">" }' | sed 's/[SL]N://g' >> header
        $GREPCMD -m1 CHROM $VCF_WITH_HEADER >> header

        ((cat header) && ($GREPCMD -h -v '^#' ~{sep=' ' vcfs})) | bcftools sort | bgzip > ~{prefix}.vcf.gz
        tabix -p vcf ~{prefix}.vcf.gz
    >>>

    output {
        File vcf = "~{prefix}.vcf.gz"
        File tbi = "~{prefix}.vcf.gz.tbi"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          4,
        mem_gb:             24,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  1,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task MergeAndSortVCFs {
    meta {
        description: "Fast merging & sorting VCFs when the default sorting is expected to be slow"
    }

    input {
        Array[File] vcfs

        File ref_fasta_fai
        File? header_definitions_file

        String prefix

        RuntimeAttr? runtime_attr_override
    }
    parameter_meta {
        header_definitions_file: "a union of definition header lines for input VCFs (related to https://github.com/samtools/bcftools/issues/1629)"
    }

    Int sz = ceil(size(vcfs, 'GB'))
    Int disk_sz = if sz > 100 then 5 * sz else 375  # it's rare to see such large gVCFs, for now

    Boolean suspected_incomplete_definitions = defined(header_definitions_file)

    Int cores = 8

    # pending a bug fix (bcftools github issue 1576) in official bcftools release,
    # bcftools sort can be more efficient in using memory
    Int machine_memory = 48 # 96
    Int work_memory = ceil(machine_memory * 0.8)

    command <<<
        set -euxo pipefail

        echo ~{sep=' ' vcfs} | sed 's/ /\n/g' > all_raw_vcfs.txt

        echo "==========================================================="
        echo "starting concatenation" && date
        echo "==========================================================="
        bcftools \
            concat \
            --naive \
            --threads ~{cores-1} \
            -f all_raw_vcfs.txt \
            --output-type v \
            -o concatedated_raw.vcf.gz  # fast, at the expense of disk space
        for vcf in ~{sep=' ' vcfs}; do rm $vcf ; done

        # this is another bug in bcftools that's hot fixed but not in official release yet
        # (see bcftools github issue 1591)
        echo "==========================================================="
        echo "done concatenation, fixing header of naively concatenated VCF" && date
        echo "==========================================================="
        if ~{suspected_incomplete_definitions}; then
            # a bug from bcftools concat --naive https://github.com/samtools/bcftools/issues/1629
            set +e
            zgrep "^##" concatedated_raw.vcf.gz > header.txt
            grep -vF 'fileformat' header.txt \
                | grep -vF 'fileDate=' \
                | grep -vF 'source=' \
                | grep -vF 'contig' \
                | grep -vF 'ALT' \
                | grep -vF 'FILTER' \
                | grep -vF 'INFO' \
                | grep -vF 'FORMAT' \
                > tmp.others.txt
            touch tmp.other.txt
            set -e
            zgrep "^#CHROM" concatedated_raw.vcf.gz > tmp.sampleline.txt
            cat \
                ~{header_definitions_file} \
                tmp.others.txt \
                tmp.sampleline.txt \
                > fixed.header.txt
            rm -f tmp.*.txt && cat fixed.header.txt

            bcftools reheader \
                -h fixed.header.txt \
                -o tmp.wgs.vcf.gz \
                concatedated_raw.vcf.gz
            rm concatedated_raw.vcf.gz
        else
            mv concatedated_raw.vcf.gz tmp.wgs.vcf.gz
        fi
        bcftools reheader \
            --fai ~{ref_fasta_fai} \
            -o wgs_raw.vcf.gz \
            tmp.wgs.vcf.gz
        rm tmp.wgs.vcf.gz

        echo "==========================================================="
        echo "starting sort operation" && date
        echo "==========================================================="
        bcftools \
            sort \
            --temp-dir tm_sort \
            --output-type z \
            -o ~{prefix}.vcf.gz \
            wgs_raw.vcf.gz
        bcftools index --tbi --force ~{prefix}.vcf.gz
        echo "==========================================================="
        echo "done sorting" && date
        echo "==========================================================="
    >>>

    output {
        File vcf = "~{prefix}.vcf.gz"
        File tbi = "~{prefix}.vcf.gz.tbi"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          cores,
        mem_gb:             "~{machine_memory}",
        disk_gb:            disk_sz,
        boot_disk_gb:       10,
        preemptible_tries:  1,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task CollectDefinitions {
    meta {
        description: "Collect (union) various definitions in vcf files, adddressing a bcftols bug: https://github.com/samtools/bcftools/issues/1629"
    }
    input {
        Array[File] vcfs

        RuntimeAttr? runtime_attr_override
    }

    Int sz = ceil(size(vcfs, 'GB'))
    Int disk_sz = if sz > 100 then 5 * sz else 375

    command <<<
        set -euxo pipefail

        zgrep "^##" ~{vcfs[0]} > header.txt
        grep -F '##fileformat' header.txt > tmp.0.txt
        grep -F '##fileDate=' header.txt > tmp.1.txt
        if grep -q -F '##source=' header.txt; then grep -F 'source=' header.txt > tmp.2.txt; fi
        touch tmp.2.txt
        grep -F '##contig=' header.txt > tmp.3.txt

        cat tmp*txt > easy.txt && rm tmp*txt

        touch tmp.alt.txt tmp.ft.txt tmp.info.txt tmp.format.txt
        for vcf in ~{sep=' ' vcfs}; do
            zgrep -F '##ALT=' "${vcf}" >> tmp.alt.txt
            zgrep -F '##FILTER=' "${vcf}" >> tmp.ft.txt
            zgrep -F '##INFO=' "${vcf}" >> tmp.info.txt
            zgrep -F '##FORMAT=' "${vcf}" >> tmp.format.txt
        done
        for txt in tmp*txt; do
            sort "${txt}" | uniq > "${txt}.union"
        done
        cat tmp.alt.txt.union tmp.ft.txt.union tmp.info.txt.union tmp.format.txt.union > hard.txt
        cat easy.txt hard.txt > definitions.union.txt
    >>>

    output {
        File union_definitions = "definitions.union.txt"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          4,
        mem_gb:             16,
        disk_gb:            disk_sz,
        boot_disk_gb:       10,
        preemptible_tries:  1,
        max_retries:        0,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task GetVCFSampleName {
    meta {
        description: "Currently mostly used for extracting sample name in fingerprinting genotyped VCF"
    }
    input {
        File fingerprint_vcf
        RuntimeAttr? runtime_attr_override
    }

    parameter_meta {
        fingerprint_vcf: "Assumed to be genotyped, and hold only one sample (other samples will be ignored)."
    }

    command <<<
        set -eux

        GREPCMD="grep"
        if [[ ~{fingerprint_vcf} =~ \.gz$ ]]; then
            GREPCMD="zgrep"
        fi
        "${GREPCMD}" \
            "^#CHROM" \
            ~{fingerprint_vcf} \
            | awk '{print $10}' \
            > sample_name.txt
    >>>

    output {
        String sample_name = read_string("sample_name.txt")
    }

    ###################
    runtime {
        cpu: 2
        memory:  "4 GiB"
        disks: "local-disk 50 HDD"
        bootDiskSizeGb: 10
        preemptible_tries:     3
        max_retries:           2
        docker:"gcr.io/cloud-marketplace/google/ubuntu2004:latest"
    }
}

task SubsetVCF {
    input {
        File vcf_gz
        File vcf_tbi
        String locus
        String prefix = "subset"

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 2*ceil(size([vcf_gz, vcf_tbi], "GB")) + 1

    command <<<
        set -euxo pipefail

        bcftools view ~{vcf_gz} --regions ~{locus} | bgzip > ~{prefix}.vcf.gz
        tabix -p vcf ~{prefix}.vcf.gz
    >>>

    output {
        File subset_vcf = "~{prefix}.vcf.gz"
        File subset_tbi = "~{prefix}.vcf.gz.tbi"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             4,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        1,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-longshot:0.1.2"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task ZipAndIndexVCF {

    meta {
        description: "gZip plain text VCF and index it."
    }

    input {
        File vcf
        RuntimeAttr? runtime_attr_override
    }

    String prefix = basename(vcf, ".vcf")
    Int proposed_disk = 3*ceil(size(vcf, "GB")) + 1
    Int disk_size = if (proposed_disk > 100) then proposed_disk else 100

    command <<<
        cp ~{vcf} ~{prefix}.vcf && \
            bgzip -c ~{prefix}.vcf > ~{prefix}.vcf.gz && \
            tabix -p vcf ~{prefix}.vcf.gz && \
            find ./ -print | sed -e 's;[^/]*/;|____;g;s;____|; |;g'
    >>>

    output {
        File vcfgz = "~{prefix}.vcf.gz"
        File tbi = "~{prefix}.vcf.gz.tbi"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             3,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        2,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task IndexVCF {

    meta {
        description: "Indexing vcf.gz. Note: do NOT use remote index as that's buggy."
    }

    input {
        File vcf
        RuntimeAttr? runtime_attr_override
    }

    String prefix = basename(vcf, ".vcf.gz")
    Int proposed_disk = 3*ceil(size(vcf, "GB")) + 1
    Int disk_size = if (proposed_disk > 100) then proposed_disk else 100

    command <<<
        cp ~{vcf} ~{prefix}.vcf.gz && \
            tabix -p vcf ~{prefix}.vcf.gz && \
            find ./ -print | sed -e 's;[^/]*/;|____;g;s;____|; |;g'
    >>>

    output {
        File tbi = "~{prefix}.vcf.gz.tbi"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             3,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        2,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task FixSnifflesVCF {
    input {
        File vcf
        String sample_name
        File? ref_fasta_fai
        RuntimeAttr? runtime_attr_override
    }

    parameter_meta {
        sample_name:    "Sniffles infers sample name from the BAM file name, so we fix it here"
        ref_fasta_fai:  "provide only when the contig section of the input vcf is suspected to be corrupted"
    }

    Boolean fix_contigs = defined(ref_fasta_fai)

    Boolean vcf_is_bgzipped = sub(vcf, ".gz", "") != sub(vcf, ".vcf.gz", "")
    String local_raw = if vcf_is_bgzipped then "to.be.fixed.vcf.gz" else "to.be.fixed.vcf"
    String local_sp_fixed = if vcf_is_bgzipped then "sample.fixed.vcf.gz" else "sample.fixed.vcf"

    String initial_grep_cmd = if vcf_is_bgzipped then "zgrep" else "grep"

    String prefix = if vcf_is_bgzipped then basename(vcf, ".vcf.gz") else basename(vcf, ".vcf")
    Int proposed_disk = 3*ceil(size(vcf, "GB")) + 1
    Int disk_size = if (proposed_disk > 100) then proposed_disk else 100

    command <<<
        set -euxo pipefail

        # 1. fix sample information (Sniffles derives VCF SM information from the path to the BAM ......)
        cp ~{vcf} ~{local_raw}
        echo ~{sample_name} > sample_names.txt
        bcftools reheader --samples sample_names.txt -o ~{local_sp_fixed} ~{local_raw}
        rm ~{vcf} && rm ~{local_raw}

        ####################################################################
        # 2. prep for fixing undefined VCF INFO/FT/FORMAT, also guard against when the VCF is empty
        ~{initial_grep_cmd} "^##" ~{local_sp_fixed} > header.txt
        ~{initial_grep_cmd} -v "^#" ~{local_sp_fixed} > body.txt || true
        if [[ ! -f body.txt ]] || [[ ! -s body.txt ]]; then
            echo "input VCF seem to contain only header, but I'll proceed anyway and give you only header"
            bcftools \
                sort \
                --temp-dir tm_sort \
                --output-type z \
                -o ~{prefix}.vcf.gz \
                ~{local_sp_fixed}
            bcftools index --tbi --force ~{prefix}.vcf.gz
            exit 0;
        fi

        ####################################################################
        # 2.1. more prep for fixing undefined VCF INFO/FT/FORMATs
        # get FORMATs in header
        if grep -q -F '##FORMAT=<' header.txt; then
            grep -F '##FORMAT=<' header.txt | awk -F ',' '{print $1}' | sed 's/##FORMAT=<ID=//' | sort > formats.in_header.txt
        else
            touch formats.in_header.txt
        fi
        # get FILTERs in header
        if grep -q -F '##FILTER=<' header.txt; then
            grep -F '##FILTER=<' header.txt | awk -F ',' '{print $1}' | sed 's/##FILTER=<ID=//' | sort > filters.in_header.txt
        else
            touch filters.in_header.txt
        fi
        # get non-flag INFO in header
        if grep -q -F '##INFO=<' header.txt; then
            grep -F '##INFO=<' header.txt | grep -vF 'Type=Flag' | awk -F ',' '{print $1}' | sed 's/##INFO=<ID=//' | sort > non_flag_info.in_header.txt
        else
            touch non_flag_info.in_header.txt
        fi
        # get     flag INFO in header
        if grep -q -F '##INFO=<' header.txt; then
            grep -F '##INFO=<' header.txt | grep  -F 'Type=Flag' | awk -F ',' '{print $1}' | sed 's/##INFO=<ID=//' | sort >     flag_info.in_header.txt
        else
            touch flag_info.in_header.txt
        fi

        # get FORMATs in practice
        awk '{print $9}' body.txt | sort | uniq | sed 's/:/\n/g' | sort | uniq > formats.in_vcf.txt
        # get FILTERs in practice, guard against no 'PASS'
        awk '{print $7}' body.txt | sort | uniq | grep -v "^PASS$" > filters.in_vcf.txt || touch filters.in_vcf.txt

        awk '{print $8}' body.txt | sed 's/;/\n/g' > tmp.info.entries.txt
        if grep -q -F '=' tmp.info.entries.txt; then
            # get non-flag INFO in practicez
            grep -F '=' tmp.info.entries.txt | awk -F '=' '{print $1}' | sort | uniq > non_flag_info.in_vcf.txt
        fi
        if grep -q -vF '=' tmp.info.entries.txt; then
            # get     flag INFO in practice
            awk '{print $8}' body.txt | sed 's/;/\n/g' | grep -vF '=' | sort | uniq > flag_info.in_vcf.txt
        fi
        touch non_flag_info.in_vcf.txt
        touch     flag_info.in_vcf.txt

        echo "I survived. More to go..."

        ####################################################################
        # 2.2. more prep for fixing undefined VCF INFO/FT/FORMATs
        comm -13 formats.in_header.txt formats.in_vcf.txt > missing.formats.txt
        while IFS= read -r line
        do
        echo "##FORMAT=<ID=${line},Number=.,Type=String,Description=\"CALLER DID NOT DEFINE THIS.\">" >> missing.formats.header
        done < missing.formats.txt

        comm -13 filters.in_header.txt filters.in_vcf.txt > missing.filters.txt
        while IFS= read -r line
        do
        echo "##FILTER=<ID=${line},Description=\"CALLER DID NOT DEFINE THIS.\">" >> missing.filters.header
        done < missing.filters.txt

        comm -13 non_flag_info.in_header.txt non_flag_info.in_vcf.txt > missing.non_flag_info.txt
        while IFS= read -r line
        do
        echo "##INFO=<ID=${line},Number=.,Type=String,Description=\"CALLER DID NOT DEFINE THIS.\">" >> missing.non_flag_info.header
        done < missing.non_flag_info.txt

        comm -13 flag_info.in_header.txt flag_info.in_vcf.txt > missing.flag_info.txt
        while IFS= read -r line
        do
        echo "##INFO=<ID=${line},Number=0,Type=Flag,Description=\"CALLER DID NOT DEFINE THIS.\">" >> missing.flag_info.header
        done < missing.flag_info.txt

        ####################################################################
        # 2. actually fix undefined VCF INFO/FT/FORMATs, if necessary
        if  find . -maxdepth 1 -type f -name "missing.*.header" 2>/dev/null | grep -q .; then
            grep "^##" ~{local_sp_fixed} | grep -v "^##[A-Z]" | grep -vF 'contig=' > first_lines.txt
            grep -F "##contig=<ID=" header.txt > contigs.txt
            grep "^#CHROM" ~{local_sp_fixed} > sample.line.txt
            grep "^##" ~{local_sp_fixed} | grep "^##[A-Z]" | sort > existing_definitions.txt
            cat existing_definitions.txt missing.*.header | sort > everything.defined.txt
            cat first_lines.txt contigs.txt everything.defined.txt sample.line.txt > fixed.header.txt
            # print to stdout for checking
            grep -vF "##contig=<ID=" fixed.header.txt

            cat fixed.header.txt body.txt > fixed.vcf
            rm ~{local_sp_fixed}
        else
            mv ~{local_sp_fixed} fixed.vcf
        fi

        ####################################################################
        # 3. fix contigs undefined (in later stages)
        if ~{fix_contigs}; then
            bcftools reheader \
                --fai ~{ref_fasta_fai} \
                -o fixed.and_contigs.vcf \
                fixed.vcf
            mv fixed.and_contigs.vcf fixed.vcf
        fi

        ####################################################################
        # 4. fix occationally unsorted VCF
        bcftools \
            sort \
            --temp-dir tm_sort \
            --output-type z \
            -o ~{prefix}.vcf.gz \
            fixed.vcf
        bcftools index --tbi --force ~{prefix}.vcf.gz
    >>>

    output {
        File sortedVCF = "~{prefix}.vcf.gz"
        File tbi = "~{prefix}.vcf.gz.tbi"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             3,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        2,
        docker:             "us.gcr.io/broad-dsp-lrma/lr-basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

########################################################################################################################
########################################################################################################################
########################################################################################################################

task HardFilterVcf {

    input {
        File vcf
        File vcf_index

        String prefix

        # From WARP:
        # ExcessHet is a phred-scaled p-value. We want a cutoff of anything more extreme
        # than a z-score of -4.5 which is a p-value of 3.4e-06, which phred-scaled is 54.69
        Float excess_het_threshold = 54.69

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 1 + 4*ceil(size([vcf, vcf_index], "GB"))

    command <<<
        set -euo pipefail

        # Get amount of memory to use:
        mem_available=$(free -m | grep '^Mem' | awk '{print $2}')
        let mem_start=${mem_available}-1000
        let mem_max=${mem_available}-750

        gatk --java-options "-Xms${mem_start}m -Xmx${mem_max}m" \
            VariantFiltration \
            --filter-expression "ExcessHet > ~{excess_het_threshold}" \
            --filter-name ExcessHet \
            -V ~{vcf} \
            -O ~{prefix}.hard_filtered.vcf.gz
    >>>

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             4,
        disk_gb:            disk_size,
        boot_disk_gb:       15,
        preemptible_tries:  1,
        max_retries:        1,
        docker:             "us.gcr.io/broad-gatk/gatk:4.3.0.0"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }

    output {
        File variant_filtered_vcf = "~{prefix}.hard_filtered.vcf.gz"
        File variant_filtered_vcf_index = "~{prefix}.hard_filtered.vcf.gz.idx"
    }
}

task MakeSitesOnlyVcf {

    input {
        File vcf
        File vcf_index

        String prefix

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 1 + 4*ceil(size([vcf, vcf_index], "GB"))

    command <<<
        set -euo pipefail

        # Get amount of memory to use:
        mem_available=$(free -m | grep '^Mem' | awk '{print $2}')
        let mem_start=${mem_available}-1000
        let mem_max=${mem_available}-750

        gatk --java-options "-Xms${mem_start}m -Xmx${mem_max}m" \
            MakeSitesOnlyVcf \
            -I ~{vcf} \
            -O ~{prefix}.sites_only.vcf.gz
    >>>

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             4,
        disk_gb:            disk_size,
        boot_disk_gb:       15,
        preemptible_tries:  1,
        max_retries:        1,
        docker:             "us.gcr.io/broad-gatk/gatk:4.3.0.0"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }

    output {
        File sites_only_vcf = "~{prefix}.sites_only.vcf.gz"
        File sites_only_vcf_index = "~{prefix}.sites_only.vcf.gz.idx"
    }
}

task AnnotateVcfWithBedRegions {
    input {
        File vcf
        File vcf_index

        Array[File] bed_files
        Array[File] bed_file_indexes
        Array[String] bed_file_annotation_names

        String prefix

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 1 + 4*ceil(size([vcf, vcf_index, bed_files, bed_file_indexes], "GB"))



    command <<<
        set -euxo pipefail

        # Get amount of memory to use:
        mem_available=$(free -m | grep '^Mem' | awk '{print $2}')
        let mem_start=${mem_available}-1000
        let mem_max=${mem_available}-750

        # We need to generate argument strings from the input arrays.
        # First we check that the arrays are the same length:
        if [[ ~{length(bed_files)} -ne ~{length(bed_file_indexes)} ]] || \
           [[ ~{length(bed_files)} -ne ~{length(bed_file_annotation_names)} ]] || \
            echo "ERROR: Not all input arrays for known variants contain the same number of elements: " 1>&2
            echo "       bed_files                 = ~{length(bed_files)}" 1>&2
            echo "       bed_file_indices          = ~{length(bed_file_indexes)}" 1>&2
            echo "       bed_file_annotation_names = ~{length(bed_file_annotation_names)}" 1>&2
            false
        fi

        # Now we can write out the arrays into a TSV file and add them line by line to the execution:
        # Create the TSV:
        options_tsv=~{write_tsv(transpose([bed_files, bed_file_annotation_names]))}

        # Now we have to run `VariantFiltration` multiple times on its own output so that it can
        # annotate each region in the file:
        # NOTE: This is dumb, but must be done because the `--mask` and `--mask-name` inputs are not arrays.

        input_vcf=~{vcf}
        output_vcf=~{prefix}.intermediate.vcf.gz
        while read mask_options ; do

            bed_file=$(echo "${mask_options}" | awk -F'\t' '{print $1}')
            mask_name=$(echo "${mask_options}" | awk -F'\t' '{print $2}')

            echo -e "RUNNING GATK ON NEW MASK: ${mask_name}\t${bed_file}"

            gatk --java-options "-Xms${mem_start}m -Xmx${mem_max}m" \
                VariantFiltration \
                -V ${input_vcf} \
                -O ${output_vcf} \
                --mask ${bed_file} \
                --mask-name ${mask_name}

            mv ${output_vcf} ~{prefix}.new_input.vcf.gz
            input_vcf=~{prefix}.new_input.vcf.gz
        done < options_tsv

        mv ${output_vcf} ~{prefix}.vcf.gz
    >>>

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             4,
        disk_gb:            disk_size,
        boot_disk_gb:       15,
        preemptible_tries:  1,
        max_retries:        1,
        docker:             "us.gcr.io/broad-gatk/gatk:4.3.0.0"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }

    output {
        File annotated_vcf = "~{prefix}.vcf.gz"
        File annotated_vcf_index = "~{prefix}.vcf.gz.idx"
    }
}

task IndelsVariantRecalibrator {

    input {
        File vcf
        File vcf_index

        String prefix

        Array[String] recalibration_tranche_values
        Array[String] recalibration_annotation_values

        Array[File] known_reference_variants
        Array[File] known_reference_variants_index
        Array[String] known_reference_variants_identifier
        Array[Boolean] is_known
        Array[Boolean] is_training
        Array[Boolean] is_truth
        Array[Int] prior

        Boolean use_allele_specific_annotations
        Int max_gaussians = 4

        RuntimeAttr? runtime_attr_override
    }

    parameter_meta {
        vcf:   "Sites only VCF.  Can be pre-filtered using hard-filters."
        vcf_index: "Tribble Index for sites only VCF."
        known_reference_variants: "Array of known reference VCF files.  For humans, dbSNP is one example."
        known_reference_variants_index: "Array of index files for known reference VCF files."
        known_reference_variants_identifier: "Array of boolean values the identifier / name for the known_reference_variant file at the same array position.  Must be the same length as `known_reference_variants`."
        is_known: "Array of boolean values indicating if the known_reference_variant file at the same array position contains known variants.  Must be the same length as `known_reference_variants`."
        is_training: "Array of boolean values indicating if the known_reference_variant file at the same array position contains training data.  Must be the same length as `known_reference_variants`."
        is_truth: "Array of boolean values indicating if the known_reference_variant file at the same array position contains truth data.  Must be the same length as `known_reference_variants`."
        prior: "Array of integer values indicating the priors for the known_reference_variant file at the same array position.  Must be the same length as `known_reference_variants`."
    }


    Int disk_size = 10 + ceil(size(known_reference_variants, "GB"))
                  + 4*ceil(size(vcf, "GB"))
                  + 2*ceil(size(vcf_index, "GB"))

    command <<<
        set -euxo pipefail

        # We need to generate resource strings from the input arrays.
        # First we check that the arrays are the same length:
        if [[ ~{length(known_reference_variants)} -ne ~{length(known_reference_variants_identifier)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(known_reference_variants_index)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(is_known)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(is_training)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(is_truth)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(prior)} ]] ; then
            echo "ERROR: Not all input arrays for known variants contain the same number of elements: " 1>&2
            echo "       known_reference_variants            = ~{length(known_reference_variants)}" 1>&2
            echo "       known_reference_variants            = ~{length(known_reference_variants_index)}" 1>&2
            echo "       known_reference_variants_identifier = ~{length(known_reference_variants_identifier)}" 1>&2
            echo "       is_known                            = ~{length(is_known)}" 1>&2
            echo "       is_training                         = ~{length(is_training)}" 1>&2
            echo "       is_truth                            = ~{length(is_truth)}" 1>&2
            echo "       prior                               = ~{length(prior)}" 1>&2
            false
        fi

        # Now we can write out the arrays into a TSV file and add them line by line to the execution:
        # Create the TSV:
        options_tsv=~{write_tsv(transpose([known_reference_variants_identifier, is_known, is_training, is_truth, prior, known_reference_variants]))}

        # Now read them into a string:
        resource_flags=$(awk '{printf("--resource:%s,known=%s,training=%s,truth=%s,prior=%d %s ", $1, $2, $3, $4, $5, $6)}' ${options_tsv})

        # Get amount of memory to use:
        mem_available=$(free -g | grep '^Mem' | awk '{print $2}')
        let mem_start=${mem_available}-2
        let mem_max=${mem_available}-1

        gatk --java-options "-Xms${mem_start}g -Xmx${mem_max}g" \
            VariantRecalibrator \
                -V ~{vcf} \
                -O ~{prefix}.recal \
                --tranches-file ~{prefix}.tranches \
                --trust-all-polymorphic \
                -tranche ~{sep=' -tranche ' recalibration_tranche_values} \
                -an ~{sep=' -an ' recalibration_annotation_values} \
                ~{true='--use-allele-specific-annotations' false='' use_allele_specific_annotations} \
                -mode INDEL \
                --output-model ~{prefix}.model.report \
                --max-gaussians ~{max_gaussians} \
                ${resource_flags}
    >>>

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          2,
        mem_gb:             26,
        disk_gb:            disk_size,
        boot_disk_gb:       15,
        preemptible_tries:  1,
        max_retries:        1,
        docker:             "us.gcr.io/broad-gatk/gatk:4.3.0.0"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }

    output {
        File recalibration = "~{prefix}.recal"
        File recalibration_index = "~{prefix}.recal.idx"
        File tranches = "~{prefix}.tranches"
        File model_report = "~{prefix}.model.report"
    }
}

task SNPsVariantRecalibratorCreateModel {

    input {
        File vcf
        File vcf_index

        String prefix

        Array[String] recalibration_tranche_values
        Array[String] recalibration_annotation_values

        Array[File] known_reference_variants
        Array[File] known_reference_variants_index
        Array[String] known_reference_variants_identifier
        Array[Boolean] is_known
        Array[Boolean] is_training
        Array[Boolean] is_truth
        Array[Int] prior

        Int? downsampleFactor

        Boolean use_allele_specific_annotations
        Int max_gaussians = 6

        RuntimeAttr? runtime_attr_override
    }

    parameter_meta {
        vcf:   "Sites only VCF.  Can be pre-filtered using hard-filters."
        vcf_index: "Tribble Index for sites only VCF."
        known_reference_variants: "Array of known reference VCF files.  For humans, dbSNP is one example."
        known_reference_variants_index: "Array of index files for known reference VCF files."
        known_reference_variants_identifier: "Array of boolean values the identifier / name for the known_reference_variant file at the same array position.  Must be the same length as `known_reference_variants`."
        is_known: "Array of boolean values indicating if the known_reference_variant file at the same array position contains known variants.  Must be the same length as `known_reference_variants`."
        is_training: "Array of boolean values indicating if the known_reference_variant file at the same array position contains training data.  Must be the same length as `known_reference_variants`."
        is_truth: "Array of boolean values indicating if the known_reference_variant file at the same array position contains truth data.  Must be the same length as `known_reference_variants`."
        prior: "Array of integer values indicating the priors for the known_reference_variant file at the same array position.  Must be the same length as `known_reference_variants`."
    }

    Int disk_size = 10 + ceil(size(known_reference_variants, "GB"))
              + 4*ceil(size(vcf, "GB"))
              + 2*ceil(size(vcf_index, "GB"))

    String downsample_factor_arg = if defined(downsampleFactor) then " --sample-every-Nth-variant " else ""

    command <<<
        set -euxo pipefail

        # We need to generate resource strings from the input arrays.
        # First we check that the arrays are the same length:
        if [[ ~{length(known_reference_variants)} -ne ~{length(known_reference_variants_identifier)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(known_reference_variants_index)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(is_known)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(is_training)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(is_truth)} ]] || \
           [[ ~{length(known_reference_variants)} -ne ~{length(prior)} ]] ; then
            echo "ERROR: Not all input arrays for known variants contain the same number of elements: " 1>&2
            echo "       known_reference_variants            = ~{length(known_reference_variants)}" 1>&2
            echo "       known_reference_variants            = ~{length(known_reference_variants_index)}" 1>&2
            echo "       known_reference_variants_identifier = ~{length(known_reference_variants_identifier)}" 1>&2
            echo "       is_known                            = ~{length(is_known)}" 1>&2
            echo "       is_training                         = ~{length(is_training)}" 1>&2
            echo "       is_truth                            = ~{length(is_truth)}" 1>&2
            echo "       prior                               = ~{length(prior)}" 1>&2
            false
        fi

        # Now we can write out the arrays into a TSV file and add them line by line to the execution:
        # Create the TSV:
        options_tsv=~{write_tsv(transpose([known_reference_variants_identifier, is_known, is_training, is_truth, prior, known_reference_variants]))}

        # Now read them into a string:
        resource_flags=$(awk '{printf("--resource:%s,known=%s,training=%s,truth=%s,prior=%d %s ", $1, $2, $3, $4, $5, $6)}' ${options_tsv})

        # Get amount of memory to use:
        mem_available=$(free -g | grep '^Mem' | awk '{print $2}')
        let mem_start=${mem_available}-2
        let mem_max=${mem_available}-1

        gatk --java-options "-Xms${mem_start}g -Xmx${mem_max}g" \
            VariantRecalibrator \
                -V ~{vcf} \
                -O ~{prefix}.recal \
                --tranches-file ~{prefix}.tranches \
                --trust-all-polymorphic \
                -tranche ~{sep=' -tranche ' recalibration_tranche_values} \
                -an ~{sep=' -an ' recalibration_annotation_values} \
                ~{true='--use-allele-specific-annotations' false='' use_allele_specific_annotations} \
                -mode SNP \
                ~{downsample_factor_arg}~{default="" sep=" --sample-every-Nth-variant " downsampleFactor} \
                --output-model ~{prefix}.model.report \
                --max-gaussians ~{max_gaussians} \
                ${resource_flags}
    >>>

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          2,
        mem_gb:             64,
        disk_gb:            disk_size,
        boot_disk_gb:       15,
        preemptible_tries:  1,
        max_retries:        1,
        docker:             "us.gcr.io/broad-gatk/gatk:4.3.0.0"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }

    output {
        File recalibration = "~{prefix}.recal"
        File recalibration_index = "~{prefix}.recal.idx"
        File tranches = "~{prefix}.tranches"
        File model_report = "~{prefix}.model.report"
    }
}

task ApplyVqsr {

    input {
        File vcf
        File vcf_index

        String prefix

        File snps_recalibration
        File snps_recalibration_index
        File snps_tranches
        Float snp_filter_level

        File indels_recalibration
        File indels_recalibration_index
        File indels_tranches
        Float indel_filter_level

        Boolean use_allele_specific_annotations

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 10 + ceil(size([vcf, vcf_index], "GB"))
          + 2*ceil(size([snps_recalibration, snps_recalibration_index, snps_tranches], "GB"))
          + 2*ceil(size([indels_recalibration, indels_recalibration_index, indels_tranches], "GB"))

    command <<<
        set -euxo pipefail

        # Get amount of memory to use:
        mem_available=$(free -m | grep '^Mem' | awk '{print $2}')
        let mem_start=${mem_available}-2000
        let mem_max=${mem_available}-500

        gatk --java-options "-Xms${mem_start}m -Xmx${mem_max}m" \
            ApplyVQSR \
                -V ~{vcf} \
                -O tmp.indel.recalibrated.vcf.gz \
                --recal-file ~{indels_recalibration} \
                ~{true='--use-allele-specific-annotations' false='' use_allele_specific_annotations} \
                --tranches-file ~{indels_tranches} \
                --truth-sensitivity-filter-level ~{indel_filter_level} \
                --create-output-variant-index true \
                -mode INDEL

        gatk --java-options "-Xms${mem_start}m -Xmx${mem_max}m" \
            ApplyVQSR \
                -V tmp.indel.recalibrated.vcf.gz \
                -O ~{prefix}.recalibrated.vcf.gz \
                --recal-file ~{snps_recalibration} \
                ~{true='--use-allele-specific-annotations' false='' use_allele_specific_annotations} \
                --tranches-file ~{snps_tranches} \
                --truth-sensitivity-filter-level ~{snp_filter_level} \
                --create-output-variant-index true \
                -mode SNP
    >>>

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          1,
        mem_gb:             7,
        disk_gb:            disk_size,
        boot_disk_gb:       15,
        preemptible_tries:  1,
        max_retries:        1,
        docker:             "us.gcr.io/broad-gatk/gatk:4.3.0.0"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " LOCAL"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }

    output {
        File recalibrated_vcf = "~{prefix}.recalibrated.vcf.gz"
        File recalibrated_vcf_index = "~{prefix}.recalibrated.vcf.gz.idx"
    }
}
