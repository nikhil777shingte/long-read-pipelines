version 1.0

import "../Structs.wdl"

task Panaroo {
    input {
        Array[File] input_gffs
        String clean_mode = "sensitive"
        Boolean clean_edges = false

        RuntimeAttr? runtime_attr_override
    }

    RuntimeAttr default_attr = object {
        cpu_cores:          4,
        mem_gb:             32,
        disk_gb:            200,
        boot_disk_gb:       10,
        preemptible_tries:  1,
        max_retries:        2,
        docker:             "quay.io/biocontainers/panaroo:1.3.2--pyhdfd78af_0"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])

    Int num_cpu = select_first([runtime_attr.cpu_cores, default_attr.cpu_cores])

    command <<<
        mkdir output
        panaroo -i ~{write_lines(input_gffs)} ~{true="" false="--no_clean_edges" clean_edges} --clean-mode ~{clean_mode} \
            -t ~{num_cpu} --remove-invalid-genes -o output/
    >>>

    output {
        File summary_stats = "output/summary_statistics.txt"
        File final_graph = "output/final_graph.gml"
        File pre_filt_graph = "output/pre_filt_graph.gml"
        File pangenome_reference = "output/pangenome_reference.fa"

        File gene_data = "output/gene_data.csv"
        File gene_presence_absence_rtab = "output/gene_presence_absence.Rtab"
        File gene_presence_absence_csv = "output/gene_presence_absence.csv"
        File gene_presence_absence_roary = "output/gene_presence_absence_roary.csv"

        File combined_DNA_CDS = "output/combined_DNA_CDS.fasta"
        File combined_protein_CDS = "output/combined_protein_CDS.fasta"
        File combined_protein_cdhit_out = "output/combined_protein_cdhit_out.txt"
        File combined_protein_cdhit_out_cluster = "output/combined_protein_cdhit_out.txt.clstr"

        File sv_triplets_presence_absence = "output/struct_presence_absence.Rtab"
    }
}
