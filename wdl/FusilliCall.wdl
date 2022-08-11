version 1.0

import "tasks/Fusilli.wdl" as Fusilli

workflow FusilliCall {
    input {
        String fusilli_db_gcs
        Array[String] ref_ids
        Array[File] ref_fastas
        Array[File] ref_gffs

        String fusilli_run_gcs
        Array[String] sample_ids
        File? hmm_config
    }

    # Perform Tesserae2 alignment across multiple shards (by default 1000 per worker)
    scatter(sample_id in sample_ids) {
        String sample_contigs = fusilli_run_gcs + "/" + sample_id + "/" + sample_id + ".contigs.fa"

        call Fusilli.ChunkSampleContigs as ChunkSampleContigs {
            input:
                sample_contigs = sample_contigs
        }

        scatter(chunk in ChunkSampleContigs.chunks) {
            call Fusilli.TesseraeAlign as TesseraeAlign {
                input:
                    sample_id = sample_id,
                    sample_contigs = chunk,
                    fusilli_run_gcs = fusilli_run_gcs,
                    hmm_config = hmm_config
            }
        }

        call Fusilli.InferGenomeCoords as InferGenomeCoords {
            input:
                fusilli_db_gcs = fusilli_db_gcs,
                ref_ids = ref_ids,
                ref_fastas = ref_fastas,

                fusilli_run_gcs = fusilli_run_gcs,
                sample_id = sample_id,
                aligned_sample_contigs = flatten(TesseraeAlign.alignments)
        }
    }
}
