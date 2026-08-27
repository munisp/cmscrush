# CRUSH Healthcare Integrity Platform

## Slide 1 — From suspicious healthcare activity to defensible evidence
CRUSH is a healthcare program-integrity platform for claims, enrollment, provider networks, clinical documents, beneficiary contact, Medicaid/CHIP, and Marketplace integrity. The platform prioritizes suspicious patterns while preserving human due process.

## Slide 2 — Nine planes, two directions of control
The nine planes flow upward from ingestion through streaming, lakehouse, entity/graph, features, models, decision, case/action, and governance. Data and evidence move upward; policy, provenance, fairness, and release gates move downward.

## Slide 3 — RFI coverage map
The platform addresses the RFI’s A–M topics: program-integrity authority/processes; identity/ownership; preclusion and MA enrollment; labs/genetic/molecular testing; DMEPOS; Parts A/B claims; MA coding and hospital billing; solicitation/contact; surety bonds; Medicaid/CHIP; state operations; and FFE/SBE integrity.

## Slide 4 — Open-source model portfolio
Deterministic rules and LightGBM/PyOD form the availability-safe baseline. MedSpaCy/cTAKES handle clinical context; PaddleOCR/docTR handle documents; Whisper handles consented contact audio; PyTorch Geometric/DGL handle provider-network graphs; Qwen3-8B or Mistral-7B-Instruct provide constrained evidence assistance.

## Slide 5 — Real-time claims path
Source adapters emit canonical events to Kafka. Flink adds event-time watermarks, deduplication, CEP windows, filing/deadline checks, and materialized feature deltas. The Go service consumes bounded, point-in-time features and returns typed recommendations or abstention.

## Slide 6 — Delta Lake medallion and graph
Bronze preserves encrypted source envelopes and hashes. Silver normalizes claims, entities, edges, and lineage. Gold stores point-in-time features, model assessments, graph snapshots, and evidence packs. Spark/Sedona produces historical and geographic features; DataFusion serves bounded read-only analytics.

## Slide 7 — Clinical NLP and evidence gates
Documents pass through classification, OCR/layout, coordinate-anchored text, quality checks, PHI minimization, terminology constraints, retrieval-grounded generation, and human review. Six gates require source spans, active vocabulary, citations, disagreement routing, calibrated abstention, and symmetric findings.

## Slide 8 — GNN provider-network analytics
Ray trains temporal heterogeneous GNNs using point-in-time graph snapshots. PyG is the primary implementation and DGL is a challenger. The online service returns bounded scores, uncertainty, graph snapshot/model versions, top-k evidence subgraphs, and abstention—never an accusation or action command.

## Slide 9 — Kubernetes deployment blueprint
Flink runs isolated application clusters with durable checkpoints/savepoints and one slot per TaskManager as an initial policy-critical profile. Ray separates CPU/GPU training workers from online inference. Clinical NLP and GNN services run non-root with PHI logging disabled and financial egress denied.

## Slide 10 — Governance, human action, and delivery status
Temporal owns reviewer identity, timers, notice, correction, and appeal. Models begin in shadow/challenger mode. TigerBeetle and Mojaloop, if enabled, are downstream authorized accounting/settlement boundaries, never model authorities. Helm and client-side manifest validation passed; production operators, images, secrets, storage, and GPU nodes remain environment-specific prerequisites.

## References

[1]: https://www.federalregister.gov/documents/2026/02/27/2026-03968/request-for-information-rfi-related-to-comprehensive-regulations-to-uncover-suspicious-healthcare "CMS CRUSH RFI"
[2]: https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files "CMS SynPUFs"
[3]: https://github.com/pyg-team/pytorch_geometric "PyTorch Geometric"
[4]: https://github.com/mindee/doctr "docTR"
[5]: https://github.com/openai/whisper "Whisper"
