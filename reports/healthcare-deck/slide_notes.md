# 1 - CRUSH Healthcare Integrity Platform

Welcome to the architecture briefing for the CRUSH Healthcare Integrity Platform. Today we are examining an evidence-first approach to program integrity that spans claims, enrollment, and provider networks.

# 2 - Healthcare Integrity Requires Evidence, Not Scores

Healthcare integrity requires immutable evidence rather than opaque risk scores. We use models to prioritize and explain anomalies, but never to make final adverse determinations. And that separation of AI from executive action protects both program integrity and due process. Building on this core principle, let us examine how the underlying architecture maintains this separation from data ingestion to final human review.

# 3 - Nine Planes Connect Data to Due Process

Moving from raw data to due process requires a structured architectural topology. Nine distinct planes connect every incoming record to a verifiable human decision. And data flows upward as versioned facts while controls flow downward to enforce strict tenancy and governance. With this nine-plane framework established, we can see how it satisfies comprehensive regulatory requirements.

# 4 - One Platform Covers the RFI A–M Topics

Compliance with federal and state program integrity mandates requires complete traceability across every operational domain. A single unified platform maps all required RFI topics directly to specific data products and review workflows. And this end-to-end mapping ensures no regulatory requirement remains unaddressed. Now let us look at how real-time data flows through the streaming backbone.

# 5 - Flink Turns Claims Streams into Timely Signals

High-volume claims processing requires immediate anomaly detection without sacrificing architectural safety. Apache Flink turns raw event streams into timely velocity and geospatial signals using event-time watermarks. But remember that if streaming state degrades, the system safely falls back to deterministic rules rather than relying on stale model output.

# 6 - Delta Lake Preserves Lineage Across Medallion Layers

We build our data architecture around a medallion pattern to ensure every analytical artifact has an unbroken chain of custody. Bronze stores raw, immutable payloads with cryptographic hashes, while silver normalizes codes and resolves identities. Gold then packages point-in-time features for decision-making. By leveraging Delta Lake change data feeds, we prevent data leakage across training runs and maintain strict data-use controls for PHI. This clear separation of layers guarantees that downstream AI and business rules always operate on verified, auditable truth.

# 7 - Clinical AI Is Gated by Source Evidence

Clinical artificial intelligence cannot operate as a black box when healthcare benefits are at stake. We enforce six mandatory hallucination-control gates before any model recommendation reaches a reviewer. Every extracted span must have a verbatim source citation, and outputs are strictly constrained to approved medical codes. If documentation is ambiguous, the system must abstain rather than guess, ensuring human investigators remain the ultimate arbiters. Building on our medallion lineage, these strict evidence gates guarantee that clinical intelligence is always bound to verifiable source documents.

# 8 - GNNs Reveal Temporal Provider-Network Patterns

To uncover sophisticated billing rings and referral anomalies, we deploy graph neural networks using Ray and PyTorch Geometric. These models analyze temporal patterns across providers, beneficiaries, and facilities using point-in-time graph snapshots. Because graph outputs are strictly advisory, they feed directly into the Go decision service without triggering automated adverse actions. Transitioning from document-level extraction to network-wide patterns, this graph intelligence scales our ability to detect coordinated fraud while preserving human due process.

# 9 - Kubernetes Separates Streaming, Training, and Review

We rely on Kubernetes to isolate distinct operational workloads across the platform. Flink application mode handles real-time claims streams with exact-once semantics, while ephemeral Ray jobs manage resource-intensive GNN training. Inference gateways run with hardened security constraints, including read-only root filesystems and disabled financial egress. Building on our graph analytics layer, this infrastructure segregation ensures that streaming telemetry, model training, and review workflows never compromise one another.

# 10 - Governance Makes the Platform Defensible

Defensibility requires more than high-performing models; it demands uncompromising governance and verifiable human oversight. Temporal workflows enforce statutory timers and require explicit reviewer rationale before any adverse action occurs. Continuous drift monitoring and immutable provenance linking ensure every decision can be replayed and audited. Moving forward from our Kubernetes deployment, these governance pillars guarantee that artificial intelligence prioritizes the work while authorized humans retain absolute control.
