# Resume

## First Last
AI & Data Engineer

---

### Professional Summary
Software Engineer specializing in AI and data platforms, with a proven track record of designing and delivering production‑scale data pipelines, analytics platforms, and LLM‑powered cloud‑native applications. Experienced in architecture ownership, performance optimization, cost reduction, and building reliable distributed systems across banking and enterprise environments.

---

### Certifications
- AWS Certified Cloud Practitioner
- Databricks Generative AI
- Orange Belt – Secure Code Warrior

---

### Experience

#### Redcat Technology – Software Engineer (AI & Data)  
**FEB 2026 – Present**  
- Architected and delivered a CDC data platform from scratch (MariaDB → AWS DMS → S3 Iceberg → Glue ETL → Redshift Serverless) processing 1.5 M POS events/day across 300+ tenants and 5,000 sites.  
- Replaced $60 K/year read replicas with a $6 K/year platform, saving $54 K AUD annually.  
- Designed and automated a reporting workflow, cutting manual effort from 2.25 days to 15 minutes per report (~99.5 % reduction).  
- Led vendor selection ADR evaluating Redshift Serverless, Snowflake, and Databricks; completed a 12‑database CDC readiness audit; final architecture adopted as the engineering standard.  
- Diagnosed and resolved critical data reliability issues, including a silent AWS DMS replication failure affecting 26 M of 127 M rows and MariaDB cascade‑delete gaps causing duplicated sales figures in production dashboards.  
- Improved development cycle time by rolling out AI‑assisted engineering workflows using automated hooks, skills, and standardized ticket lifecycles.  
- Evaluated BI tooling (QuickSight, Superset, Metabase) against 22 weighted requirements for a platform scaling to 270 K sessions/month.

#### Macquarie Bank – Software Engineer (Payments)  
**JUL 2023 – FEB 2026**  
- Reduced annual cloud‑security billing by over 50 % by implementing managed K8s workloads for secret vaults.  
- Reduced observability costs by 9 % by optimizing log ingestion efficiency.  
- Reduced MTTR by 40 % for change‑related incidents by implementing automated release verification & SLIs.  
- Optimized API performance by 25 ms by trimming payloads and designing a performance test suite.  
- Built production SLI dashboards, defined metrics & alerting, and wrote runbooks.  
- Upgraded CI/CD to Bitbucket Cloud and increased SonarQube coverage to >80 %.

#### National Australia Bank – Software Engineer (Business Banking)  
**JUL 2022 – JUL 2023**  
- Led a Spring Boot upgrade, reducing critical vulnerabilities by over 85 % and introducing Ivy automation for smoother dependency upgrades; recognized as Secure Code Champion.  
- Enhanced database reliability via PostgreSQL failover testing and Flyway‑based rollback mechanisms.

---

### Technical Skill Set

| Category        | Tools & Technologies |
|-----------------|----------------------|
| **Languages**   | SQL (4 yr), Python (4 yr), Java (4 yr) |
| **Data & AI**   | Spark, Presto, Data lakes & warehouses, ETL, AWS Redshift, dbt, Step Functions |
| **Cloud**       | AWS (EC2, S3, Lambda, SQS, SNS, RDS, Athena), Google Cloud (3 yr) |
| **Frameworks**  | Spring Boot, Flask |
| **Messaging**   | IBM MQ, JMS, Solace |
| **Infra**       | IaC (Terraform), Docker, Kubernetes, Bitbucket Cloud CI/CD, Jenkins |
| **Databases**   | PostgreSQL, Neo4j, MongoDB, MariaDB |
| **Observability** | Sumo Logic, Dynatrace, CloudWatch |
| **Testing/QA**  | JUnit, PyTest, Locust |

---

### Education

- **Monash University** – Bachelor of Software Engineering (Honours) – Dean’s Honours List  
- **Co‑author** – *‘CRAFTER: A Persona Generation Tool for Requirements Engineering’* (presented at ENASE 2024)  
- **Deakin University** – Master of Artificial Intelligence (2026 – 2027)

---

### Selected Projects

#### Monash University – Socratic Coach AI (FEB – NOV 2024)
- Built a conversational system using LLM APIs with structured backend logic for session & context management.  
- Developed semantic search & Retrieval‑Augmented Generation (RAG) using embeddings and a Neo4j knowledge graph to improve response relevance.  
- Designed backend data flow for real‑time interaction and response generation.  
- Implemented an API integration layer for scalable LLM request handling and orchestration.

#### SecureMail AI – Phishing & Fraud Classification (FEB – JUN 2025)
- Designed and implemented an ETL pipeline to ingest, clean, and transform multi‑source email and web data for downstream analytics and modeling.  
- Developed and evaluated multiple classification models for phishing and fraud detection.  
- Applied unsupervised learning to group and analyse large‑scale unstructured text data.