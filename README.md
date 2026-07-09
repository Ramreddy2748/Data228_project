# 🚕 NYC Taxi Big Data Streaming Project

> End‑to‑end big data pipeline combining **historical batch processing** (2024) and **real-time streaming** (2025+) with advanced algorithms for duplicate detection, trend analysis, and similarity search.

![Python](https://img.shields.io/badge/Python-3.x-blue?style=flat-square)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-batch%20%26%20ground%20truth-orange?style=flat-square)
![Kafka](https://img.shields.io/badge/Kafka-streaming-red?style=flat-square)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Redshift-yellow?style=flat-square)

---

## 📑 Table of Contents

- [🎯 Quick Start](#-quick-start)
- [⚙️ Architecture Overview](#-architecture-overview)
- [🛠️ Tech Stack](#-tech-stack)
- [📁 Project Structure](#-project-structure)
- [📊 Key Components](#-key-components)
- [🔍 Data Dictionary](#-data-dictionary)
- [🚀 How to Run](#-how-to-run)
- [🧪 Algorithms & Techniques](#-algorithms--techniques)
- [📚 Documentation](#-documentation)
- [📝 For Reviewers](#-for-reviewers)

---

## 🎯 Quick Start

### Prerequisites
- **Python 3.x** with `pandas`, `pyarrow`, `pyspark`, `boto3`, Kafka client libs
- **AWS S3** access (S3 bucket for data storage)
- **AWS Redshift** (optional, for warehouse queries)
- **Apache Kafka** (optional, for streaming)

### 5-Minute Setup
```bash
# 1. Install dependencies
pip install pandas pyarrow pyspark boto3 confluent-kafka

# 2. Configure AWS credentials
aws configure

# 3. Run local data cleaning
jupyter notebook ETL/analyze_and_clean_Local.ipynb

# 4. Explore the algorithms
python algorithms/bloom_filter/bloom_filter_demo.py
```

---

## ⚙️ Architecture Overview

### Two-Phase Pipeline

```
Historical Phase (2024)           Streaming Phase (2025+)
        ↓                                ↓
   Raw Data                         Kafka Topics
        ↓                                ↓
   ETL/Clean                       Algorithms
        ↓                          (Bloom/DGIM/LSH)
   S3 Storage                            ↓
        ↓                           S3 Results
   Redshift (Staging)                    ↓
        ↓                          Redshift (Prod)
  Ground Truth                           ↓
    (Baselines)                    QuickSight Dashboards
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Storage** | AWS S3 | Raw, cleaned, and production datasets |
| **Warehouse** | AWS Redshift | Query engine on top of S3 |
| **Batch Processing** | Apache Spark | Ground truth jobs & validation |
| **Streaming** | Kafka | Continuous event stream (2025+) |
| **Algorithms** | Python | Bloom Filter, DGIM, LSH implementations |
| **Visualization** | QuickSight | BI dashboards & monitoring |
| **Privacy** | k-Anonymity, DP | Data privacy demos & techniques |

---

## 📁 Project Structure

```
Data_project/
├── ETL/                          # Data ingestion & cleaning
│   ├── analyze_and_clean_Local.ipynb       # Local development
│   └── lambda/                             # AWS Lambda pipeline
├── algorithms/                   # Core streaming algorithms
│   ├── bloom_filter/             # Duplicate detection
│   ├── DGIM/                     # Sliding window counting
│   └── LSH/                      # Similarity search
├── spark-jobs_groundTruth/       # Spark ground truth jobs
│   ├── ground_truth_basic_stats.py
│   ├── ground_truth_duplicates.py
│   └── GROUND_TRUTH_JOBS.md
├── privacy/                      # Privacy & responsible AI
│   ├── privacy_demo.py
│   └── privacy_techniques.py
├── validation/                   # Data validation
│   ├── data_validation.ipynb
│   └── ground_truth_baseline.ipynb
├── data_dictionary/              # Schema definitions
│   ├── data_dictionary_Master_Summary.csv
│   ├── data_dictionary_canonical_columns.csv
│   └── data_dictionary_Data_Mapping.csv
├── graphs/                       # All visualizations
│   ├── tech_design/              # Architecture diagrams
│   ├── bloomFilter_graph/        # Bloom filter analysis
│   ├── dgim_graph/               # DGIM results
│   ├── lsh_graph/                # LSH efficiency
│   └── privacy_graphs/           # Privacy techniques
├── Guide/                        # Documentation
│   ├── Design_Document.md        # Architecture & design
│   ├── Implementation_Guide.md   # Step-by-step runbook
│   └── INSIGHTS_GUIDE.md         # Key findings
└── README.md                     # This file
```

---

## 📊 Key Components

### Historical Phase (Batch) – Through 2024

| Step | What | Where | Output |
|------|------|-------|--------|
| **Ingest** | NYC taxi data (yellow, green, FHV, FHVHV) | Public sources | Raw datasets |
| **Clean** | Normalize types, remove duplicates, standardize | `ETL/` | Parquet files |
| **Store** | Write cleaned data | AWS S3 | S3 staging paths |
| **Build Baselines** | Compute ground truth metrics | Spark jobs | Reference data |

### Streaming Phase (Online) – 2025+

| Algorithm | Purpose | Input | Output |
|-----------|---------|-------|--------|
| **Bloom Filter** | Flag likely duplicates | Kafka stream + historical reference | Duplicate flags |
| **DGIM** | Approximate recent-window counts | Stream of trip counts | Trend estimates |
| **LSH** | Find similar trips/routes | Kafka stream + LSH index | Similar trip matches |

---

## 🔍 Data Dictionary

The **canonical schema** is maintained in `data_dictionary/` with Git-friendly CSV files:

| File | Purpose |
|------|---------|
| `data_dictionary_Master_Summary.csv` | Overview of all fields & usage |
| `data_dictionary_canonical_columns.csv` | Column names, types, meanings |
| `data_dictionary_canonical_availability_by_type_.csv` | Which fields exist in yellow/green/FHV/FHVHV |
| `data_dictionary_Data_Mapping.csv` | Raw → canonical column mapping |
| `data_dictionary_Cleanng_validation.csv` | Validation rules & cleaning logic |

These are cited in the report for **schema design**, **ETL rules**, and **data quality**.

---

## 🚀 How to Run

See [`Guide/Implementation_Guide.md`](Guide/Implementation_Guide.md) for detailed step-by-step instructions.

### Phase 1: ETL & Ground Truth (Local or Lambda)

```bash
# 1. Clean historical data
jupyter notebook ETL/analyze_and_clean_Local.ipynb

# 2. Compute ground truth with Spark
cd spark-jobs_groundTruth/
spark-submit ground_truth_basic_stats.py
spark-submit ground_truth_duplicates.py
```

### Phase 2: Run Algorithms

```bash
# 3. Test algorithms offline
python algorithms/bloom_filter/bloom_filter_demo.py
python algorithms/DGIM/dgim_simulator.py
python algorithms/LSH/lsh_similarity.py

# 4. Generate plots
python graphs/display_basic_stats.py
```

### Phase 3: Streaming (with Kafka)

```bash
# 5. Start Kafka broker & topics
# ... (see Implementation_Guide.md)

# 6. Run streaming consumers
python algorithms/bloom_filter/streaming_consumer.py
python algorithms/DGIM/streaming_consumer.py
python algorithms/LSH/streaming_consumer.py
```

### Phase 4: Visualize Results

```bash
# 7. Build dashboards
# Use QuickSight on Redshift prod tables
# Or view static reports in graphs/
```

---

## 🧪 Algorithms & Techniques

### Core Streaming Algorithms

| Algorithm | Technique | Use Case | Course Topic |
|-----------|-----------|----------|--------------|
| **Bloom Filter** | Probabilistic data structure | Duplicate detection in stream | Streaming algorithms |
| **DGIM** | Sliding window sketch | Approximate recent counts | Data stream mining |
| **LSH** | Locality sensitive hashing | Fast similarity search | Similarity search |

### Supporting Techniques

- **Ground Truth Computation**: Spark jobs for baseline metrics
- **Privacy**: k-Anonymity generalization, differential privacy noise addition
- **Fairness & Explainability**: Analyze flag rates by taxi type/borough
- **Data Quality**: Validation rules & spike detection

---

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| [`Guide/Design_Document.md`](Guide/Design_Document.md) | Architecture, design choices, trade-offs |
| [`Guide/Implementation_Guide.md`](Guide/Implementation_Guide.md) | Step-by-step runbook & setup instructions |
| [`Guide/INSIGHTS_GUIDE.md`](Guide/INSIGHTS_GUIDE.md) | Key findings & experimental results |
| [`spark-jobs_groundTruth/GROUND_TRUTH_JOBS.md`](spark-jobs_groundTruth/GROUND_TRUTH_JOBS.md) | Spark job reference |

### Visualization Galleries

| Folder | Contains |
|--------|----------|
| `graphs/tech_design/` | Architecture & pipeline diagrams (3 views) |
| `graphs/bloomFilter_graph/` | Bloom filter analysis & use cases |
| `graphs/dgim_graph/` | DGIM performance & accuracy |
| `graphs/lsh_graph/` | LSH efficiency & S-curves |
| `graphs/privacy_graphs/` | Privacy technique demonstrations |

---

## 📝 For Reviewers

**Start here** for understanding the full project:

1. **Code Overview**  
   - Read [`Guide/Design_Document.md`](Guide/Design_Document.md)  
   - Browse [`algorithms/`](algorithms/) implementations

2. **Data Quality**  
   - See [`data_dictionary/`](data_dictionary/) CSVs for schema & mappings  
   - Check [`validation/`](validation/) for data quality reports

3. **Streaming Algorithms**  
   - Study [`algorithms/bloom_filter/`](algorithms/bloom_filter/), `DGIM/`, `LSH/`  
   - Review [`spark-jobs_groundTruth/`](spark-jobs_groundTruth/) for ground truth

4. **Visualizations**  
   - View diagrams in [`graphs/tech_design/`](graphs/tech_design/) (simple, detailed, timeline views)  
   - See algorithm analysis in [`graphs/bloomFilter_graph/`](graphs/bloomFilter_graph/), `dgim_graph/`, `lsh_graph/`

5. **Privacy & Responsible AI**  
   - Check [`privacy/`](privacy/) for implementations  
   - See fairness/explainability notes in [`Guide/INSIGHTS_GUIDE.md`](Guide/INSIGHTS_GUIDE.md)

---

## 📖 Dataset Info

- **Source**: NYC Taxi & Limousine Commission (TLC)
- **Coverage**: Yellow, Green, FHV (For-Hire Vehicle), FHVHV cabs
- **Time**: Historical through 2024; Simulated streaming 2025+
- **Scale**: ~100M+ historical trips, streaming validation on subsets

---

## 🎓 Course Rubric Mapping

This project demonstrates:

| Topic | Location |
|-------|----------|
| Streaming Algorithms | `algorithms/` (Bloom, DGIM, LSH) |
| Data Stream Mining | `spark-jobs_groundTruth/`, ground truth validation |
| Lambda Architecture | `Guide/Design_Document.md`, architecture diagrams |
| Privacy | `privacy/` demos, k-Anonymity & DP techniques |
| Responsible AI | Fairness analysis, explainability in flagging |
| Data Quality | `validation/`, `data_dictionary/` |

---

## 📝 License & Attribution
