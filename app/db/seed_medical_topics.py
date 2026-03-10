"""Seed medical_topic_embeddings table for pgvector fast-pass domain validation.

Usage (from backend root):
    python -m app.db.seed_medical_topics

Generates embeddings for ~200 core medical topics using text-embedding-004,
then upserts them into the medical_topic_embeddings table.

This is a one-time setup script. Re-running is safe (idempotent via ON CONFLICT).
"""

import sys
import time
import logging

from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Core medical topics covering the 10 Breaking specialties + general terms.
# Each entry should be a concise medical concept (not a sentence).
MEDICAL_TOPICS = [
    # Cardiology
    "myocardial infarction", "heart failure", "atrial fibrillation", "cardiac arrest",
    "coronary artery disease", "hypertension", "aortic stenosis", "mitral regurgitation",
    "cardiomyopathy", "pericarditis", "endocarditis", "pulmonary embolism",
    "deep vein thrombosis", "cardiac catheterization", "echocardiography",
    "STEMI", "NSTEMI", "troponin", "BNP natriuretic peptide", "anticoagulation therapy",

    # Neurology
    "stroke", "ischemic stroke", "hemorrhagic stroke", "epilepsy", "seizure disorder",
    "multiple sclerosis", "Parkinson disease", "Alzheimer disease", "dementia",
    "migraine", "neuropathy", "peripheral neuropathy", "meningitis", "encephalitis",
    "traumatic brain injury", "amyotrophic lateral sclerosis", "Guillain-Barre syndrome",
    "trigeminal neuralgia", "cerebral aneurysm", "lumbar puncture",

    # Hepatology
    "autoimmune hepatitis", "hepatocellular carcinoma", "cirrhosis", "liver fibrosis",
    "hepatitis B", "hepatitis C", "nonalcoholic fatty liver disease", "NAFLD", "NASH",
    "primary biliary cholangitis", "primary sclerosing cholangitis", "Wilson disease",
    "hemochromatosis", "liver transplantation", "portal hypertension", "ascites",
    "hepatic encephalopathy", "liver biopsy", "ALT AST liver enzymes", "jaundice",

    # Oncology
    "breast cancer", "lung cancer", "colorectal cancer", "prostate cancer",
    "pancreatic cancer", "lymphoma", "leukemia", "melanoma", "glioblastoma",
    "immunotherapy checkpoint inhibitors", "chemotherapy", "radiation therapy",
    "targeted therapy", "tumor biomarkers", "PD-L1 expression", "BRCA mutation",
    "cancer staging TNM", "bone marrow transplant", "CAR-T cell therapy", "metastasis",

    # Pulmonology
    "chronic obstructive pulmonary disease", "COPD", "asthma", "pneumonia",
    "interstitial lung disease", "pulmonary fibrosis", "sarcoidosis",
    "obstructive sleep apnea", "pleural effusion", "pneumothorax",
    "acute respiratory distress syndrome", "bronchoscopy", "spirometry",
    "tuberculosis", "cystic fibrosis", "pulmonary hypertension",
    "mechanical ventilation", "bronchiectasis", "lung transplantation", "emphysema",

    # Endocrinology
    "diabetes mellitus type 1", "diabetes mellitus type 2", "insulin resistance",
    "thyroid disorders", "hypothyroidism", "hyperthyroidism", "Graves disease",
    "Hashimoto thyroiditis", "Cushing syndrome", "Addison disease",
    "pheochromocytoma", "diabetic ketoacidosis", "HbA1c glycated hemoglobin",
    "polycystic ovary syndrome", "adrenal insufficiency", "acromegaly",
    "hyperparathyroidism", "osteoporosis", "GLP-1 receptor agonists", "SGLT2 inhibitors",

    # Gastroenterology
    "Crohn disease", "ulcerative colitis", "inflammatory bowel disease",
    "gastroesophageal reflux disease", "GERD", "peptic ulcer disease",
    "celiac disease", "irritable bowel syndrome", "gastrointestinal bleeding",
    "colonoscopy", "endoscopy", "Barrett esophagus", "diverticulitis",
    "pancreatitis acute", "pancreatitis chronic", "Helicobacter pylori",
    "gastrointestinal malignancy", "fecal microbiota transplant",
    "small bowel obstruction", "gallstones cholecystitis",

    # General Medicine
    "sepsis", "septic shock", "acute kidney injury", "chronic kidney disease",
    "anemia", "iron deficiency anemia", "sickle cell disease", "thalassemia",
    "systemic lupus erythematosus", "rheumatoid arthritis", "gout",
    "HIV AIDS", "COVID-19", "influenza", "malaria", "dengue fever",
    "electrolyte imbalance", "hyponatremia", "hyperkalemia", "acid base disorders",

    # Nephrology
    "chronic kidney disease", "dialysis", "kidney transplantation",
    "glomerulonephritis", "nephrotic syndrome", "nephritic syndrome",
    "polycystic kidney disease", "renal cell carcinoma", "urinary tract infection",
    "acute tubular necrosis", "IgA nephropathy", "lupus nephritis",
    "diabetic nephropathy", "renal artery stenosis", "hemodialysis",
    "peritoneal dialysis", "creatinine clearance GFR", "proteinuria",
    "kidney biopsy", "electrolyte renal tubular acidosis",

    # Rheumatology
    "rheumatoid arthritis", "systemic lupus erythematosus", "ankylosing spondylitis",
    "psoriatic arthritis", "vasculitis", "scleroderma", "dermatomyositis",
    "polymyositis", "Sjogren syndrome", "antiphospholipid syndrome",
    "giant cell arteritis", "polymyalgia rheumatica", "fibromyalgia",
    "osteoarthritis", "gout uric acid", "biologic DMARD therapy",
    "TNF inhibitors", "IL-6 inhibitors", "JAK inhibitors",
    "autoimmune disease ANA antibody",
]


def seed_embeddings():
    """Generate embeddings and insert into medical_topic_embeddings."""
    from app.config import settings
    from app.breaking.semantic_utils import get_embeddings_batch

    sync_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
    engine = create_engine(sync_url)

    # De-duplicate (some topics appear in multiple specialties)
    unique_topics = list(dict.fromkeys(MEDICAL_TOPICS))
    logger.info("Generating embeddings for %d medical topics...", len(unique_topics))

    # Batch embed (100 per API call)
    start = time.time()
    embeddings = get_embeddings_batch(unique_topics)
    elapsed = time.time() - start
    logger.info("Embeddings generated in %.1fs", elapsed)

    # Upsert into DB
    inserted = 0
    with engine.begin() as conn:
        for topic, emb in zip(unique_topics, embeddings):
            emb_str = "[" + ",".join(str(v) for v in emb) + "]"
            conn.execute(
                text(
                    "INSERT INTO medical_topic_embeddings (topic, embedding) "
                    "VALUES (:topic, :emb::vector) "
                    "ON CONFLICT (topic) DO UPDATE SET embedding = EXCLUDED.embedding"
                ),
                {"topic": topic, "emb": emb_str},
            )
            inserted += 1

    logger.info("Seeded %d medical topic embeddings", inserted)


if __name__ == "__main__":
    seed_embeddings()
