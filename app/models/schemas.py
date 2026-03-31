from pydantic import BaseModel, Field
from typing import Optional
from uuid import UUID
from datetime import datetime


# --- Request Schemas ---

class LabValue(BaseModel):
    name: str
    value: float
    unit: str
    flag: Optional[str] = None
    reference_range: Optional[str] = None


class CaseSubmitStructured(BaseModel):
    patient_age: int = Field(ge=0, le=120)
    patient_sex: str
    patient_ethnicity: Optional[str] = None
    presenting_complaint: str = Field(min_length=20)
    medical_history: Optional[str] = None
    medications: Optional[str] = None
    physical_exam: Optional[str] = None
    lab_results: Optional[list[LabValue]] = None
    imaging_reports: Optional[str] = None
    referring_diagnosis: Optional[str] = None
    specific_question: Optional[str] = None
    mode: Optional[str] = "standard"  # "standard" or "zebra"


class CaseSubmitFreeText(BaseModel):
    raw_text: str = Field(min_length=50)
    mode: Optional[str] = "standard"  # "standard" or "zebra"


class ResearchSubmit(BaseModel):
    research_topic: str = Field(min_length=10)
    additional_context: Optional[str] = None
    specialty: Optional[str] = None
    research_intent: Optional[str] = None


class ResearchConfirm(BaseModel):
    original_topic: str = Field(min_length=10)
    confirmed_topic: str = Field(min_length=10)
    specialty: Optional[str] = None
    research_intent: Optional[str] = None
    confirmed_as_medical: bool = True


class FollowUpRequest(BaseModel):
    question: str = Field(min_length=5)


# --- Response Schemas ---

class CaseResponse(BaseModel):
    id: UUID
    status: str
    pipeline_type: Optional[str] = "diagnosis"
    diagnosis_mode: Optional[str] = "standard"
    created_at: datetime
    presenting_complaint: Optional[str] = None
    referring_diagnosis: Optional[str] = None

    model_config = {"from_attributes": True}


class PipelineStep(BaseModel):
    step: int
    label: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_s: Optional[float] = None
    preview: Optional[str] = None
    progress: Optional[str] = None


class PipelineStatus(BaseModel):
    case_id: UUID
    status: str
    current_step: int
    total_steps: int
    steps: list[PipelineStep]
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class EvidenceClaim(BaseModel):
    claim: str
    verdict: str
    evidence: str
    references: list[int]


class Reference(BaseModel):
    id: int
    title: str
    url: str
    snippet: Optional[str] = None


class ReportResponse(BaseModel):
    case_id: UUID
    pipeline_type: Optional[str] = "diagnosis"
    diagnosis_mode: Optional[str] = "standard"
    research_topic: Optional[str] = None
    executive_summary: Optional[str] = None
    medgemma_analysis: Optional[str] = None
    evidence_claims: list[EvidenceClaim] = []
    evidence_synthesis: Optional[str] = None
    storm_article: Optional[str] = None
    references: list[Reference] = []
    primary_diagnosis: Optional[str] = None
    total_sources: int = 0
    hallucination_issues: int = 0
    verification_stats: Optional[dict] = None
    report_html: Optional[str] = None
    pdf_url: Optional[str] = None
    docx_url: Optional[str] = None
    created_at: datetime


class TrialStatus(BaseModel):
    """Trial/subscription status for rate-limited users."""
    tier: Optional[str] = None  # None = free trial, "pro" = paid
    free_reports_used: int = 0
    limit: int = 4
    remaining: int = 4
    trial_ends_at: Optional[datetime] = None


class ResearchReportResponse(BaseModel):
    case_id: UUID
    specialty: Optional[str] = None
    research_intent: Optional[str] = None
    executive_summary: Optional[str] = None
    evidence_claims: list[EvidenceClaim] = []
    evidence_synthesis: Optional[str] = None
    storm_article: Optional[str] = None
    references: list[Reference] = []
    total_sources: int = 0
    hallucination_issues: int = 0
    verification_stats: Optional[dict] = None
    report_html: Optional[str] = None
    pdf_url: Optional[str] = None
    docx_url: Optional[str] = None
    trial_status: Optional[TrialStatus] = None
    created_at: Optional[datetime] = None


class FollowUpResponse(BaseModel):
    question: str
    answer: str
    created_at: datetime


class CaseListItem(BaseModel):
    id: UUID
    status: str
    pipeline_type: Optional[str] = "diagnosis"
    diagnosis_mode: Optional[str] = "standard"
    presenting_complaint: str
    primary_diagnosis: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class CaseListResponse(BaseModel):
    cases: list[CaseListItem]
    total: int
    page: int
    per_page: int


# --- SDSS Async Task Schemas ---

class SdssSubmitRequest(BaseModel):
    case_text: str = Field(min_length=20)
    mode: str = "standard"  # standard / zebra / medgemma
    india_context: bool = False


class SdssSubmitResponse(BaseModel):
    task_id: UUID


class SdssTaskResponse(BaseModel):
    task_id: UUID
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None
    elapsed_seconds: Optional[float] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
