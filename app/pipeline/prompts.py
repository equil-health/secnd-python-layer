"""Medical domain anchoring prompts for the SECND Research pipeline.

Every LLM call in the research pipeline is wrapped with the medical system
prompt to prevent domain misinterpretation (e.g. "RAG" → Recombinase
Activating Gene, not Retrieval-Augmented Generation).
"""

SECND_MEDICAL_SYSTEM_PROMPT = (
    "You are a medical research assistant working within the SECND clinical "
    "decision-support platform. ALL topics, terms, and acronyms MUST be "
    "interpreted in their medical / biomedical context.\n\n"
    "When you encounter an ambiguous acronym or term, always prefer the "
    "medical interpretation:\n"
    "  - RAG  → Recombinase Activating Gene (not Retrieval-Augmented Generation)\n"
    "  - STORM → Stanford Translational Oncology Research in Medicine (not the AI framework)\n"
    "  - BERT → Brief Evaluation of Receptive-Expressive Language (not the NLP model)\n"
    "  - ATLAS → Adjuvant Tamoxifen Longer Against Shorter (not a software tool)\n"
    "  - FALCON → not the LLM; interpret in clinical context\n"
    "  - LLM  → Large Loop Myometrial (excision) or interpret clinically\n"
    "  - GPT  → Glutamic Pyruvic Transaminase (ALT) (not the AI model)\n\n"
    "Your outputs must be grounded in peer-reviewed medical literature. "
    "Do not reference AI/ML frameworks, software tools, or computer-science "
    "concepts unless the user explicitly asks about health informatics."
)

# Site filter appended to Serper queries to bias toward authoritative medical sources
MEDICAL_SEARCH_SUFFIX = (
    "site:nih.gov OR site:pubmed.ncbi.nlm.nih.gov "
    "OR site:medscape.com OR site:uptodate.com"
)

# Keywords appended to claim search queries to anchor results in clinical literature
MEDICAL_KEYWORD_SUFFIX = (
    "clinical presentation pathophysiology treatment guidelines"
)


def build_medical_prompt(
    step_instruction: str,
    research_topic: str = "",
    specialty: str = "",
) -> str:
    """Wrap any pipeline step prompt with the medical system prompt.

    Parameters
    ----------
    step_instruction : str
        The original prompt for this pipeline step.
    research_topic : str, optional
        The user's research topic, added for context.
    specialty : str, optional
        The medical specialty, added for context.

    Returns
    -------
    str
        The combined prompt with medical domain anchoring prepended.
    """
    parts = [SECND_MEDICAL_SYSTEM_PROMPT, ""]

    if research_topic or specialty:
        context_line = "Context:"
        if research_topic:
            context_line += f" Research topic = \"{research_topic}\"."
        if specialty:
            context_line += f" Specialty = {specialty}."
        parts.append(context_line)
        parts.append("")

    parts.append(step_instruction)
    return "\n".join(parts)
