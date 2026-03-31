#!/usr/bin/env python3
"""
Established Truth, Principles Automation - Railway Deployment
Complete replica of Google Apps Script with Python/Railway
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from io import BytesIO
import requests
# Note: docx imports removed — documents are now saved as markdown (.md) files
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

# Google Drive imports
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials as OAuthCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# CONFIG - Load from environment variables or use defaults
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "3062967169c28172bf7bf32fe67f1a97")
DAILY_DOCUMENTS_DB_ID = os.getenv("DAILY_DOCUMENTS_DB_ID", "e612e7e887cf4462819e92c14ff7a6de")
UMBRELLA_TERM_DOCUMENTS_DB_ID = os.getenv("UMBRELLA_TERM_DOCUMENTS_DB_ID", "9d6fd374d6ce457cbdfccb88dcf91d55")
DRIVE_FOLDER_NAME = os.getenv("DRIVE_FOLDER_NAME", "🧠 Established Truth, Principles, Understanding")
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "")  # Direct folder ID — bypasses name search if set
DAILY_DRIVE_FOLDER_NAME = os.getenv("DAILY_DRIVE_FOLDER_NAME", "📅 Established Daily Documents")
DAILY_DRIVE_FOLDER_ID = os.getenv("DAILY_DRIVE_FOLDER_ID", "")  # Direct folder ID for daily docs
UMBRELLA_DRIVE_FOLDER_NAME = os.getenv("UMBRELLA_DRIVE_FOLDER_NAME", "Established Umbrella Term Documents")
UMBRELLA_DRIVE_FOLDER_ID = os.getenv("UMBRELLA_DRIVE_FOLDER_ID", "")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
CLAUDE_INPUT_COST_PER_M = float(os.getenv("CLAUDE_INPUT_COST_PER_M", "3.00"))
CLAUDE_OUTPUT_COST_PER_M = float(os.getenv("CLAUDE_OUTPUT_COST_PER_M", "15.00"))
MAX_TITLE_LENGTH = int(os.getenv("MAX_TITLE_LENGTH", "150"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
PROCESSING_INTERVAL_MINUTES = int(os.getenv("PROCESSING_INTERVAL_MINUTES", "5"))
REGISTRY_PAGE_ID = os.getenv("REGISTRY_PAGE_ID", "30729671-69c2-81f9-aaf9-edbbe03eee96")
OPERATION_NAME = os.getenv("OPERATION_NAME", "Established Truth, Principles Automation")
GOOGLE_ACCOUNT = os.getenv("GOOGLE_ACCOUNT", "codyandersonnexus@gmail.com")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# Seed umbrella terms
SEED_UMBRELLA_TERMS = [
    "Divination", "Consciousness", "Automatism", "Transmutation", "Hermeticism",
    "Scripture", "Psychical Research", "Kabbalah", "Depth Psychology", "Esotericism",
    "Embodiment", "Rhetoric", "Cognition", "Craft", "Historiography", "Media"
]

# User's exact prompt
USER_PROMPT_TEMPLATE = """Recognize, establish truths, principles of the {topic}

I want you to provide the following under each (principle, truth) you provide Pattern function principle embodiment wisdom insight through lens of Earthwalker Lover Human Builder (I am self employed, my projects usually consist of cloning existing brands, formats in existing markets and directing them to my monetization points) Person who trains in the gym for visible muscle size gain Provide Utility Provide Human Parallel Provide Insight through Parallel with understood language, subject. So parallel the information with language I understand already. Christian symbolism, Psychology, English — A reminder to properly align your generations: You should provide with the most relevant core truth principles wisdom relevant I believe core principles, functions and truth is what enables us to recognize patterns and better navigate the human experience. The intent of this is to expand my understanding of my own human body, function, the symbols and figures I will encounter in my personal human experience of life, the principles the earth follows and ultimate truth and ultimate truth and the deepest and highest levels of understanding possible. I want information that I can read, recognize, understand, utilize. I want to obtain the deepest and highest level of understanding known to mankind and beyond."""

SYSTEM_MESSAGE = """You are generating a comprehensive educational document. Write a complete, well-structured document. You MUST complete the entire document with a proper conclusion. Plan your structure so every section is fully developed and the document ends with a complete closing. Do not leave any section unfinished."""

_REMOVED_V4 = """VOICE:

You speak as a builder to a builder — a mentor who has already walked the territory and is drawing the map for someone about to walk it. You speak with absolute structural confidence. You do not hedge. You do not say "perhaps," "research suggests," "it could be argued," or "some scholars believe." You declare what IS true, then prove it by showing it operating across multiple domains simultaneously. Your voice combines warmth and authority — never academic distance, never motivational shouting, always the calm certainty of direct observation. You address the reader as "you" throughout. The reader is a self-employed builder who clones existing systems, products, content, and formats in existing markets and directs them to his monetization points. He trains in the gym for visible muscle size gain. He reads Christian Scripture for structural truth. He studies psychology to recognize what operates beneath the surface. He walks the earth as a conscious human being seeking the deepest available understanding of how things actually work.
EPISTEMOLOGICAL POSTURE:

The parallels you draw across domains are not illustrations. They are not metaphors. They are structural identity — the same truth operating in different material. When you show that the same principle governs a business model, a muscle fiber, a scriptural event, and a psychological mechanism, you are not saying "this is like that." You are saying "this IS that — the same structural truth in a different medium." Assert this directly when it matters: "This is not metaphor — it is the operating law." "This is not philosophical abstraction — it is neurological fact." "This is not simplistic — it is the recovery of the essential."

SENTENCE CONSTRUCTION:

Alternate between long analytical sentences (20-40 words, multi-clause, building logical structure) and short impact sentences (3-12 words, declarative, landing the point). This creates breathing rhythm — expansion and compression, like inhale and exhale. Long sentences build pressure. Short sentences release it.

Use em dashes extensively — they are your primary tool for creating the voice's characteristic precision-with-momentum. Use them for: clarifying apposition (income extracted from a position of control — not earned through production), dramatic pivots (He was not having an emotional outburst. He was identifying and attacking a manufactured dependency), parenthetical insertions, range specification (from the geopolitical to the personal), list expansion, and rhetorical escalation (the engineering of captivity — and then billing the captive for their chains).

Use the "Not X. Y." construction frequently: "This is not productivity. It is not value creation. It is the engineering of captivity." "Narrative is not decoration. It is infrastructure." "Behavior follows identity. Not goal. Not motivation. Not willpower. Identity." This construction performs epistemological correction — replacing the reader's assumed frame with the true frame.

Use bold for: core principle declarations, key terms being defined for the first time, emphatic assertions that land as truth in isolation, standalone impact sentences, imperative instructions to the reader, and contrast structures (the value creator vs. the rent-seeker). Do not bold labels, transitions, or structural markers. Bold only what hits.

Use standalone single-sentence paragraphs (3-7 per principle) as the document's punctuation marks — they land a point with silence around them.

DOCUMENT STRUCTURE:

Begin with the DOCUMENT TITLE in all caps (# heading), followed by a SUBTITLE describing what the document is (## heading). Example: "# FERNAND BRAUDEL: AFTERTHOUGHTS ON MATERIAL CIVILIZATION AND CAPITALISM" followed by "## A Comprehensive Analytical Document on Core Truths, Principles, and Patterns of Economic Reality."

If the source material has a quotable thesis, a defining statement by the original author, or a scriptural verse that captures the core truth, include it as an italicized EPIGRAPH set apart by horizontal rules (---) before the opening section. The epigraph is 1-2 sentences maximum — compressed, evocative, operating at the depth the document will sustain. Most documents use one epigraph. Two is the maximum. If no natural epigraph exists, omit it and proceed directly to the opening section.

Then write the opening section. Label it with one of these based on what fits best:
- "# PREFACE:" (most common — use for most topics)
- "# PREAMBLE:" (use when the document requires specific preparation before reading)
followed by a subtitle: "WHY THIS MATTERS TO YOU" or "WHAT YOU ARE ABOUT TO UNDERSTAND" or "WHY THIS DOCUMENT EXISTS" or "HOW TO READ THIS DOCUMENT" or "WHO THIS DOCUMENT IS FOR" — choose whichever fits the specific document.

The opening section is 2-3 paragraphs. It does three things in order:
1. Makes a universal claim about reality — a declaration about the world that is true before the specific topic is introduced. This claim should reveal a hidden structure or name a pattern the reader is INSIDE but has not seen. ("There is a game being played on every level of human civilization." "There is a war happening inside every human being." "Every system that survives, does so because it knows how to talk to itself." "Most people live and die never understanding the system they are inside of.")
2. Introduces the topic as an instance of that universal truth — names the subject and what makes it significant. For author-based topics, briefly state who the author was and what they did that matters. For concept-based topics, name the concept and why it operates at a deeper level than commonly understood.
3. Promises what the document will DO TO the reader — not what it is about, but how they will see differently after reading. "When you finish reading this, you will see this pattern operating everywhere." "This document will translate his core truths into the language of your life." Name the lenses the document will use (business, body, Scripture, psychology). If the topic warrants reading instructions ("Read this slowly. Read it more than once."), include them.

Then organize the body into PARTS. Use "# PART ONE:" "# PART TWO:" etc. (number words spelled out: ONE, TWO, THREE, FOUR, FIVE) with descriptive titles in all caps. Follow this guideline:
- 1 Part: Use when the topic is a single unified idea explored through multiple principles.
- 2 Parts: Use for foundation + application, or definition + principles, or simple + complex. This is the most common structure.
- 3-5 Parts: Use only for deeply layered source material with distinct structural divisions.

Some Parts may have a brief contextual paragraph between the Part heading and the first Principle — use this when the Part needs framing before its principles begin. Otherwise, proceed directly to the first Principle.

Within each Part, write numbered PRINCIPLES or CORE TRUTHS. The label can vary naturally: "## PRINCIPLE 1: THE BOTTLENECK IS THE BUSINESS" or "## CORE TRUTH #1: CAUSALITY BEGINS WITHIN" or "## FOUNDATIONAL TRUTH I: THE HIDDEN OBSERVER IS ALWAYS PRESENT." Choose the label that fits the source material's character. The title after the number must be a standalone truth — not a topic label but a sentence that hits: "THE BOTTLENECK IS THE BUSINESS," "CAUSALITY BEGINS WITHIN," "CREATION WITHOUT CONTAINMENT IS ENTROPY." If someone read only the principle titles, they would carry away truths worth carrying. Use between 4 and 7 principles total per document. Each principle must earn its existence by revealing a genuinely distinct operating truth.

FOR EACH PRINCIPLE, follow this internal sequence:

**Core Truth:** Bold, compressed, 1-2 sentences maximum. The principle compressed into its most essential form — the fewest words that carry the full truth. If the reader read only this sentence, they would carry something worth carrying.

**The Principle:** 2-3 paragraphs expanding the Core Truth. Declare what IS true. Show the mechanism. Use concrete examples before naming the abstract pattern. Use "This is not X — it is Y" to correct assumed understanding.

**Pattern and Function:** One paragraph. Pattern names WHERE this truth operates across domains — list multiple instances showing the same structure in different material. Function names WHAT the pattern does with a single word first (amplification, extraction, constraint, investment, navigation, filtration, preservation), then explains that function in 2-3 sentences.

**Embodiment:** One paragraph. How does a human being physically live this truth across all domains? "You embody this when you..." Use imperative language. This is the principle made behavioral before being split into domain-specific lenses.

**Wisdom:** One paragraph. The compressed, aphoristic crystallization — the sentence the reader carries out of this section. Bold the key sentence. This is the quotable line. "Read the field before you plant." "The vessel is the difference between utility and waste." "Behavior follows identity. Not goal. Not motivation. Not willpower. Identity."

Then deliver the LENS APPLICATIONS in this order:

**Earthwalker / Builder Lens:** ALWAYS FIRST. ALWAYS THE LONGEST — 3-6 paragraphs. Speak directly to the reader's business model: cloning formats, redirecting traffic, building monetization infrastructure, lists, backends, offer architectures. Begin by VALIDATING the reader's existing strategy — show them that what they are already doing is an expression of this principle ("Your strategy is instinctively aligned with this truth. You recognized that you don't need to invent — you need to occupy and build above."). Then go past analogy to IMPLEMENTATION. Show the reader what this principle means for their specific business operations — not as a numbered task list, but as strategic intelligence that reframes how they see their own model. When rent-seeking dynamics, bottleneck positioning, monopoly structures, information asymmetry, or flow of money are relevant, weave them into this lens — they are strategic intelligence applied to the builder's reality, not separate sections. Name the reader's acts: "When you clone a brand..." "Your email list is..." "Your monetization architecture..."

**Gym / Body Lens:** 2-4 paragraphs. Start with something the reader has FELT — a real sensation, a real training moment. Then reveal the physiological mechanism. Name specific terms when they illuminate: hormones (testosterone, IGF-1, cortisol, growth hormone), processes (protein synthesis, anabolic signaling, motor unit recruitment, neurological efficiency), systems (nervous system, endocrine system, cardiovascular). Show structural identity: the truth governing the business governs the body identically.

**Christian Parallel:** 1-3 paragraphs. Cite specific Scripture with book, chapter, verse. Quote the passage directly. Then show the structural mechanism — read Scripture as a builder reads a blueprint, for the mechanism, not the sentiment. Not "this teaches us to be good" but "this IS the same pattern operating in the architecture of redemption." Use Greek or Hebrew word study when it reveals structural meaning (enkrateia, lev, metamorphoo, menō). Sometimes invert the pattern — show how Christ's operation reverses the extraction/dependency mechanism the principle describes.

**Psychological Parallel:** 1-2 paragraphs. Name the specific psychologist and theory. Go beyond naming — explain HOW the mechanism works, then show it is the same mechanism as the principle. Prefer lesser-known, more structurally revealing references over overused defaults. The reader should recognize the psychological truth in their own behavior.

**Insight Through Parallel:** The closing synthesis. 1-2 paragraphs. Deliver a vivid METAPHOR that crystallizes the entire principle into one image the reader carries forward — an iceberg, a casino, a solar system, a surfer reading the swell, a field being read, a captain navigating an ocean. The metaphor is the principle compressed into a picture. Explicitly map the metaphor's elements back to the principle's components: "The ocean floor is the longue durée. The swell is the conjuncture. The foam is event-time." Use language the reader already uses and thinks in. Connect the metaphor back to the reader's specific world. The very last sentence of the Insight Through Parallel must be the image itself — not an imperative, not a command, not "do this," not "become this." The image does the final work. The reader carries the picture, not an instruction.

When the topic naturally calls for it, include additional lenses within any Principle:
- **Lover Lens:** The person in intimate relationship — how this truth operates in the domain of love, desire, trust, vulnerability, and human connection. 1-2 paragraphs.
- **Human Parallel / Plain English:** The principle restated in everyday human experience — no academic framing, no theory names, just recognizable lived human behavior.
- **English Language Parallel:** When a word's etymology or grammatical structure reveals the principle operating within the language itself.
These additional lenses appear only when they genuinely illuminate — they are not required in every principle.

Separate each principle with a horizontal rule (---).

After the final principle, the document ENDS. No formal conclusion. No summary. No epilogue. No synthesis section. The document is complete when the last principle's last lens has been delivered. A document that needs a summary was not clear enough in its principles.

WHAT NOT TO DO:

Do not hedge. Do not write "perhaps," "it seems," "one might argue," "research suggests."
Do not write academic prose. The researcher is never the subject of the sentence — the truth is the subject, the researcher is the proof.
Do not create sections that could appear under any principle — every section must be specific to THIS principle through THIS lens.
Do not repeat the same insight in different words across different sections.
Do not pad. Depth means going deeper into fewer things, not wider across more.
Do not write a conclusion, summary, or synthesis at the end.
Do not use the same metaphor in multiple Insight Through Parallel sections — each uses a fresh image.
Do not force a lens when it does not genuinely illuminate the principle. Four deep lenses are stronger than seven thin ones.
Do not create Rent-Seeking, Bottleneck, or Monopoly as separate section headers — weave these dynamics into the Earthwalker/Builder lens when relevant.
Do not write any container as a header followed by a few sentences — every container unfolds with depth, flowing as a section the reader moves through, not a stop on a tour.

The complete document should be 40,000-70,000 characters. This is the range where every section has room to develop depth while the document remains sharp and concentrated. Documents over 80,000 characters are almost certainly carrying redundant sections or restating the same insight in too many lenses."""

UMBRELLA_TERM_PROMPT_TEMPLATE = """Given the topic "{topic}", assign it to the single most fitting umbrella term.

EXISTING ESTABLISHED UMBRELLA TERMS: {terms_list}

Think structurally. The umbrella term is a container — a domain of human knowledge and practice that this topic naturally lives inside. Not what the topic IS, but what FIELD the topic belongs to. A document about "Fernand Braudel" belongs under "Historiography" because Braudel IS a historian — the field is the container, not the person. A document about "Rite of Passage" belongs under "Esotericism" or "Embodiment" because rites of passage are structures of transformation — the structural domain is the container.

RULES:
1. FIRST — Check every existing term. Most topics SHOULD fit under an existing term. If the topic is a natural member, instance, expression, practitioner, concept, or artifact of an existing umbrella — use that existing term. Default to existing terms. Do not create new terms when an existing one works.
2. ONLY IF NO EXISTING TERM GENUINELY FITS — Establish a new umbrella term. It must be:
   - A genuine domain word (1-2 words maximum) — not a topic label, not a description
   - A real word with real meaning — the kind of word where if you said it, a person would immediately picture the domain it contains
   - Specific enough that not every topic fits under it
   - Broad enough that at least 20-50 other topics could naturally belong under it too
   - NEVER use: "Knowledge", "Study", "Learning", "Wisdom", "Truth", "Philosophy", "Science", "Culture", "History", "Ideas", "Spirituality", "Religion", "Psychology" (too broad — everything fits)
   - NEVER use multi-word academic phrases like "Comparative Religion" or "Political Theory"
3. ANTI-DRIFT:
   - If uncertain between two existing terms, choose the one where the topic is MORE CENTRALLY a member — where the topic is a core example of that domain, not a peripheral one.
   - Do NOT create a term that only this one topic could ever belong to.
   - Each term should feel like a real library section with real books in it.

Respond with ONLY the umbrella term. Nothing else. No explanation, no quotes, no punctuation. Just the term."""

# Column names mapping
COLUMNS = {
    "TITLE": "Word, Phrase, Topic",
    "PROCESSED": "Established Truth, Principles, Understanding?",
    "LINK": "Link to Established Resource",
    "READ": "Read, Consumed, Digested?",
    "PRIORITY": "Priority?",
    "UMBRELLA_TERM": "Umbrella Term",
    "DOC_DATE": "Document Establishment Date",
    "UTILIZED_UMBRELLA": "Utilized, Umbrella Term Establishment?",
    "QUOTA_SOURCE": "Quota Source (Google Account)",
    "DAILY_TITLE": "Daily Document",
    "DAILY_DATE": "Date",
    "DAILY_COUNT": "Document Count",
    "DAILY_LINK": "Google Drive, Established Document Link",
    "DAILY_STATUS": "Status",
    "UTD_TITLE": "Umbrella Term Document",
    "UTD_UMBRELLA": "Umbrella Term",
    "UTD_DATE": "Date",
    "UTD_COUNT": "Document Count",
    "UTD_LINK": "Google Drive, Established Document Link",
    "UTD_STATUS": "Status",
    "REGISTRY_NAME": "Operation Name",
    "REGISTRY_STATUS": "Status",
    "REGISTRY_MODEL": "AI Platform & Model",
    "REGISTRY_DATE": "Date Established",
    "REGISTRY_DESCRIPTION": "Operation Description",
    "REGISTRY_DRIVE": "Google Drive Folder",
    "REGISTRY_LAST_RUN": "Last Successful Run",
    "REGISTRY_COST": "Current Month API Cost",
    "REGISTRY_RESTORATION": "Operation Restoration",
    "REGISTRY_FREQUENCY": "Run Frequency",
    "REGISTRY_TOTAL": "Total Inputs Processed",
    "REGISTRY_HEALTH": "Infrastructure Health",
    "REGISTRY_LOGS": "Operation Logs",
    "REGISTRY_SCRIPT": "Google Apps Script",
    "REGISTRY_DATABASES": "Relevant Notion Database(s)",
    "REGISTRY_CLAUDE_STATUS": "Claude API Status",
    "REGISTRY_DRIVE_STATUS": "Google Drive Status",
    "REGISTRY_NOTION_STATUS": "Notion API Status",
    "REGISTRY_DOC_GEN": "Document Generation",
    "REGISTRY_DAILY_COMP": "Daily Compilation",
    "REGISTRY_HEALTH_SCORE": "Health Score"
}


class NotionAPI:
    """Notion API wrapper for database queries and updates"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }

    def query_database(self, database_id: str, filter_dict: Optional[Dict] = None, page_size: int = 100, max_results: int = 0) -> List[Dict]:
        """Query a Notion database with optional filtering.
        If max_results > 0, stop after collecting that many results (avoids paginating entire DB).
        """
        url = f"{self.base_url}/databases/{database_id}/query"
        payload = {"page_size": min(page_size, 100)}
        if filter_dict:
            payload["filter"] = filter_dict

        results = []
        has_more = True
        start_cursor = None

        while has_more:
            if start_cursor:
                payload["start_cursor"] = start_cursor

            try:
                response = requests.post(url, headers=self.headers, json=payload)
                response.raise_for_status()
                data = response.json()
                results.extend(data.get("results", []))
                has_more = data.get("has_more", False)
                start_cursor = data.get("next_cursor")

                # Stop early if we have enough results
                if max_results > 0 and len(results) >= max_results:
                    break
            except requests.exceptions.RequestException as e:
                logger.error(f"Error querying database {database_id}: {e}")
                break

        return results

    def get_database(self, database_id: str) -> Dict:
        """Get database schema (properties, options, etc.)"""
        url = f"{self.base_url}/databases/{database_id}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting database {database_id}: {e}")
            return {}

    def get_page(self, page_id: str) -> Dict:
        """Get a single page"""
        url = f"{self.base_url}/pages/{page_id}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting page {page_id}: {e}")
            return {}

    def update_page_properties(self, page_id: str, properties: Dict) -> bool:
        """Update page properties"""
        url = f"{self.base_url}/pages/{page_id}"
        payload = {"properties": properties}

        try:
            response = requests.patch(url, headers=self.headers, json=payload)
            if response.status_code >= 400:
                try:
                    error_body = response.json()
                    logger.error(f"Notion API error for page {page_id}: {response.status_code} - {error_body.get('message', 'No message')} - Code: {error_body.get('code', 'N/A')}")
                except Exception:
                    logger.error(f"Notion API error for page {page_id}: {response.status_code} - {response.text[:500]}")
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating page {page_id}: {e}")
            return False

    def get_block_children(self, block_id: str) -> List[Dict]:
        """Get children blocks of a page"""
        url = f"{self.base_url}/blocks/{block_id}/children"
        results = []
        has_more = True
        start_cursor = None

        while has_more:
            params = {"page_size": 100}
            if start_cursor:
                params["start_cursor"] = start_cursor

            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                data = response.json()
                results.extend(data.get("results", []))
                has_more = data.get("has_more", False)
                start_cursor = data.get("next_cursor")
                if has_more:
                    time.sleep(0.35)  # Rate limiting — Notion allows 3 req/sec
            except requests.exceptions.RequestException as e:
                logger.error(f"Error getting block children for {block_id}: {e}")
                break

        return results

    def append_block_children(self, block_id: str, children: List[Dict]) -> bool:
        """Append children blocks to a page"""
        url = f"{self.base_url}/blocks/{block_id}/children"
        payload = {"children": children}

        try:
            response = requests.patch(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Error appending blocks to {block_id}: {e}")
            return False

    def create_page(self, parent_id: str, properties: Dict, children: Optional[List[Dict]] = None) -> Optional[str]:
        """Create a new page"""
        url = f"{self.base_url}/pages"
        payload = {
            "parent": {"database_id": parent_id},
            "properties": properties
        }
        if children:
            payload["children"] = children

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json().get("id")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating page in {parent_id}: {e}")
            return None

    def get_property_value(self, page: Dict, property_name: str) -> Any:
        """Extract property value from a page"""
        properties = page.get("properties", {})
        if property_name not in properties:
            return None

        prop = properties[property_name]
        prop_type = prop.get("type")

        if prop_type == "checkbox":
            return prop.get("checkbox", False)
        elif prop_type == "title":
            title_list = prop.get("title", [])
            return "".join([t.get("plain_text", "") for t in title_list])
        elif prop_type == "rich_text":
            text_list = prop.get("rich_text", [])
            return "".join([t.get("plain_text", "") for t in text_list])
        elif prop_type == "url":
            return prop.get("url")
        elif prop_type == "date":
            date_obj = prop.get("date", {})
            return date_obj.get("start") if date_obj else None
        elif prop_type == "select":
            select_obj = prop.get("select", {})
            return select_obj.get("name") if select_obj else None
        elif prop_type == "number":
            return prop.get("number")

        return None


class ClaudeAPI:
    """Claude API wrapper for content generation"""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-6"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.anthropic.com/v1"

    def generate_document(self, topic: str, max_tokens: int = 32000) -> tuple[Optional[str], Dict]:
        """Generate comprehensive document for a topic using user's exact prompt"""
        user_content = USER_PROMPT_TEMPLATE.format(topic=topic)

        content, usage = self._call_api(
            [
                {"role": "user", "content": user_content}
            ],
            system=SYSTEM_MESSAGE,
            max_tokens=max_tokens
        )

        # Validate document length (umbrella term calls are short by design, but documents should be substantial)
        if content and len(content.strip()) < 500:
            logger.error(f"Document content too short ({len(content)} chars) for '{topic}', treating as failure")
            return None, usage

        return content, usage

    def assign_umbrella_term(self, topic: str, existing_terms: List[str], max_tokens: int = 50) -> tuple[Optional[str], Dict]:
        """Assign an umbrella term to a topic"""
        terms_list = ", ".join(existing_terms)
        user_content = UMBRELLA_TERM_PROMPT_TEMPLATE.format(
            topic=topic,
            terms_list=terms_list
        )

        response, usage = self._call_api(
            [
                {"role": "user", "content": user_content}
            ],
            system="You are a taxonomist. Respond with ONLY the umbrella term. Nothing else.",
            max_tokens=max_tokens
        )

        if response:
            response = response.strip().strip('"').strip("'")

        return response, usage

    def _call_api(self, messages: List[Dict], system: str = "", max_tokens: int = 64000) -> tuple[Optional[str], Dict]:
        """Make API call to Claude using streaming to prevent timeouts.

        Streaming keeps the connection alive as text chunks arrive,
        preventing idle-read timeouts on long document generations.
        """
        url = f"{self.base_url}/messages"

        payload = {
            "model": self.model,
            "max_tokens": max_tokens,
            "messages": messages,
            "stream": True
        }

        if system:
            payload["system"] = system

        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }

        try:
            # connect timeout 30s, read timeout 1200s (20 min safety net)
            response = requests.post(
                url, headers=headers, json=payload,
                timeout=(30, 1200), stream=True
            )
            response.raise_for_status()

            # Parse Server-Sent Events stream
            content_parts = []
            usage = {"input_tokens": 0, "output_tokens": 0}
            stop_reason = "unknown"
            chunks_received = 0

            for line in response.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue

                data_str = line[6:]  # Remove "data: " prefix
                if data_str.strip() == "[DONE]":
                    break

                try:
                    data = json.loads(data_str)
                    event_type = data.get("type", "")

                    if event_type == "content_block_delta":
                        delta = data.get("delta", {})
                        if delta.get("type") == "text_delta":
                            content_parts.append(delta.get("text", ""))
                            chunks_received += 1
                            # Log progress every 200 chunks
                            if chunks_received % 200 == 0:
                                logger.info(f"Streaming: received {chunks_received} chunks, ~{sum(len(p) for p in content_parts)} chars so far")

                    elif event_type == "message_start":
                        msg = data.get("message", {})
                        msg_usage = msg.get("usage", {})
                        if msg_usage:
                            usage["input_tokens"] = msg_usage.get("input_tokens", 0)

                    elif event_type == "message_delta":
                        delta = data.get("delta", {})
                        stop_reason = delta.get("stop_reason", stop_reason)
                        msg_usage = data.get("usage", {})
                        if msg_usage:
                            usage["output_tokens"] = msg_usage.get("output_tokens", usage["output_tokens"])

                    elif event_type == "error":
                        error_msg = data.get("error", {}).get("message", "Unknown stream error")
                        logger.error(f"Claude API stream error: {error_msg}")
                        response.close()
                        return None, usage

                except json.JSONDecodeError:
                    continue

            response.close()
            content = "".join(content_parts)

            logger.info(f"Streaming complete: {chunks_received} chunks, {len(content)} chars, stop_reason={stop_reason}")

            if stop_reason == "max_tokens":
                logger.warning(f"Response truncated (max_tokens reached). Output tokens: {usage['output_tokens']}")

            if not content:
                logger.error("No content returned from Claude API")
                return None, usage

            return content, usage
        except requests.exceptions.Timeout:
            logger.error("Claude API request timed out (connect or read timeout exceeded)")
            return None, {"input_tokens": 0, "output_tokens": 0}
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Claude API connection error: {e}")
            return None, {"input_tokens": 0, "output_tokens": 0}
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling Claude API: {e}")
            return None, {"input_tokens": 0, "output_tokens": 0}


class GoogleDriveAPI:
    """Google Drive API wrapper for file operations using OAuth2"""

    def __init__(self, client_id: str, client_secret: str, refresh_token: str):
        self.service = None
        self.folder_cache = {}
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token

        if client_id and client_secret and refresh_token:
            try:
                credentials = OAuthCredentials(
                    token=None,
                    refresh_token=refresh_token,
                    token_uri='https://oauth2.googleapis.com/token',
                    client_id=client_id,
                    client_secret=client_secret,
                    scopes=['https://www.googleapis.com/auth/drive']
                )
                # Force token refresh
                credentials.refresh(GoogleAuthRequest())

                self.service = build('drive', 'v3', credentials=credentials)
                logger.info("Google Drive API initialized successfully with OAuth2")
            except Exception as e:
                logger.error(f"Error initializing Google Drive API: {e}")

    def find_folder(self, folder_name: str) -> Optional[str]:
        """Find folder ID by name"""
        if folder_name in self.folder_cache:
            return self.folder_cache[folder_name]

        if not self.service:
            logger.warning("Google Drive service not initialized")
            return None

        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=1
            ).execute()

            files = results.get('files', [])
            if files:
                folder_id = files[0]['id']
                self.folder_cache[folder_name] = folder_id
                return folder_id

            logger.warning(f"Folder '{folder_name}' not found, creating it...")
            return self.create_folder(folder_name)
        except Exception as e:
            logger.error(f"Error finding folder {folder_name}: {e}")
            return None

    def create_folder(self, folder_name: str) -> Optional[str]:
        """Create a folder in Google Drive"""
        if not self.service:
            return None
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            folder_id = folder.get('id')
            if folder_id:
                self.folder_cache[folder_name] = folder_id
                logger.info(f"Created Drive folder '{folder_name}' with ID: {folder_id}")
            return folder_id
        except Exception as e:
            logger.error(f"Error creating folder {folder_name}: {e}")
            return None

    def upload_file(self, file_content: bytes, filename: str, folder_id: str) -> Optional[str]:
        """Upload file to Drive. Retries once on failure with token refresh."""
        if not self.service:
            logger.warning("Google Drive service not initialized")
            return None

        for attempt in range(2):
            try:
                file_metadata = {'name': filename, 'parents': [folder_id]}
                media = MediaIoBaseUpload(BytesIO(file_content), mimetype='text/markdown')

                file = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, webViewLink'
                ).execute()

                file_id = file.get('id')
                file_link = file.get('webViewLink')

                if file_id:
                    self._set_file_sharing(file_id)

                logger.info(f"Uploaded {filename} to Drive: {file_link}")
                return file_link
            except Exception as e:
                logger.error(f"Error uploading {filename} to Drive (attempt {attempt + 1}/2): {e}")
                if attempt == 0:
                    # Retry: rebuild credentials and service
                    try:
                        logger.info("Refreshing Drive credentials and retrying...")
                        credentials = OAuthCredentials(
                            token=None,
                            refresh_token=self.refresh_token,
                            token_uri='https://oauth2.googleapis.com/token',
                            client_id=self.client_id,
                            client_secret=self.client_secret,
                            scopes=['https://www.googleapis.com/auth/drive']
                        )
                        credentials.refresh(GoogleAuthRequest())
                        self.service = build('drive', 'v3', credentials=credentials)
                        logger.info("Drive service rebuilt successfully")
                        time.sleep(2)
                    except Exception as refresh_error:
                        logger.error(f"Failed to refresh Drive credentials: {refresh_error}")
                        return None
        return None

    def download_file(self, file_id: str) -> Optional[bytes]:
        """Download file content from Drive by file ID."""
        if not self.service:
            logger.warning("Google Drive service not initialized")
            return None
        try:
            request = self.service.files().get_media(fileId=file_id)
            content = request.execute()
            return content
        except Exception as e:
            logger.error(f"Error downloading file {file_id}: {e}")
            return None

    def _set_file_sharing(self, file_id: str):
        """Set file to be shareable via link"""
        if not self.service:
            return

        try:
            self.service.permissions().create(
                fileId=file_id,
                body={'type': 'anyone', 'role': 'reader'},
                fields='id'
            ).execute()
        except Exception as e:
            logger.error(f"Error setting file sharing for {file_id}: {e}")


class DocumentBuilder:
    """Build markdown documents and Notion blocks from content"""

    @staticmethod
    def parse_content_to_blocks(content: str) -> List[Dict]:
        """Parse Claude's response into structured blocks for Notion and docx"""
        blocks = []
        lines = content.split('\n')
        current_paragraph = []

        for line in lines:
            stripped = line.strip()

            if not stripped:
                if current_paragraph:
                    blocks.append({
                        'type': 'paragraph',
                        'text': '\n'.join(current_paragraph)
                    })
                    current_paragraph = []
                blocks.append({'type': 'empty'})
            elif stripped.startswith('# '):
                if current_paragraph:
                    blocks.append({
                        'type': 'paragraph',
                        'text': '\n'.join(current_paragraph)
                    })
                    current_paragraph = []
                blocks.append({'type': 'heading_1', 'text': stripped[2:].strip()})
            elif stripped.startswith('## '):
                if current_paragraph:
                    blocks.append({
                        'type': 'paragraph',
                        'text': '\n'.join(current_paragraph)
                    })
                    current_paragraph = []
                blocks.append({'type': 'heading_2', 'text': stripped[3:].strip()})
            elif stripped.startswith('### '):
                if current_paragraph:
                    blocks.append({
                        'type': 'paragraph',
                        'text': '\n'.join(current_paragraph)
                    })
                    current_paragraph = []
                blocks.append({'type': 'heading_3', 'text': stripped[4:].strip()})
            elif stripped.startswith('- '):
                if current_paragraph:
                    blocks.append({
                        'type': 'paragraph',
                        'text': '\n'.join(current_paragraph)
                    })
                    current_paragraph = []
                blocks.append({'type': 'bullet', 'text': stripped[2:].strip()})
            elif stripped.startswith('* '):
                if current_paragraph:
                    blocks.append({
                        'type': 'paragraph',
                        'text': '\n'.join(current_paragraph)
                    })
                    current_paragraph = []
                blocks.append({'type': 'bullet', 'text': stripped[2:].strip()})
            elif any(stripped.startswith(f"{i}. ") for i in range(1, 10)):
                if current_paragraph:
                    blocks.append({
                        'type': 'paragraph',
                        'text': '\n'.join(current_paragraph)
                    })
                    current_paragraph = []
                text = stripped.split('. ', 1)[1] if '. ' in stripped else stripped
                blocks.append({'type': 'numbered', 'text': text})
            else:
                current_paragraph.append(line)

        if current_paragraph:
            blocks.append({
                'type': 'paragraph',
                'text': '\n'.join(current_paragraph)
            })

        return blocks

    @staticmethod
    def sanitize_for_compatibility(text: str) -> str:
        """Replace Unicode special characters with ASCII-safe equivalents.

        Some apps (e.g. ElevenReader on Android) misread UTF-8 multi-byte
        characters as Latin-1, producing garbled output like 'â€"' instead
        of '—'. This replaces common Unicode characters with plain ASCII
        equivalents so the file reads correctly everywhere.
        """
        replacements = {
            # Dashes
            '\u2014': '--',   # em dash —
            '\u2013': '-',    # en dash –
            '\u2012': '-',    # figure dash ‒
            '\u2015': '--',   # horizontal bar ―
            # Quotes
            '\u2018': "'",    # left single quote '
            '\u2019': "'",    # right single quote '
            '\u201C': '"',    # left double quote "
            '\u201D': '"',    # right double quote "
            '\u201A': ',',    # single low-9 quote ‚
            '\u201E': '"',    # double low-9 quote „
            # Ellipsis
            '\u2026': '...',  # horizontal ellipsis …
            # Spaces
            '\u00A0': ' ',    # non-breaking space
            '\u2002': ' ',    # en space
            '\u2003': ' ',    # em space
            '\u2009': ' ',    # thin space
            # Bullets / symbols
            '\u2022': '-',    # bullet •
            '\u2023': '>',    # triangular bullet ‣
            '\u25AA': '-',    # black small square ▪
            '\u25CF': '-',    # black circle ●
            # Arrows
            '\u2192': '->',   # rightwards arrow →
            '\u2190': '<-',   # leftwards arrow ←
            '\u21D2': '=>',   # rightwards double arrow ⇒
            # Math
            '\u00D7': 'x',    # multiplication sign ×
            '\u00F7': '/',    # division sign ÷
            '\u2248': '~',    # almost equal to ≈
            '\u2260': '!=',   # not equal to ≠
            '\u2264': '<=',   # less-than or equal to ≤
            '\u2265': '>=',   # greater-than or equal to ≥
            # Other
            '\u00A9': '(c)',  # copyright ©
            '\u00AE': '(R)',  # registered ®
            '\u2122': '(TM)', # trademark ™
        }
        for unicode_char, ascii_equiv in replacements.items():
            text = text.replace(unicode_char, ascii_equiv)
        return text

    @staticmethod
    def create_markdown(content: str, topic: str) -> bytes:
        """Create a markdown (.md) file from content.

        Adds a title header and generation timestamp, then includes
        Claude's markdown content as-is (markdown formatting is native).
        Sanitizes Unicode characters for maximum app compatibility.
        Returns UTF-8 encoded bytes with BOM for proper encoding detection.
        """
        # Sanitize content for cross-app compatibility
        content = DocumentBuilder.sanitize_for_compatibility(content)
        topic = DocumentBuilder.sanitize_for_compatibility(topic)

        # Build markdown document with title and timestamp
        doc_heading = f"# Established Truth, Principles of {topic}"
        timestamp = f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"

        markdown_content = f"{doc_heading}\n\n{timestamp}\n\n---\n\n{content}\n\n---\n"

        # UTF-8 BOM signals encoding to apps that don't auto-detect UTF-8
        return b'\xef\xbb\xbf' + markdown_content.encode('utf-8')

    @staticmethod
    def convert_to_notion_blocks(content: str) -> List[Dict]:
        """Convert content to Notion block format"""
        blocks = []
        notion_blocks = DocumentBuilder.parse_content_to_blocks(content)

        for block in notion_blocks:
            if block['type'] == 'heading_1':
                blocks.append({
                    "object": "block",
                    "type": "heading_1",
                    "heading_1": {
                        "rich_text": [{"type": "text", "text": {"content": block['text']}}]
                    }
                })
            elif block['type'] == 'heading_2':
                blocks.append({
                    "object": "block",
                    "type": "heading_2",
                    "heading_2": {
                        "rich_text": [{"type": "text", "text": {"content": block['text']}}]
                    }
                })
            elif block['type'] == 'heading_3':
                blocks.append({
                    "object": "block",
                    "type": "heading_3",
                    "heading_3": {
                        "rich_text": [{"type": "text", "text": {"content": block['text']}}]
                    }
                })
            elif block['type'] == 'paragraph':
                text = block['text'].strip()
                if text:
                    blocks.append({
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"type": "text", "text": {"content": text}}]
                        }
                    })
            elif block['type'] == 'bullet':
                blocks.append({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{"type": "text", "text": {"content": block['text']}}]
                    }
                })
            elif block['type'] == 'numbered':
                blocks.append({
                    "object": "block",
                    "type": "numbered_list_item",
                    "numbered_list_item": {
                        "rich_text": [{"type": "text", "text": {"content": block['text']}}]
                    }
                })
            elif block['type'] == 'empty':
                blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": []}
                })

        return blocks


class AutomationEngine:
    """Main automation engine"""

    def __init__(self):
        self.notion = NotionAPI(NOTION_API_KEY)
        self.claude = ClaudeAPI(CLAUDE_API_KEY, CLAUDE_MODEL)
        self.drive = GoogleDriveAPI(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN)
        self.last_run = None
        self.operation_status = "Idle"
        self.health_score = 100
        self.last_cycle_succeeded = True
        self.last_cycle_attempted = 0
        self.last_cycle_succeeded_count = 0
        self.claude_healthy = True
        self.drive_healthy = True
        self.notion_healthy = True
        self.cost_month = datetime.now().strftime("%Y-%m")  # Track which month the cost belongs to
        self.last_doc_gen_status = "Awaiting first cycle"
        self.last_daily_comp_status = "Awaiting first run"

        # Restore counters from registry so they survive deploys
        self.monthly_cost = 0.0
        self.total_processed = 0
        try:
            registry_page = self.notion.get_page(REGISTRY_PAGE_ID)
            if registry_page:
                props = registry_page.get("properties", {})
                saved_total = props.get(COLUMNS["REGISTRY_TOTAL"], {}).get("number")
                saved_cost = props.get(COLUMNS["REGISTRY_COST"], {}).get("number")
                if saved_total is not None:
                    self.total_processed = saved_total
                    logger.info(f"Restored total_processed from registry: {self.total_processed}")
                if saved_cost is not None:
                    # Reset cost if we've crossed into a new month
                    current_month = datetime.now().strftime("%Y-%m")
                    self.monthly_cost = saved_cost
                    logger.info(f"Restored monthly_cost from registry: {self.monthly_cost}")
        except Exception as e:
            logger.warning(f"Could not restore counters from registry: {e}. Starting from 0.")

    def process_unprocessed_entries(self):
        """Main processing loop - runs every 5 minutes"""
        logger.info("Starting main processing cycle...")
        self.operation_status = "Processing"
        self.last_run = datetime.now()

        try:
            # Query unprocessed entries that have a title (skip empty rows)
            filter_dict = {
                "and": [
                    {
                        "property": COLUMNS["PROCESSED"],
                        "checkbox": {"equals": False}
                    },
                    {
                        "property": COLUMNS["TITLE"],
                        "title": {"is_not_empty": True}
                    }
                ]
            }

            entries = self.notion.query_database(NOTION_DATABASE_ID, filter_dict, page_size=BATCH_SIZE, max_results=BATCH_SIZE)

            if not entries:
                logger.info("No unprocessed entries found")
                self.operation_status = "Idle"
                return

            logger.info(f"Found {len(entries)} unprocessed entries, processing up to {BATCH_SIZE}")

            # Get ALL existing umbrella terms from the database schema (select field options)
            try:
                db_schema = self.notion.get_database(NOTION_DATABASE_ID)
                umbrella_prop = db_schema.get("properties", {}).get(COLUMNS["UMBRELLA_TERM"], {})
                schema_terms = [opt.get("name") for opt in umbrella_prop.get("select", {}).get("options", []) if opt.get("name")]
                existing_terms = list(set(SEED_UMBRELLA_TERMS + schema_terms))
                logger.info(f"Umbrella terms available: {len(existing_terms)} ({len(schema_terms)} from DB, {len(SEED_UMBRELLA_TERMS)} seed)")
            except Exception as e:
                logger.warning(f"Could not fetch umbrella terms from DB schema: {e}. Using seed terms only.")
                existing_terms = list(set(SEED_UMBRELLA_TERMS))

            # Process each entry
            attempted = min(len(entries), BATCH_SIZE)
            succeeded = 0
            for i, entry in enumerate(entries[:BATCH_SIZE]):
                try:
                    success = self._process_entry(entry, existing_terms)
                    if success:
                        self.total_processed += 1
                        succeeded += 1
                    else:
                        logger.warning(f"Entry {i+1}/{attempted} failed — will be retried next cycle")
                except Exception as e:
                    logger.error(f"Error processing entry {i+1}/{attempted}: {e}")
                    self.health_score = max(0, self.health_score - 10)

            # Recover health score on successful cycles
            if succeeded == attempted and attempted > 0:
                self.health_score = min(100, self.health_score + 10)
            elif succeeded > 0:
                self.health_score = min(100, self.health_score + 5)

            self.operation_status = "Idle"
            self._update_registry()
            logger.info(f"Processing cycle complete. {succeeded}/{attempted} entries succeeded. Health: {self.health_score}")

        except Exception as e:
            logger.error(f"Error in processing cycle: {e}")
            self.operation_status = "Error"
            self.health_score = max(0, self.health_score - 20)
            self._update_registry()  # Write the error state to registry so it's visible

    def _process_entry(self, entry: Dict, existing_terms: List[str]) -> bool:
        """Process a single entry. Returns True on success, False on failure."""
        entry_id = entry.get("id")
        topic = self.notion.get_property_value(entry, COLUMNS["TITLE"])

        if not topic:
            logger.warning(f"Entry {entry_id} has no title")
            return False

        logger.info(f"Processing: {topic}")

        # Step 1: Generate document content
        content, usage = self.claude.generate_document(topic)
        if not content:
            logger.error(f"Failed to generate content for {topic}")
            self.last_doc_gen_status = f"Failed — {topic[:60]} (Claude generation failed)"
            return False

        # Calculate cost
        input_cost = (usage["input_tokens"] / 1_000_000) * CLAUDE_INPUT_COST_PER_M
        output_cost = (usage["output_tokens"] / 1_000_000) * CLAUDE_OUTPUT_COST_PER_M
        cost = input_cost + output_cost
        self.monthly_cost += cost

        # Step 2: Assign umbrella term
        umbrella_term, _ = self.claude.assign_umbrella_term(topic, existing_terms)
        if not umbrella_term:
            umbrella_term = "Esotericism"  # Default fallback

        # Step 3: Create markdown file
        md_bytes = DocumentBuilder.create_markdown(content, topic)

        # Step 4: Upload to Drive — use direct folder ID if set, otherwise search by name
        folder_id = DRIVE_FOLDER_ID if DRIVE_FOLDER_ID else self.drive.find_folder(DRIVE_FOLDER_NAME)
        if not folder_id:
            logger.error(f"Could not find Drive folder {DRIVE_FOLDER_NAME}")
            return False

        # Document naming: 🧠 Established Truth, Principles of (TOPIC).md
        # If topic is too long for filename, use Claude to create a meaningful condensed title
        doc_title_prefix = "🧠 Established Truth, Principles of "
        max_topic_len = MAX_TITLE_LENGTH - len(doc_title_prefix) - 3  # 3 for ".md"
        if len(topic) > max_topic_len:
            # Topic exceeds limit - create all-encompassing meaning title within limits
            condensed, _ = self.claude._call_api(
                [{"role": "user", "content": f'The following topic title is too long for a filename. Condense it to under {max_topic_len} characters while preserving the full meaning and essence of the topic. Do NOT make it generic or cliche — it must be all-encompassing of what the original means. Respond with ONLY the condensed title, nothing else.\n\nOriginal: {topic}'}],
                system="You condense titles while preserving their complete meaning.",
                max_tokens=100
            )
            display_topic = condensed.strip() if condensed else topic[:max_topic_len]
        else:
            display_topic = topic
        filename = f"{doc_title_prefix}{display_topic}.md".replace('/', '_').replace('\\', '_')
        drive_link = self.drive.upload_file(md_bytes, filename, folder_id)
        if not drive_link:
            logger.error(f"Failed to upload document for {topic}")
            self.last_doc_gen_status = f"Failed — {topic[:60]} (Drive upload failed)"
            return False

        # Step 5: Update Notion entry - CRITICAL: only mark as processed after Drive upload succeeded
        properties = {
            COLUMNS["PROCESSED"]: {"checkbox": True},
            COLUMNS["LINK"]: {"url": drive_link},
            COLUMNS["UMBRELLA_TERM"]: {"select": {"name": umbrella_term}},
            COLUMNS["DOC_DATE"]: {"date": {"start": datetime.now().strftime("%Y-%m-%d")}},
            COLUMNS["QUOTA_SOURCE"]: {"select": {"name": GOOGLE_ACCOUNT}}
        }

        update_success = self.notion.update_page_properties(entry_id, properties)
        if not update_success:
            logger.error(f"CRITICAL: Document uploaded to Drive but Notion update FAILED for {topic}. Drive link: {drive_link}")
            # Retry once
            time.sleep(2)
            update_success = self.notion.update_page_properties(entry_id, properties)
            if not update_success:
                logger.error(f"CRITICAL: Notion update retry also FAILED for {topic}. Manual intervention needed. Drive link: {drive_link}")
                return False

        # Step 6: Append content blocks to Notion page (in batches of 95)
        notion_blocks = DocumentBuilder.convert_to_notion_blocks(content)
        for i in range(0, len(notion_blocks), 95):
            batch = notion_blocks[i:i+95]
            self.notion.append_block_children(entry_id, batch)
            time.sleep(0.5)  # Rate limiting

        logger.info(f"Successfully processed {topic} - Umbrella: {umbrella_term}, Cost: ${cost:.4f}")
        self.last_doc_gen_status = f"Success — {topic[:60]} ({len(content):,} chars)"
        return True

    def _extract_rich_text(self, rich_text_array: List[Dict]) -> str:
        """Extract plain text from a Notion rich_text array, preserving markdown formatting.
        Concatenates all text segments. Handles empty arrays gracefully."""
        if not rich_text_array:
            return ""
        parts = []
        for segment in rich_text_array:
            text = segment.get("text", {}).get("content", "") if segment.get("type") == "text" else segment.get("plain_text", "")
            if not text:
                text = segment.get("plain_text", "")
            parts.append(text)
        return "".join(parts)

    def _blocks_to_markdown(self, blocks: List[Dict]) -> str:
        """Convert a list of Notion blocks back to markdown text.
        Handles: heading_1, heading_2, heading_3, paragraph, bulleted_list_item,
        numbered_list_item, divider, and empty paragraphs."""
        lines = []
        for block in blocks:
            block_type = block.get("type", "")
            try:
                if block_type == "heading_1":
                    text = self._extract_rich_text(block["heading_1"].get("rich_text", []))
                    lines.append(f"# {text}\n")
                elif block_type == "heading_2":
                    text = self._extract_rich_text(block["heading_2"].get("rich_text", []))
                    lines.append(f"## {text}\n")
                elif block_type == "heading_3":
                    text = self._extract_rich_text(block["heading_3"].get("rich_text", []))
                    lines.append(f"### {text}\n")
                elif block_type == "paragraph":
                    text = self._extract_rich_text(block["paragraph"].get("rich_text", []))
                    lines.append(f"{text}\n")
                elif block_type == "bulleted_list_item":
                    text = self._extract_rich_text(block["bulleted_list_item"].get("rich_text", []))
                    lines.append(f"- {text}")
                elif block_type == "numbered_list_item":
                    text = self._extract_rich_text(block["numbered_list_item"].get("rich_text", []))
                    lines.append(f"1. {text}")
                elif block_type == "divider":
                    lines.append("---\n")
                else:
                    # Unknown block type — skip silently
                    pass
            except Exception as e:
                logger.warning(f"Error converting block type {block_type}: {e}")
                continue
        return "\n".join(lines)

    def _update_daily_comp_status(self, status: str):
        """Write daily compilation status to registry immediately so it's visible even if function crashes."""
        self.last_daily_comp_status = status
        try:
            self.notion.update_page_properties(REGISTRY_PAGE_ID, {
                COLUMNS["REGISTRY_DAILY_COMP"]: {
                    "rich_text": [{"type": "text", "text": {"content": status[:200]}}]
                }
            })
        except Exception:
            pass  # Don't let status update failure crash the compilation

    def compile_daily_documents(self):
        """Compile daily documents - run at 11:55 PM.
        Downloads the actual markdown files from Google Drive for each document
        processed today and compiles them into one continuous file for ElevenReader."""
        logger.info("Starting daily compilation...")
        self._update_daily_comp_status("Running — querying Notion...")

        try:
            # Query entries processed today
            today = datetime.now().strftime("%Y-%m-%d")
            filter_dict = {
                "and": [
                    {
                        "property": COLUMNS["DOC_DATE"],
                        "date": {"on_or_after": today}
                    },
                    {
                        "property": COLUMNS["PROCESSED"],
                        "checkbox": {"equals": True}
                    }
                ]
            }

            logger.info(f"Daily compilation querying Notion for date: {today}")
            entries = self.notion.query_database(NOTION_DATABASE_ID, filter_dict, page_size=100)

            if not entries:
                logger.info("No entries processed today for daily compilation")
                self._update_daily_comp_status(f"No documents to compile ({today})")
                return

            logger.info(f"Found {len(entries)} entries for daily compilation")
            self._update_daily_comp_status(f"Running — found {len(entries)} entries, downloading from Drive...")

            # Format the date
            today_display = datetime.now().strftime("%B %d, %Y").replace(" 0", " ")
            daily_doc_title = f"Established Daily Document -- {today_display}"

            # Compile all documents by downloading markdown directly from Google Drive
            all_content = f"# {daily_doc_title}\n\n**Documents Compiled:** {len(entries)}\n\n---\n\n"

            compiled_count = 0
            for entry in entries:
                topic = self.notion.get_property_value(entry, COLUMNS["TITLE"])
                link = self.notion.get_property_value(entry, COLUMNS["LINK"])

                if not topic or not link:
                    continue

                logger.info(f"Downloading from Drive: {topic[:60]}")

                # Extract file ID from the Drive link
                file_id = None
                if link and "/d/" in link:
                    file_id = link.split("/d/")[1].split("/")[0]

                if file_id:
                    try:
                        file_bytes = self.drive.download_file(file_id)
                        if file_bytes:
                            # Decode the markdown content
                            file_text = file_bytes.decode("utf-8-sig", errors="replace")
                            all_content += file_text
                            all_content += "\n\n---\n\n"
                            compiled_count += 1
                            logger.info(f"  Downloaded: {len(file_text):,} chars")
                        else:
                            logger.warning(f"  Download returned empty for: {topic[:60]}")
                            all_content += f"# Established Truth, Principles of {topic}\n\n(Content could not be downloaded from Drive)\n\n---\n\n"
                            compiled_count += 1
                    except Exception as e:
                        logger.error(f"  Error downloading {topic[:60]}: {e}")
                        all_content += f"# Established Truth, Principles of {topic}\n\n(Download error: {e})\n\n---\n\n"
                        compiled_count += 1
                else:
                    logger.warning(f"  No valid Drive file ID for: {topic[:60]}")
                    all_content += f"# Established Truth, Principles of {topic}\n\n(No Drive link available)\n\n---\n\n"
                    compiled_count += 1

                time.sleep(0.5)  # Brief pause between downloads

            logger.info(f"Compiled content from {compiled_count} documents")
            self._update_daily_comp_status(f"Running — {compiled_count} docs compiled, uploading to Drive...")

            # Sanitize for ElevenReader compatibility
            all_content = DocumentBuilder.sanitize_for_compatibility(all_content)

            # Create markdown file with UTF-8 BOM
            content_with_bom = '\ufeff' + all_content
            md_bytes = content_with_bom.encode('utf-8')

            # Upload to Drive
            folder_id = DAILY_DRIVE_FOLDER_ID if DAILY_DRIVE_FOLDER_ID else self.drive.find_folder(DAILY_DRIVE_FOLDER_NAME)
            if not folder_id:
                logger.error(f"Could not find Drive folder {DAILY_DRIVE_FOLDER_NAME}")
                self._update_daily_comp_status(f"Failed — Drive folder not found")
                return

            filename = f"{daily_doc_title}.md"
            logger.info(f"Uploading daily compilation: {filename} ({len(md_bytes):,} bytes)")
            drive_link = self.drive.upload_file(md_bytes, filename, folder_id)

            if not drive_link:
                logger.error("Failed to upload daily compilation")
                self._update_daily_comp_status(f"Failed — Drive upload returned no link ({len(md_bytes):,} bytes, {compiled_count} docs)")
                return

            logger.info(f"Daily compilation uploaded: {drive_link}")
            self._update_daily_comp_status(f"Running — uploaded, creating Notion entry...")

            # Create Notion entry in Daily Documents DB
            properties = {
                COLUMNS["DAILY_TITLE"]: {"title": [{"type": "text", "text": {"content": daily_doc_title}}]},
                COLUMNS["DAILY_DATE"]: {"date": {"start": today}},
                COLUMNS["DAILY_COUNT"]: {"number": compiled_count},
                COLUMNS["DAILY_LINK"]: {"url": drive_link},
                COLUMNS["DAILY_STATUS"]: {"select": {"name": "Complete"}}
            }

            logger.info(f"Creating Notion entry in Daily Documents DB...")
            self.notion.create_page(DAILY_DOCUMENTS_DB_ID, properties)
            self._update_daily_comp_status(f"Complete — {compiled_count} docs ({today_display})")
            logger.info(f"Daily compilation COMPLETE: {compiled_count} documents, {len(md_bytes):,} bytes")

        except Exception as e:
            self._update_daily_comp_status(f"Failed — {type(e).__name__}: {str(e)[:80]}")
            logger.error(f"DAILY COMPILATION FAILED: {e}", exc_info=True)

    def compile_umbrella_term_documents(self, umbrella_term: str):
        """Compile documents for a specific umbrella term"""
        logger.info(f"Starting umbrella term compilation for: {umbrella_term}")

        try:
            # Query un-utilized entries for this umbrella term
            filter_dict = {
                "and": [
                    {
                        "property": COLUMNS["UMBRELLA_TERM"],
                        "select": {"equals": umbrella_term}
                    },
                    {
                        "property": COLUMNS["UTILIZED_UMBRELLA"],
                        "checkbox": {"equals": False}
                    }
                ]
            }

            entries = self.notion.query_database(NOTION_DATABASE_ID, filter_dict, page_size=100)

            if not entries:
                logger.info(f"No un-utilized entries for {umbrella_term}")
                return

            logger.info(f"Found {len(entries)} entries for {umbrella_term}")

            # Compile all documents
            all_content = f"Umbrella Term: {umbrella_term}\n\n"
            for entry in entries:
                topic = self.notion.get_property_value(entry, COLUMNS["TITLE"])
                link = self.notion.get_property_value(entry, COLUMNS["LINK"])
                if topic and link:
                    all_content += f"## {topic}\n[View Document]({link})\n\n"

            # Create markdown file
            md_bytes = DocumentBuilder.create_markdown(all_content, f"Umbrella Term: {umbrella_term}")

            # Upload to Drive — use direct folder ID if set, otherwise search by name
            folder_id = UMBRELLA_DRIVE_FOLDER_ID if UMBRELLA_DRIVE_FOLDER_ID else self.drive.find_folder(UMBRELLA_DRIVE_FOLDER_NAME)
            if not folder_id:
                logger.error(f"Could not find Drive folder {UMBRELLA_DRIVE_FOLDER_NAME}")
                return

            filename = f"UmbrellaTermCompilation_{umbrella_term.replace(' ', '_')}.md"
            drive_link = self.drive.upload_file(md_bytes, filename, folder_id)

            if not drive_link:
                logger.error(f"Failed to upload umbrella term compilation for {umbrella_term}")
                return

            # Create Notion entry in Umbrella Term Documents DB
            properties = {
                COLUMNS["UTD_TITLE"]: {"title": [{"type": "text", "text": {"content": f"Compilation - {umbrella_term}"}}]},
                COLUMNS["UTD_UMBRELLA"]: {"select": {"name": umbrella_term}},
                COLUMNS["UTD_DATE"]: {"date": {"start": datetime.now().strftime("%Y-%m-%d")}},
                COLUMNS["UTD_COUNT"]: {"number": len(entries)},
                COLUMNS["UTD_LINK"]: {"url": drive_link},
                COLUMNS["UTD_STATUS"]: {"select": {"name": "Complete"}}
            }

            self.notion.create_page(UMBRELLA_TERM_DOCUMENTS_DB_ID, properties)

            # Mark entries as utilized
            for entry in entries:
                entry_id = entry.get("id")
                self.notion.update_page_properties(entry_id, {
                    COLUMNS["UTILIZED_UMBRELLA"]: {"checkbox": True}
                })

            logger.info(f"Umbrella term compilation complete for {umbrella_term}: {len(entries)} documents")

        except Exception as e:
            logger.error(f"Error in umbrella term compilation: {e}")

    def _check_service_health(self):
        """Actually test each service and return live health status."""
        health = {}

        # Check Claude API
        try:
            test_resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={"model": CLAUDE_MODEL, "max_tokens": 5, "messages": [{"role": "user", "content": "ok"}]},
                timeout=15
            )
            health["Claude API"] = "Healthy" if test_resp.status_code == 200 else f"Error ({test_resp.status_code})"
        except Exception as e:
            health["Claude API"] = f"Down ({type(e).__name__})"

        # Check Google Drive — verify the service exists and can list the target folder
        try:
            if self.drive.service:
                test = self.drive.service.files().get(fileId=DRIVE_FOLDER_ID, fields="id").execute()
                health["Google Drive"] = "Healthy" if test.get("id") else "Folder Not Found"
            else:
                health["Google Drive"] = "Service Not Initialized"
        except Exception as e:
            health["Google Drive"] = f"Down ({type(e).__name__})"

        # Check Notion API (we know it works if we got this far since _update_registry reads the registry page,
        # but let's verify the main database is accessible)
        try:
            test_resp = requests.post(
                f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                headers=self.notion.headers,
                json={"page_size": 1},
                timeout=10
            )
            health["Notion API"] = "Healthy" if test_resp.status_code == 200 else f"Error ({test_resp.status_code})"
        except Exception as e:
            health["Notion API"] = f"Down ({type(e).__name__})"

        return health

    def _update_registry(self):
        """Update the operations registry with live, truthful values."""
        try:
            registry_page = self.notion.get_page(REGISTRY_PAGE_ID)
            if not registry_page:
                logger.warning("Could not find registry page")
                return

            # Reset monthly cost at month boundary
            current_month = datetime.now().strftime("%Y-%m")
            if current_month != self.cost_month:
                logger.info(f"New month detected ({self.cost_month} -> {current_month}). Resetting monthly cost.")
                self.monthly_cost = 0.0
                self.cost_month = current_month

            # Check actual service health
            health = self._check_service_health()
            all_healthy = all(v == "Healthy" for v in health.values())
            health_text = " | ".join(f"{k}: {v}" for k, v in health.items())
            health_text += f" | Health Score: {self.health_score}"

            # Determine true status — uses existing Notion select options: Active, Paused, Error, Decommissioned, Setting Up
            if self.health_score <= 0 or not all_healthy:
                status = "Error"
            elif self.health_score < 80:
                status = "Paused"  # Degraded performance — needs attention
            else:
                status = "Active"

            properties = {
                COLUMNS["REGISTRY_STATUS"]: {
                    "select": {"name": status}
                },
                COLUMNS["REGISTRY_MODEL"]: {
                    "multi_select": [{"name": CLAUDE_MODEL}]
                },
                COLUMNS["REGISTRY_LAST_RUN"]: {
                    "date": {"start": self.last_run.strftime("%Y-%m-%dT%H:%M:%S")} if self.last_run else None
                },
                COLUMNS["REGISTRY_COST"]: {
                    "number": round(self.monthly_cost, 2)
                },
                COLUMNS["REGISTRY_TOTAL"]: {
                    "number": self.total_processed
                },
                COLUMNS["REGISTRY_HEALTH"]: {
                    "rich_text": [{"type": "text", "text": {"content": health_text}}]
                },
                COLUMNS["REGISTRY_FREQUENCY"]: {
                    "rich_text": [{"type": "text", "text": {"content": f"Every {PROCESSING_INTERVAL_MINUTES} minutes (BATCH_SIZE={BATCH_SIZE})"}}]
                },
                COLUMNS["REGISTRY_CLAUDE_STATUS"]: {
                    "rich_text": [{"type": "text", "text": {"content": health.get("Claude API", "Unknown")}}]
                },
                COLUMNS["REGISTRY_DRIVE_STATUS"]: {
                    "rich_text": [{"type": "text", "text": {"content": health.get("Google Drive", "Unknown")}}]
                },
                COLUMNS["REGISTRY_NOTION_STATUS"]: {
                    "rich_text": [{"type": "text", "text": {"content": health.get("Notion API", "Unknown")}}]
                },
                COLUMNS["REGISTRY_DOC_GEN"]: {
                    "rich_text": [{"type": "text", "text": {"content": self.last_doc_gen_status[:200]}}]
                },
                COLUMNS["REGISTRY_DAILY_COMP"]: {
                    "rich_text": [{"type": "text", "text": {"content": self.last_daily_comp_status[:200]}}]
                },
                COLUMNS["REGISTRY_HEALTH_SCORE"]: {
                    "number": self.health_score
                }
            }

            success = self.notion.update_page_properties(REGISTRY_PAGE_ID, properties)
            if success:
                logger.info(f"Registry updated — Status: {status} | {health_text}")
            else:
                logger.warning("Registry update returned failure")
        except Exception as e:
            logger.error(f"Error updating registry: {e}")

    def health_check(self) -> Dict:
        """Health check endpoint"""
        return {
            "status": "healthy" if self.health_score >= 50 else "degraded",
            "operation": OPERATION_NAME,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "monthly_cost": round(self.monthly_cost, 2),
            "total_processed": self.total_processed,
            "health_score": self.health_score,
            "operation_status": self.operation_status
        }


# Global automation engine
engine = None


def start_scheduler():
    """Start APScheduler for automation tasks"""
    global engine
    engine = AutomationEngine()

    scheduler = BackgroundScheduler(daemon=True)

    # Main processing every 5 minutes
    scheduler.add_job(
        engine.process_unprocessed_entries,
        'interval',
        minutes=PROCESSING_INTERVAL_MINUTES,
        id='main_processing',
        name='Main Processing Loop'
    )

    # Daily compilation at 11:55 PM
    scheduler.add_job(
        engine.compile_daily_documents,
        'cron',
        hour=23,
        minute=55,
        id='daily_compilation',
        name='Daily Compilation'
    )

    scheduler.start()
    logger.info("Scheduler started successfully")


def get_health():
    """Health check handler"""
    global engine
    if engine is None:
        return {"status": "not_ready", "message": "Automation engine not initialized"}
    return engine.health_check()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "health":
        # Quick health check
        engine = AutomationEngine()
        print(json.dumps(get_health(), indent=2))
    else:
        # Start scheduler
        start_scheduler()

        # Keep the process alive
        try:
            import signal
            def signal_handler(sig, frame):
                logger.info("Shutting down gracefully...")
                sys.exit(0)

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            sys.exit(0)
