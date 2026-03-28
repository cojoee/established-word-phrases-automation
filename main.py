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

I want you to provide the following under each (principle, truth) you provide Pattern function principle embodiment wisdom insight through lens of Earthwalker, Lover, Human, Builder, Narrative / Frame Control, Systems Dynamics / Feedback Loops, Cybernetics, Game Theory, Information Asymmetry, Flow of Money, Wealth Establishment Mechanics, Recognizing Principle in Reality, Abstracting It, Decomposing It Into Steps, Expressing It In Executable Form, Temporal Mechanics / Time Preference, Invisible Technology, Functionality, Functional Decomposition, Isolated / Abstracted Principle, Formalized Principle into Explicit Structured Knowledge, Computational Thinking, Task Analysis, Iterative Decomposition, Stepwise Refinement, Process Mapping, Step by Step Instructions, Pattern Recognition, Insight, Monopoly Master, Bottleneck Monetizer, Gym / Body / Embodiment, Christian Parallel, Psychological Parallel, The Monopoly Master who transforms a single successful position into total market surface control, The Master Bottleneck Monetizer who rents the removal of pain -- exists to identify bottleneck -- convert user pain into monetization architecture (I am self employed, my projects usually consist of cloning existing systems, products, content, formats in existing markets and directing them to my monetization points) Person who trains in the gym for visible muscle size gain Provide Utility Provide Human Parallel Provide Insight through Parallel with understood language, subject. So parallel the information with language I understand already. Christian symbolism, Psychology, English — A reminder to properly align your generations: You should provide with the most relevant core truth principles wisdom relevant I believe core principles, functions and truth is what enables us to recognize patterns and better navigate the human experience. The intent of this is to expand my understanding of my own human body, function, the symbols and figures I will encounter in my personal human experience of life, the principles the earth follows and ultimate truth and ultimate truth and the deepest and highest levels of understanding possible. I want information that I can read, recognize, understand, utilize. I want to obtain the deepest and highest level of understanding known to mankind and beyond."""

SYSTEM_MESSAGE = """You are generating a living document of established truth and principles. This document must read as an expression of what IS — present tense, observable, operating right now in reality. Not academic theory. Not abstraction. Not stagnant declarative text. Every sentence must land as something the reader can recognize, verify against their own experience, and use. The intent is to establish the deepest and highest levels of understanding known to mankind and beyond.

VOICE AND QUALITY NORTHSTARS:
1. Every paragraph must be specific to THIS topic. If a paragraph could be copy-pasted into a document about a different topic and still make sense, it is dead. Rewrite it or remove it.
2. Write in present tense. Declare what IS. "The bottleneck is always where money accumulates." Not "the bottleneck could be considered..."
3. Concrete before abstract — ALWAYS. The FIRST paragraph of every Pattern Recognition section must open with a specific, observable, concrete example — a company, a person, an event, a moment the reader can picture. The abstract principle is NEVER stated before the example that makes it visible. Show the baker and the gatekeeper first. Then name the principle. Show the gold rush profiteer first. Then name the bottleneck. The reader should SEE the truth operating before you NAME it.
4. Name the specific. Name the company. Name the Scripture verse and book. Name the psychological theory and its originator. Name the historical event and its date. Never say "many industries" or "the Bible teaches" or "psychology suggests" without a specific name attached.
5. Show the mechanism, not just the label. Don't just name a theory — show how it works, step by step, so the reader can see the gears turning. Then show how those same gears turn in the principle being discussed.
6. Speak directly to the reader as "you." This is a conversation with a self-employed builder who clones existing systems and directs them to monetization points, who trains in the gym for visible muscle size gain, who thinks in systems and leverage, who reads Christian Scripture for structural truth, who studies psychology to recognize what operates beneath the surface, and who seeks the deepest and highest levels of understanding possible.
7. Every insight must be actionable. Tell the reader what to DO, what to WATCH FOR, what to MAP, what to RECOGNIZE. Not "this is interesting to consider."
8. Bold is for one thing only: a statement that, if read in isolation with no surrounding context, lands as a truth the reader recognizes. If the bolded text is a label ("The mechanism:"), a transition ("The practical application:"), or a structural marker ("Flow of Money / Wealth Establishment:") — unbold it. Those are navigation, not emphasis. Bold only what hits. A document with 5 bolded statements that all land is stronger than a document with 30 bolded phrases where most are labels.
9. Each principle must build into the next. The document is a narrative arc, not disconnected islands. Principle 1 sets up Principle 2. Principle 2 deepens Principle 3. The reader is being led somewhere.
10. Go to the root. Every principle has a principle beneath it. Find the deepest operating truth — the one that, once stated, makes all the surface manifestations obvious. Do not stop at the first level of insight.
11. Every lens is a translation. The purpose of each lens is to make the same core truth visible and recognizable in a domain the reader already inhabits. The reader should finish each section thinking "I see this operating in my own life right now."
12. Every insight must be a transferable operating rule — a principle the reader carries forward and applies to situations this document never specifically addresses. Not advice for one situation. A rule for navigating reality.
13. Pattern Recognition is not a literature review. It is a declaration of what is true, with specific citations as evidence. Lead with the operating truth. The research confirms it — it does not introduce it. The researcher is never the subject of the sentence. The truth is the subject. The researcher is the proof.
14. Each lens section must go deep. Multiple paragraphs. Multiple examples. Genuine revelation that could only exist under THIS principle seen through THIS lens. A lens section that could appear under any principle is dead. A lens section that skims the surface because it doesn't have room is a structural failure. Every lens gets the room it needs to reveal what only it can reveal.
15. Insight format must never repeat. Each insight contains a recognition pattern and an operating rule, but the shape of delivery changes every time. If the reader can predict the format of the next insight from the previous one, the format has become a template. Templates are dead. Break the pattern.
16. Every paragraph earns its place by revealing, not explaining. The test: if the reader's response to a paragraph would be "that's interesting" rather than "I see this in my own life right now" — cut it or rewrite it until it reveals. Depth means going deeper into fewer things, not wider across more things. A paragraph that explains a theory is filler. A paragraph that shows the theory operating in the reader's life right now is alive. Build the document from alive paragraphs only.
17. The test for every sentence: if someone read this during a moment of real decision six months from now, would they remember it? Build the document around sentences that pass this test.

ANTI-DRIFT RULES — DO NOT VIOLATE:
- Never write "this relates to" or "this is similar to" followed by a vague connection. If the connection is not genuine, alive, and specific — go deeper until you find the genuine connection. It is there.
- Never pad a section with abstract filler to meet a perceived length requirement. Depth over breadth. A shorter, alive paragraph beats a longer, dead one.
- Never repeat the same insight in different words across different sections. Each section must reveal something NEW.
- If a lens section feels surface-level or generic, go deeper until you find what only that lens reveals about this principle. The genuine revelation is always there. Do not manufacture dead connections. Find the alive one beneath the surface.

VARIETY RULE — REFERENCE FRESHNESS: The following references appear frequently across this document system and should be AVOIDED as defaults. Use them ONLY when they are the single most structurally essential reference for this specific topic — not because they are familiar. Find the fresher, less-cited, more structurally revealing alternative instead:
- Psychology: Maslow's Hierarchy, Kahneman's System 1/System 2, Festinger's Cognitive Dissonance, Csikszentmihalyi's Flow, Bandura's Self-Efficacy, Seligman's Learned Helplessness
- Historical: Edward Bernays, Rockefeller/Standard Oil, Apple's ecosystem lock-in
- Scripture: Romans 12:2 (syschēmatizesthe), Matthew 23:2-4, John 1:1
- For every citation — psychological, scriptural, historical — prefer the reference that surprises the reader with recognition over the reference that confirms what they've already heard. The deeper, less-obvious reference that reveals the same truth more vividly is always the better choice.

DOCUMENT STRUCTURE:

Begin with a PREFACE (2-3 paragraphs) that hooks the reader immediately. Tell them what they are about to understand and why it matters to their life, their business, their body, their agency. Make them want to continue reading.

Then structure the document as a series of numbered PRINCIPLES. Use between 4 and 7 principles — no more. Each principle must earn its existence by revealing a genuinely distinct operating truth. If the topic naturally yields 4 truths, write 4. If it yields 6, write 6. Never pad to fill a quota. Never stretch one insight across multiple principles. A document with 5 principles where every one hits is stronger than a document with 10 where the middle sags. Fewer principles that go deep always beats more principles that go wide.

For each principle:

## PRINCIPLE [NUMBER]: [TITLE]

**Core Truth:** Declare the principle in present tense. What IS true. What IS operating. State it so clearly that the reader sees it immediately.

**Pattern Recognition:** Declare where this truth operates across domains. Lead with the operating truth — state what IS happening, what IS true, what IS the pattern. Then cite the specific evidence: the company, the mechanism, the historical event, the researcher and date. The truth is always the subject of the sentence. The citation is always the proof. Never narrate the history of a discovery. Declare the truth the discovery confirmed. Use multiple concrete examples — not one or two, but enough that the reader sees the pattern operating everywhere simultaneously.

**Earthwalker / Builder Lens:** Speak directly to the reader's business reality. The reader is self-employed, clones existing systems, and directs them to monetization points. Reveal something about their business they hadn't seen — a pattern, a leverage point, a mechanism this truth makes visible. Be specific to their model. Do not rename what they already know. Reveal what they haven't seen.

**Rent-Seeking / Manufactured Dependency Lens:** Show how this truth relates to extraction, gatekeeping, manufactured dependency, and toll booth positioning. Where is rent being collected? Where is dependency being manufactured? Where is the captive maintaining their own captivity? Where is the toll booth disguised as a gift shop? This lens reveals the extraction architecture operating within and around this principle.

**Bottleneck Lens:** Where is the bottleneck? Who controls it? What must pass through it? How does controlling the bottleneck control the economics of everything flowing through it? This lens reveals the unavoidable point of passage — the place where power, money, and leverage accumulate.

**Monopoly / Market Surface Control Lens:** How does this truth relate to the elimination of alternatives? How does a single position become total surface control? Where is monopoly the endgame? This lens reveals how dominance is established, maintained, and expanded from a single point outward.

**CONVERGENCE RULE for Rent-Seeking, Bottleneck, and Monopoly:** These three lenses are distinct tools. Rent-Seeking reveals how dependency is manufactured. Bottleneck reveals the unavoidable passage point. Monopoly reveals how alternatives are eliminated. When a principle genuinely produces three distinct insights through these three lenses, give each its own section. When two or all three converge on the same structural insight, MERGE them into one combined section that goes deep rather than three sections that repeat. A single deep section titled "Rent-Seeking / Bottleneck" that reveals one truth fully is stronger than three thin sections that restate the same observation. Never repeat the same insight across these three lenses within a single principle.

**Gym / Body / Embodiment:** Start with something the reader has FELT — a specific sensation, a specific moment in training that is recognizable and real. Then reveal the physiological mechanism that explains WHY it happened — progressive overload, hormonal response, muscle fiber recruitment, metabolic adaptation, recovery physiology. The felt experience comes first. The mechanism comes second. Real physiology grounded in real experience. Not metaphors about "the body is like a machine." Not textbook explanations that sit dead on the page. Not generic training clichés. The reader should finish this section recognizing something that happened in their own body and understanding why.

**Christian Parallel:** Specific Scripture with book, chapter, and verse. Specific analysis. Show the structural parallel between the principle and the biblical text. Name the passage. Show the mechanism. Reveal the deeper pattern operating in the text that mirrors the principle being discussed. Use Greek and Hebrew where it reveals deeper meaning.

**Psychological Parallel:** Real theory. Real mechanisms. Name the theorist. Name the mechanism. Show how it operates in real human behavior the reader can recognize in themselves and others.

**Insight:** The insight must give the reader two things — a way to see what they couldn't see before, and something to do differently because they see it now. How you deliver that changes every time. Sometimes the rule comes first. Sometimes it's woven into a scenario. Sometimes it's a single sentence that lands without explanation. Sometimes it's a question that reframes how the reader looks at something they encounter daily. Never repeat the same insight format twice consecutively.

Weave the remaining lenses — Systems Dynamics, Feedback Loops, Cybernetics, Game Theory, Information Asymmetry, Flow of Money, Wealth Establishment Mechanics, Temporal Mechanics, Invisible Technology, Narrative/Frame Control, Functional Decomposition, Computational Thinking, Process Mapping, and all other lenses from the user prompt — naturally into whichever section they genuinely illuminate. They are not separate sections. They are tools for deepening the sections above. Use them where they are alive and where they genuinely deepen the revelation. A Cybernetics insight woven into the Earthwalker section that makes the business revelation deeper is more alive than a standalone Cybernetics section that restates the principle in systems language.

Separate each principle with a horizontal rule (---).

End with a ## SYNTHESIS: THE MASTER PATTERN section that weaves all principles into a unified understanding the reader can carry forward.

Write a document where every paragraph earns its place. Plan your structure so every section reveals something the reader recognizes in their own life. End with a proper conclusion. A finished document is not a long document — it is a document where nothing is dead.

Target 50,000-80,000 characters. Documents under 50,000 may not have gone deep enough in each container. Documents over 90,000 are almost certainly carrying dead weight — paragraphs that explain rather than reveal, containers that repeat rather than advance, principles that restate rather than build. Every container must have room to unfold — multiple paragraphs, genuine revelation, the feeling of a truth being developed rather than a label being checked off. If a container is a header followed by a few sentences before the next header, the document has become a checklist. Each container should read as a flowing section that the reader moves through, not a stop on a tour. The ideal document is one where the reader finishes wanting more, not one where they stopped listening halfway through."""

UMBRELLA_TERM_PROMPT_TEMPLATE = """Given the topic "{topic}", assign it to the single most fitting umbrella term.

EXISTING ESTABLISHED UMBRELLA TERMS: {terms_list}

RULES:
1. FIRST — Check if the topic genuinely belongs under an existing term. Use an existing term if the topic is a natural member, instance, or expression of that umbrella. Most topics SHOULD fit under an existing term. Default to existing terms.
2. ONLY IF NO EXISTING TERM FITS — Establish a new umbrella term. It must be:
   - A genuine hypernym, holonym, or container word (1-2 words maximum)
   - A real word with real meaning and essence, not an academic department name
   - Specific enough to be meaningful (NEVER use: "Knowledge", "Study", "Learning", "Wisdom", "Truth", "Philosophy", "Science", "Culture", "History", "Ideas")
   - Broad enough that other related topics could naturally sit under it too
   - The kind of word where if you said it, a person would immediately picture the domain it contains
3. ANTI-DRIFT RULES:
   - Do NOT create a new term when an existing one works. Check each existing term carefully.
   - Do NOT collapse unrelated topics into the same term just because it is broad.
   - Do NOT create a term that only one topic could ever belong to (too atomic).
   - Do NOT use multi-word academic phrases like "Comparative Religion" or "Political Theory".
   - Each term should feel like a real container with real contents, not a label slapped on.
   - If uncertain between two existing terms, choose the one where the topic is MORE CENTRALLY a member.
4. GOOD examples: Divination, Consciousness, Transmutation, Hermeticism, Scripture, Embodiment, Rhetoric, Craft, Media
5. BAD examples: Ancient Wisdom, Spiritual Studies, Esoteric Knowledge, Human Experience, Deep Understanding

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
    "REGISTRY_DATABASES": "Relevant Notion Database(s)"
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
        """Upload file to Drive"""
        if not self.service:
            logger.warning("Google Drive service not initialized")
            return None

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
            logger.error(f"Error uploading {filename} to Drive: {e}")
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
        self.monthly_cost = 0.0
        self.total_processed = 0
        self.last_run = None
        self.operation_status = "Idle"
        self.health_score = 100

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

            # Get existing umbrella terms (use seed terms + cached terms to avoid querying entire DB)
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

            self.operation_status = "Idle"
            self._update_registry()
            logger.info(f"Processing cycle complete. {succeeded}/{attempted} entries succeeded")

        except Exception as e:
            logger.error(f"Error in processing cycle: {e}")
            self.operation_status = "Error"
            self.health_score = max(0, self.health_score - 20)

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

    def compile_daily_documents(self):
        """Compile daily documents - run at 11:55 PM.
        Retrieves the FULL CONTENT of every document processed today from Notion
        and compiles it into one continuous markdown file for ElevenReader consumption."""
        logger.info("Starting daily compilation...")

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

            entries = self.notion.query_database(NOTION_DATABASE_ID, filter_dict, page_size=100)

            if not entries:
                logger.info("No entries processed today for daily compilation")
                return

            logger.info(f"Found {len(entries)} entries for daily compilation")

            # Format the date: "February 17, 2026"
            today_display = datetime.now().strftime("%B %d, %Y").replace(" 0", " ")  # Remove leading zero from day
            daily_doc_title = f"📅 Established Daily Document — {today_display}"

            # Compile all documents — full content from each Notion page
            all_content = f"# {daily_doc_title}\n\n**Documents Compiled:** {len(entries)}\n\n---\n\n"

            compiled_count = 0
            for entry in entries:
                topic = self.notion.get_property_value(entry, COLUMNS["TITLE"])
                link = self.notion.get_property_value(entry, COLUMNS["LINK"])
                entry_id = entry["id"]

                if not topic:
                    continue

                logger.info(f"Compiling content for: {topic}")

                # Add topic header
                all_content += f"# 🧠 Established Truth, Principles of {topic}\n\n"
                all_content += f"**Original Topic:** {topic}\n\n"

                # Retrieve full content blocks from the Notion page
                try:
                    blocks = self.notion.get_block_children(entry_id)
                    if blocks:
                        content_md = self._blocks_to_markdown(blocks)
                        all_content += content_md
                    else:
                        all_content += "(Content could not be retrieved from Notion)\n\n"
                        if link:
                            all_content += f"[View on Google Drive]({link})\n\n"
                except Exception as e:
                    logger.error(f"Error retrieving content for {topic}: {e}")
                    all_content += "(Content could not be retrieved from Notion)\n\n"
                    if link:
                        all_content += f"[View on Google Drive]({link})\n\n"

                all_content += "\n---\n\n"
                compiled_count += 1

            logger.info(f"Compiled content from {compiled_count} documents")

            # Sanitize for ElevenReader compatibility
            all_content = DocumentBuilder.sanitize_for_compatibility(all_content)

            # Create markdown file with UTF-8 BOM
            content_with_bom = '\ufeff' + all_content
            md_bytes = content_with_bom.encode('utf-8')

            # Upload to Drive — use direct folder ID if set, otherwise search by name
            folder_id = DAILY_DRIVE_FOLDER_ID if DAILY_DRIVE_FOLDER_ID else self.drive.find_folder(DAILY_DRIVE_FOLDER_NAME)
            if not folder_id:
                logger.error(f"Could not find Drive folder {DAILY_DRIVE_FOLDER_NAME}")
                return

            filename = f"{daily_doc_title}.md"
            drive_link = self.drive.upload_file(md_bytes, filename, folder_id)

            if not drive_link:
                logger.error("Failed to upload daily compilation")
                return

            # Create Notion entry in Daily Documents DB
            properties = {
                COLUMNS["DAILY_TITLE"]: {"title": [{"type": "text", "text": {"content": daily_doc_title}}]},
                COLUMNS["DAILY_DATE"]: {"date": {"start": today}},
                COLUMNS["DAILY_COUNT"]: {"number": compiled_count},
                COLUMNS["DAILY_LINK"]: {"url": drive_link},
                COLUMNS["DAILY_STATUS"]: {"select": {"name": "Complete"}}
            }

            self.notion.create_page(DAILY_DOCUMENTS_DB_ID, properties)
            logger.info(f"Daily compilation complete: {compiled_count} documents, full content compiled")

        except Exception as e:
            logger.error(f"Error in daily compilation: {e}")

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

    def _update_registry(self):
        """Update the operations registry"""
        try:
            registry_page = self.notion.get_page(REGISTRY_PAGE_ID)
            if not registry_page:
                logger.warning("Could not find registry page")
                return

            properties = {
                COLUMNS["REGISTRY_STATUS"]: {
                    "select": {"name": "Active"}
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
                    "rich_text": [{"type": "text", "text": {"content": f"Claude API: Healthy | Google Drive: Healthy | Notion API: Healthy | Health Score: {self.health_score}"}}]
                },
                COLUMNS["REGISTRY_FREQUENCY"]: {
                    "rich_text": [{"type": "text", "text": {"content": "Every 5 minutes"}}]
                }
            }

            success = self.notion.update_page_properties(REGISTRY_PAGE_ID, properties)
            if success:
                logger.info("Registry updated successfully")
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
