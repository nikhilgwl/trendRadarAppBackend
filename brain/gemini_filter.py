import os
import json
import logging
import httpx
import urllib3
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

# -- SSL bypass --
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
_orig_httpx_client_init = httpx.Client.__init__
def _patched_httpx_client_init(self, *args, **kwargs):
    kwargs["verify"] = False
    _orig_httpx_client_init(self, *args, **kwargs)
httpx.Client.__init__ = _patched_httpx_client_init

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_categorized_trends(config, google_trends, reddit_posts, rss_headlines, social_trends=None, pinterest_trends=None):
    """
    Step 1: Identify 5 trends with mobile-optimized data fields.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not found in environment.")
        return []

    try:
        client = genai.Client(api_key=api_key)
        model_id = "gemini-3.1-flash-lite-preview" 

        # Format inputs
        reddit_list = "\n".join([f"- {p['title']} [{p.get('score', 0)} upvotes, Sub: {p['subreddit']}]" for p in reddit_posts[:80]])
        news_list = "\n".join([f"- {r['title']}" for r in rss_headlines[:30]])
        other_list = f"Social/Search: {social_trends} {google_trends} {pinterest_trends}"

        system_instr = """You are an Expert Beauty Intelligence Analyst for HUL. Your goal is to transform raw social signals into actionable marketing insights for Skincare, Haircare, and Makeup.

Labels: [REDDIT EXCLUSIVE], [GOOGLE/SOCIAL], [NEWS/RSS], [CROSS-PLATFORM], [INGREDIENT BREAKOUT]
Categories: Skincare, Haircare, Makeup

STRICT RULES for HIGH SPECIFICITY:
1. Trend Name: Must be specific (e.g., "Cloud Skin Matte Finish" not "Matte Makeup").
2. Context: Explain the 'Why' and 'What'. Mention specific ingredients or techniques being discussed (e.g., "Users on Reddit are swapping heavy oils for lightweight Glycerin-based humectants for summer hydration").
3. The Result: Actionable for HUL Marketing. What product category or messaging should we prioritize? (e.g., "Pivot marketing to emphasize non-comedogenic gel-creams").
4. Category: Must be one of [Skincare, Haircare, Makeup].

OUTPUT FORMAT: Return a JSON array of 5 objects:
[
  {
    "label": "[REDDIT EXCLUSIVE]",
    "category": "Skincare",
    "trend_name": "Specific Trend Name",
    "source_platform": "Platform Name",
    "metric": "e.g., 107 upvotes",
    "context": "Brief but high-specificity context (max 25 words).",
    "result": "Marketing takeaway/Product focus (max 20 words)."
  }
]"""

        gemini_config = types.GenerateContentConfig(
            system_instruction=system_instr,
            temperature=0.1,
            response_mime_type="application/json",
        )

        prompt = f"DATA SOURCES:\n\nREDDIT:\n{reddit_list}\n\nGOOGLE/SOCIAL:\n{other_list}\n\nNEWS:\n{news_list}"

        logger.info(f"Extracting 5-Platform Beauty Intelligence for mobile report using {model_id}...")

        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=gemini_config,
        )

        if response.text:
            return json.loads(response.text.strip())
        return []

    except Exception as e:
        logger.error(f"Trend Analysis Error: {str(e)}")
        return []
