"""
Local Business Review Summariser  —  FREE LLM Edition
======================================================
Supports THREE free backends (no billing):

  1. GROQ   — Free API, no credit card. Fast Llama 3.3 70B.
               Sign up: https://console.groq.com  (free key in 2 mins)

  2. OLLAMA — 100% local, offline, unlimited. Runs on your machine.
               Install: https://ollama.com  then run: ollama pull llama3.2

  3. GEMINI — Google AI Studio free tier (1500 req/day, no card).
               Sign up: https://aistudio.google.com

Usage:
  python review_summariser.py --backend groq   --file reviews.xlsx
  python review_summariser.py --backend ollama --file reviews.xlsx
  python review_summariser.py --backend gemini --file reviews.xlsx

Environment variables needed:
  GROQ_API_KEY   (for groq backend)
  GEMINI_API_KEY (for gemini backend)
  Ollama needs no key — just run locally.
"""

import os, sys, re, json, argparse
import pandas as pd
import urllib.request
import urllib.error

# ── CONFIG ─────────────────────────────────────────────────────────────────
DEFAULT_FILE   = "Fried_chicken_review.xlsx"
MAX_REVIEWS    = 40

# Groq settings
GROQ_MODEL     = "llama-3.3-70b-versatile"
GROQ_API_URL   = "https://api.groq.com/openai/v1/chat/completions"

# Ollama settings (local)
OLLAMA_MODEL   = "llama3.2"          # change to any model you have pulled
OLLAMA_API_URL = "http://localhost:11434/api/chat"

# Gemini settings
GEMINI_MODEL   = "gemini-2.0-flash"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
# ──────────────────────────────────────────────────────────────────────────


# ─── COLUMN AUTO-DETECTION ────────────────────────────────────────────────

COLUMN_ALIASES = {
    "name":     ["name", "business", "restaurant", "shop", "store", "brand", "place"],
    "review":   ["review", "text", "comment", "feedback", "description", "content", "body"],
    "stars":    ["stars", "rating", "score", "grade", "rate"],
    "location": ["location", "address", "city", "area", "region"],
}

def detect_column(df, field):
    for col in df.columns:
        if col.strip().lower() in COLUMN_ALIASES[field]:
            return col
    return None


# ─── DATASET LOADER ───────────────────────────────────────────────────────

def load_dataset(filepath):
    ext = os.path.splitext(filepath)[1].lower()
    if ext in (".xlsx", ".xls", ".xlsm"):
        df = pd.read_excel(filepath)
    elif ext == ".csv":
        df = pd.read_csv(filepath)
    else:
        raise ValueError(f"Unsupported file: {ext}. Use .xlsx or .csv")

    df.columns = [c.strip() for c in df.columns]
    cols = {f: detect_column(df, f) for f in COLUMN_ALIASES}

    if not cols["name"]:
        raise ValueError(f"No business-name column found. Expected: {COLUMN_ALIASES['name']}")
    if not cols["review"]:
        raise ValueError(f"No review column found. Expected: {COLUMN_ALIASES['review']}")

    print(f"\n  Dataset loaded  ->  {len(df):,} reviews | {df[cols['name']].nunique()} businesses")
    print(f"  Columns : name='{cols['name']}' | review='{cols['review']}' | "
          f"stars='{cols['stars'] or 'N/A'}' | location='{cols['location'] or 'N/A'}'\n")
    return df, cols


# ─── LLM BACKENDS ─────────────────────────────────────────────────────────

def _post_json(url, payload, headers=None):
    """Simple HTTP POST using stdlib — no extra dependencies."""
    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(url, data=data,
                                   headers={"Content-Type": "application/json",
                                            **(headers or {})},
                                   method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def call_groq(system_prompt, user_msg):
    key = os.environ.get("GROQ_API_KEY", "")
    if not key:
        raise EnvironmentError(
            "GROQ_API_KEY not set.\n"
            "  1. Sign up free at https://console.groq.com\n"
            "  2. export GROQ_API_KEY='gsk_...'"
        )
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_msg},
        ],
        "max_tokens": 1024,
        "temperature": 0.3,
    }
    result = _post_json(GROQ_API_URL, payload,
                        headers={"Authorization": f"Bearer {key}"})
    return result["choices"][0]["message"]["content"].strip()


def call_ollama(system_prompt, user_msg):
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_msg},
        ],
        "stream": False,
    }
    try:
        result = _post_json(OLLAMA_API_URL, payload)
    except urllib.error.URLError:
        raise EnvironmentError(
            "Cannot reach Ollama at localhost:11434.\n"
            "  1. Install Ollama from https://ollama.com\n"
            f"  2. Run: ollama pull {OLLAMA_MODEL}\n"
            "  3. Make sure Ollama is running (it starts automatically after install)"
        )
    return result["message"]["content"].strip()


def call_gemini(system_prompt, user_msg):
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        raise EnvironmentError(
            "GEMINI_API_KEY not set.\n"
            "  1. Get free key at https://aistudio.google.com\n"
            "  2. export GEMINI_API_KEY='AIza...'"
        )
    url = GEMINI_API_URL.format(model=GEMINI_MODEL) + f"?key={key}"
    payload = {
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"parts": [{"text": user_msg}]}],
        "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.3},
    }
    result = _post_json(url, payload)
    return result["candidates"][0]["content"]["parts"][0]["text"].strip()


BACKENDS = {
    "groq":   call_groq,
    "ollama": call_ollama,
    "gemini": call_gemini,
}


# ─── PROMPTS ──────────────────────────────────────────────────────────────

PARSE_SYSTEM = """Extract the business name and optional city from the query.
Return ONLY valid JSON, no markdown, no explanation.
Format: {"business": "<n>", "city": "<city or empty string>"}

Examples:
"How is K Soul Chicken in San Francisco?" -> {"business": "K Soul Chicken", "city": "San Francisco"}
"chicken king dallas"                     -> {"business": "Chicken King", "city": "Dallas"}
"rowdy rooster in new york"               -> {"business": "Rowdy Rooster", "city": "New York"}
"tell me about minnie bell"               -> {"business": "Minnie Bell", "city": ""}
"a2b chennai"                             -> {"business": "A2B", "city": "Chennai"}
"""

SUMMARY_SYSTEM = """You are a local business review analyst.
Analyse the customer reviews and return ONLY the structured output below.
No markdown, no asterisks, no extra text.

Pros:
- <specific point from reviews>
- <specific point from reviews>
- <specific point from reviews>
- <specific point from reviews>

Cons:
- <specific point from reviews>
- <specific point from reviews>
- <specific point from reviews>

Summary:
<2-3 sentences describing the overall customer experience>

Sentiment:
<exactly one word: Positive, Mixed, or Negative>

Rules:
- Every point must be grounded in the actual reviews.
- Be specific — name dishes, service issues, real experiences.
- Sentiment must be exactly: Positive, Mixed, or Negative.
"""


# ─── QUERY PARSER ─────────────────────────────────────────────────────────

def parse_query(user_query, backend_fn):
    raw = backend_fn(PARSE_SYSTEM, user_query)
    # strip possible markdown fences
    raw = re.sub(r"```json|```", "", raw).strip()
    try:
        return json.loads(raw)
    except Exception:
        return {"business": user_query, "city": ""}


# ─── BUSINESS MATCHER ─────────────────────────────────────────────────────

def find_business(parsed, df, cols):
    name_col     = cols["name"]
    location_col = cols["location"]
    target_biz   = parsed["business"].lower().strip()
    target_city  = parsed["city"].lower().strip()
    businesses   = df[name_col].dropna().unique().tolist()

    def score(biz):
        b = biz.lower()
        if b == target_biz:
            ns = 4
        elif target_biz in b or b in target_biz:
            ns = 3
        else:
            bw = set(re.sub(r"[^a-z0-9 ]", "", b).split())
            tw = set(re.sub(r"[^a-z0-9 ]", "", target_biz).split())
            ns = len(bw & tw)
        if ns == 0:
            return 0
        if target_city and location_col:
            locs = df[df[name_col] == biz][location_col].dropna().str.lower().tolist()
            if any(target_city in loc for loc in locs):
                ns += 3
        return ns

    scored = [(b, score(b)) for b in businesses]
    scored = [(b, s) for b, s in scored if s > 0]
    if not scored:
        return None
    return max(scored, key=lambda x: x[1])[0]


# ─── CONTEXT BUILDER ──────────────────────────────────────────────────────

def build_context(df, cols, business):
    nc, rc, sc, lc = cols["name"], cols["review"], cols["stars"], cols["location"]
    subset = df[df[nc].str.lower() == business.lower()].copy()

    avg_stars = None
    if sc:
        subset[sc] = pd.to_numeric(subset[sc], errors="coerce")
        avg_stars  = round(float(subset[sc].mean()), 2)

    reviews = subset[rc].dropna().tolist()
    if len(reviews) > MAX_REVIEWS:
        step    = max(1, len(reviews) // MAX_REVIEWS)
        reviews = reviews[::step][:MAX_REVIEWS]

    return {
        "business":      business,
        "total_reviews": len(subset),
        "location":      str(subset[lc].iloc[0]) if lc else "N/A",
        "avg_stars":     avg_stars,
        "reviews":       reviews,
    }


# ─── SUMMARY GENERATOR ────────────────────────────────────────────────────

def generate_summary(ctx, backend_fn):
    reviews_text = "\n".join(f"- {r}" for r in ctx["reviews"])
    stars_line   = f"Average rating: {ctx['avg_stars']}/5\n" if ctx["avg_stars"] else ""
    user_msg = (
        f"Business: {ctx['business']}\n"
        f"Location: {ctx['location']}\n"
        f"Total reviews: {ctx['total_reviews']}\n"
        f"{stars_line}"
        f"\nCustomer Reviews:\n{reviews_text}"
    )
    return backend_fn(SUMMARY_SYSTEM, user_msg)


# ─── OUTPUT FORMATTER ─────────────────────────────────────────────────────

def sentiment_icon(text):
    t = text.lower()
    if "positive" in t: return "Positive  😊 📊"
    if "negative" in t: return "Negative  😞 📊"
    return "Mixed  😐 📊"

def print_result(business, location, summary_text):
    lines  = summary_text.splitlines()
    output = []
    for line in lines:
        if line.strip().lower().startswith("sentiment:"):
            output.append(f"Sentiment:\n{sentiment_icon(line)}")
        else:
            output.append(line)

    print()
    print(f"  Business : {business}")
    print(f"  Location : {location}")
    print()
    print("  ================ RESULT ================")
    print()
    for line in output:
        print(f"  {line}")
    print()
    print("  ========================================")
    print()


# ─── QUERY HANDLER ────────────────────────────────────────────────────────

def handle_query(user_query, df, cols, backend_fn):
    name_col = cols["name"]

    if user_query.strip().lower() in ("list", "businesses", "help", "?"):
        businesses = sorted(df[name_col].dropna().unique().tolist())
        print(f"\n  {len(businesses)} businesses in this dataset:")
        for i, b in enumerate(businesses, 1):
            loc = ""
            if cols["location"]:
                loc = f"  |  {df[df[name_col]==b][cols['location']].iloc[0]}"
            print(f"     {i:2}. {b}{loc}")
        print()
        return

    print(f"\n  Parsing: \"{user_query}\" ...")
    parsed = parse_query(user_query, backend_fn)
    print(f"  -> Business : '{parsed['business']}'")
    print(f"  -> City     : '{parsed['city'] or 'not specified'}'")

    business = find_business(parsed, df, cols)

    if not business:
        print()
        print("  ❌ No reviews found for this business in this city.")
        print("     Type 'list' to see all available businesses.\n")
        return

    ctx = build_context(df, cols, business)
    print(f"  -> Matched  : '{business}'  ({ctx['total_reviews']} reviews)")
    print("  Generating summary ...\n")

    summary = generate_summary(ctx, backend_fn)
    print_result(business, ctx["location"], summary)


# ─── MAIN ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Local Business Review Summariser (Free LLM)")
    parser.add_argument("--backend", default="groq",
                        choices=["groq", "ollama", "gemini"],
                        help="LLM backend to use (default: groq)")
    parser.add_argument("--file", default=DEFAULT_FILE,
                        help="Path to review dataset (.xlsx or .csv)")
    args = parser.parse_args()

    backend_fn = BACKENDS[args.backend]

    print()
    print("  +--------------------------------------------------+")
    print("  |   LOCAL BUSINESS REVIEW SUMMARISER              |")
    print(f"  |   Backend : {args.backend.upper():<38}|")
    print("  +--------------------------------------------------+")

    filepath = args.file
    if not os.path.exists(filepath):
        alt = os.path.join("/mnt/user-data/uploads", os.path.basename(filepath))
        if os.path.exists(alt):
            filepath = alt
        else:
            print(f"\n  ERROR: File not found: {filepath}")
            sys.exit(1)

    df, cols = load_dataset(filepath)

    print("  Natural language queries supported:")
    print('     "How is K Soul Chicken in San Francisco?"')
    print('     "chicken king dallas"')
    print('     "rowdy rooster new york"')
    print('     "list"  -> show all businesses | "quit" -> exit')
    print()

    while True:
        try:
            user_query = input("  Query > ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\n  Goodbye!\n")
            break
        if not user_query:
            continue
        if user_query.lower() in ("quit", "exit", "q"):
            print("\n  Goodbye!\n")
            break
        try:
            handle_query(user_query, df, cols, backend_fn)
        except EnvironmentError as e:
            print(f"\n  ⚙️  Setup needed:\n  {e}\n")
        except Exception as e:
            print(f"\n  Error: {e}\n")


if __name__ == "__main__":
    main()
