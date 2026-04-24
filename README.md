Local Business Review Summariser	
Fetches reviews for a specified business or location, analyzes the content, and delivers a concise summary of key pros and cons.


Option 1 — Groq (Recommended ⭐)
Free, fast, no credit card, 14,400 requests/day
bash# 1. Sign up at https://console.groq.com  (2 minutes, free)
# 2. Copy your API key, then:

pip install pandas openpyxl

export GROQ_API_KEY="gsk_your_key_here"
python review_summariser.py --backend groq --file Fried_chicken_review.xlsx

Option 2 — Ollama (Fully Local, Zero Internet, Unlimited)
Runs on your own machine — no API key, no limits, private
bash# 1. Install from https://ollama.com
# 2. Pull a model:
ollama pull llama3.2

pip install pandas openpyxl
python review_summariser.py --backend ollama --file Fried_chicken_review.xlsx

Option 3 — Google Gemini (1500 requests/day free)
bash# 1. Get free key at https://aistudio.google.com
export GEMINI_API_KEY="AIza_your_key"
python review_summariser.py --backend gemini --file Fried_chicken_review.xlsx

All queries work the same regardless of backend:
Query > How is K Soul Chicken in San Francisco?
Query > chicken king dallas
Query > rowdy rooster new york
Query > a2b chennai          ← returns "No reviews found"
Query > list                 ← shows all businesses
My recommendation: start with Groq — fastest setup (2 min), completely free, no card needed, and the Llama 3.3 70B model gives excellent quality summaries.
