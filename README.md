# WorldPulse

WorldPulse is a free website that shows world news from 30 sources, with each story tagged by its likely impact on share markets. It runs entirely on GitHub: no server and no cost.

- **News**: Google News editions for the US, India, UK, Singapore and Australia, plus BBC, Al Jazeera, the Guardian, DW, France 24, NYT, CNBC, Economic Times, Livemint, Business Standard and Moneycontrol.
- **Market impact**: each story is tagged with the markets it touches, the sectors it may push up (▲) or down (▼), and an impact level.
- **Markets**: 16 world indices, plus oil, gold, currencies, the US 10-year yield and Bitcoin.
- **Reader**: a "how the market reacted" chart, related coverage, and an optional Claude AI analysis.
- **My tracker**: tracked stories with notes, followed topics and reading history. These are saved in each visitor's own browser.

## How it works
A GitHub Action runs `scripts/update_data.py` every 15 minutes. The script fetches news and prices and saves them to `data/news.json` and `data/prices.json`. It then republishes the site. Open pages check for new data every minute and show "N new stories" when an update lands.

## Files
| Path | What it is |
|---|---|
| `index.html` | The whole website |
| `scripts/update_data.py` | Fetches news and prices. Sources, markets and keyword rules are at the top. |
| `.github/workflows/update-data.yml` | Runs the updater every 15 minutes and publishes the site |
| `requirements.txt` | Python packages for the updater |
| `data/` | Created automatically on the first run |

## One-time setup
1. The repository must be **Public**.
2. Go to **Settings → Pages → Source** and choose **GitHub Actions**.
3. Go to the **Actions** tab. If asked, click **I understand my workflows, go ahead and enable them**. Then open **Update news and publish site** and click **Run workflow**.
4. After 2–3 minutes the site is live at `https://<your-username>.github.io/<repository-name>/`.

## Not investment advice
Impact tags come from keyword rules (or AI, if you add a key) and are indicative only. Prices come from Yahoo Finance and can be delayed.
