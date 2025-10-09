import requests
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.blocks.system import Secret

# Configuration - Using Prefect Secret Blocks
SLACK_CHANNEL_ID = "C079Z48QE49"

RED_COLOR = "#ff0000"
YELLOW_COLOR = "#F7DC6F"
GREEN_COLOR = "#229954"

# ----------------- Tasks -----------------
@task
def get_yesterday_date():
    today = datetime.utcnow().date()
    return today - timedelta(days=1)

@task
async def get_account_id(api_token):
    url = "https://api.cloudflare.com/client/v4/accounts"
    headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200: 
        return None
    data = response.json()
    if not data["success"] or not data["result"]: 
        return None
    return data["result"][0]["id"]

@task
async def get_aggregated_metrics(date, account_id, api_token):
    url = "https://api.cloudflare.com/client/v4/graphql"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_token}"}
    query = """
    {
      viewer {
        accounts(filter: {accountTag: "%s"}) {
          httpRequests1dGroups(limit: 1, filter: {date: "%s"}) {
            dimensions { date }
            sum {
              requests
              bytes
              cachedBytes
              cachedRequests
              responseStatusMap { edgeResponseStatus requests }
            }
          }
        }
      }
    }""" % (account_id, date)
    response = requests.post(url, headers=headers, json={"query": query})
    return response.json() if response.status_code == 200 else None

@task
def process_account_data(metrics_data):
    if not metrics_data or "data" not in metrics_data: return None
    viewer = metrics_data["data"].get("viewer", {})
    accounts = viewer.get("accounts", [])
    totals = {"total_requests":0, "total_cached_requests":0, "total_bytes":0, "total_cached_bytes":0, "total_4xx":0, "total_5xx":0}

    for account in accounts:
        http_group = account.get("httpRequests1dGroups", [{}])[0]
        stats = http_group.get("sum", {})
        totals["total_requests"] += stats.get("requests",0)
        totals["total_cached_requests"] += stats.get("cachedRequests",0)
        totals["total_bytes"] += stats.get("bytes",0)
        totals["total_cached_bytes"] += stats.get("cachedBytes",0)

        for status in stats.get("responseStatusMap", []):
            try:
                code = int(status["edgeResponseStatus"])
                count = status["requests"]
                if 400 <= code < 500: totals["total_4xx"] += count
                elif 500 <= code < 600: totals["total_5xx"] += count
            except ValueError: continue
    return totals

@task
def get_hit_ratio_color(hit_ratio):
    if hit_ratio < 90: return RED_COLOR
    elif hit_ratio < 95: return YELLOW_COLOR
    else: return GREEN_COLOR

@task
async def send_to_slack(target_date, hit_ratio, cache_coverage, total_requests, origin_fetches, bandwidth, status_4xx, status_5xx, slack_bot_token):
    color = get_hit_ratio_color(hit_ratio)
    text = (
        f"*Avg Hit Ratio: {hit_ratio:.2f}%*\n"
        f"*Avg Cache Coverage: {cache_coverage:.2f}%*\n"
        f"*CDN Requests: {total_requests}*\n"
        f"*CDN Origin Fetches: {origin_fetches}*\n"
        f"*CDN Bandwidth: {bandwidth}*\n"
        f"*CDN Status 4xx Requests: {status_4xx}*\n"
        f"*CDN Status 5xx Requests: {status_5xx}*"
    )
    payload = {"channel": SLACK_CHANNEL_ID, "text": f"Cloudflare Stats *{target_date}*", "attachments":[{"fallback":"Cloudflare Performance Stats","color":color,"text":text}]}
    response = requests.post("https://slack.com/api/chat.postMessage", headers={"Content-Type":"application/json","Authorization":f"Bearer {slack_bot_token}"}, json=payload)
    return response.status_code == 200

# ----------------- Flow -----------------
@flow(name="Cloudflare Stats Flow")
async def cloudflare_stats_flow():
    # Load tokens from Prefect Secret Blocks
    cloudflare_token_block = await Secret.load("cloudflare-api-token")
    slack_token_block = await Secret.load("slack-bot-token")
    
    api_token = cloudflare_token_block.get()
    slack_bot_token = slack_token_block.get()
    
    print(f"🔐 Loaded CloudFlare API token: {api_token[:10]}...")
    print(f"🔐 Loaded Slack Bot token: {slack_bot_token[:10]}...")
    
    yesterday = get_yesterday_date()
    formatted_yesterday = yesterday.strftime("%Y-%m-%d")
    account_id = await get_account_id(api_token)
    if not account_id: return
    
    metrics = await get_aggregated_metrics(formatted_yesterday, account_id, api_token)
    stats = process_account_data(metrics)
    if not stats: return

    origin_fetches = stats["total_requests"] - stats["total_cached_requests"]
    hit_ratio = (stats["total_cached_requests"] / stats["total_requests"] * 100) if stats["total_requests"]>0 else 0
    cache_coverage = (stats["total_cached_bytes"] / stats["total_bytes"] * 100) if stats["total_bytes"]>0 else 0
    bandwidth = f"{stats['total_bytes']/(1024**4):.2f} TiB"

    await send_to_slack(formatted_yesterday, hit_ratio, cache_coverage, stats["total_requests"], origin_fetches, bandwidth, stats["total_4xx"], stats["total_5xx"], slack_bot_token)

# Run flow if script executed directly
if __name__ == "__main__":
    import asyncio
    asyncio.run(cloudflare_stats_flow())
