import requests
from datetime import datetime, timedelta
import os
from prefect import flow, task

# Configuration
API_TOKEN = os.getenv("API_TOKEN")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
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
def get_account_id():
    url = "https://api.cloudflare.com/client/v4/accounts"
    headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200: 
        return None
    data = response.json()
    if not data["success"] or not data["result"]: 
        return None
    return data["result"][0]["id"]

@task
def get_aggregated_metrics(date, account_id):
    url = "https://api.cloudflare.com/client/v4/graphql"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {API_TOKEN}"}
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
def send_to_slack(target_date, hit_ratio, cache_coverage, total_requests, origin_fetches, bandwidth, status_4xx, status_5xx):
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
    response = requests.post("https://slack.com/api/chat.postMessage", headers={"Content-Type":"application/json","Authorization":f"Bearer {SLACK_BOT_TOKEN}"}, json=payload)
    return response.status_code == 200

# ----------------- Flow -----------------
@flow(name="Cloudflare Stats Flow")
def cloudflare_stats_flow():
    yesterday = get_yesterday_date()
    formatted_yesterday = yesterday.strftime("%Y-%m-%d")
    account_id = get_account_id()
    if not account_id: return
    
    metrics = get_aggregated_metrics(formatted_yesterday, account_id)
    stats = process_account_data(metrics)
    if not stats: return

    origin_fetches = stats["total_requests"] - stats["total_cached_requests"]
    hit_ratio = (stats["total_cached_requests"] / stats["total_requests"] * 100) if stats["total_requests"]>0 else 0
    cache_coverage = (stats["total_cached_bytes"] / stats["total_bytes"] * 100) if stats["total_bytes"]>0 else 0
    bandwidth = f"{stats['total_bytes']/(1024**4):.2f} TiB"

    send_to_slack(formatted_yesterday, hit_ratio, cache_coverage, stats["total_requests"], origin_fetches, bandwidth, stats["total_4xx"], stats["total_5xx"])

# Run flow if script executed directly
if __name__ == "__main__":
    cloudflare_stats_flow()
