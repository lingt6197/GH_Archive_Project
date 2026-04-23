import requests
import pandas as pd
import time
from geopy.geocoders import Nominatim
import os

TOKEN = os.getenv("GITHUB_TOKEN")

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}

users = pd.read_csv("data/users.csv")

results = []

for username in users["login"]:
    url = f"https://api.github.com/users/{username}"
    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        data = r.json()

        results.append({
            "login": data["login"],
            "name": data.get("name"),
            "company": data.get("company"),
            "location": data.get("location"),
            "followers": data.get("followers"),
            "public_repos": data.get("public_repos"),
            "created_at": data.get("created_at")
        })

    # time.sleep(0.5)

df = pd.DataFrame(results)
# df.to_csv("github_profiles.csv", index=False)
print(df.head())

# geo = Nominatim(user_agent="gh-study")

# geo.geocode("Seattle, WA")