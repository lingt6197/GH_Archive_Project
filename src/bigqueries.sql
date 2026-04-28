-- For querying user profiles for geocoding:
SELECT
  actor.login,
  created_at,
  type,
  EXTRACT(MONTH FROM created_at) AS month,
  EXTRACT(DAY FROM created_at) AS day,
  EXTRACT(YEAR FROM created_at) AS year
FROM `githubarchive.year.2015` -- adjust the year as needed
WHERE actor.login IS NOT NULL
AND NOT REGEXP_CONTAINS(
    LOWER(actor.login),
    r'(\[bot\]|bot\b|\bbot\b)'
)
LIMIT 1000000

-- For querying timezone info from github.repos dataset:
SELECT author.tz_offset, 
COUNT(*) as sample_count 
FROM `bigquery-public-data.github_repos.commits` TABLESAMPLE SYSTEM (1 PERCENT) 
GROUP BY author.tz_offset 
ORDER BY sample_count DESC