-- SELECT author FROM `bigquery-public-data.github_repos.commits` LIMIT 10000

-- Q1
SELECT 
  author.tz_offset, 
  COUNT(*) AS commit_count 
FROM `bigquery-public-data.github_repos.commits` 
WHERE NOT REGEXP_CONTAINS(LOWER(author.email), r'(bot|git|noreply|actions|travis|jenkins)')
GROUP BY author.tz_offset 
ORDER BY commit_count DESC

-- Q2
SELECT
  EXTRACT(YEAR FROM created_at) AS event_year,
  EXTRACT(MONTH FROM created_at) AS event_month,
  COUNT(*) AS event_count
FROM `githubarchive.month.*`
WHERE type = 'PushEvent'
  AND actor.login NOT LIKE '%bot%'
GROUP BY event_year, event_month
ORDER BY event_year ASC, event_month ASC


-- Q3
WITH 
  -- Step A: Extract timezone to repository mapping from submission records, filtering out noise from automated systems
  RegionRepo AS (
    SELECT DISTINCT 
      repo, 
      author.tz_offset
    FROM `bigquery-public-data.github_repos.commits`,
    UNNEST(repo_name) AS repo
    WHERE author.tz_offset != 0
      AND NOT REGEXP_CONTAINS(LOWER(author.email), r'(bot|git|noreply|actions|travis|jenkins)')
  ),
  

-- Step B: Flatten language records per repository
  RepoLang AS (
    SELECT 
      repo_name, 
      lang.name AS language_name
    FROM `bigquery-public-data.github_repos.languages`,
    UNNEST(language) AS lang
  )

-- Step C: Perform association and aggregation calculations for language distribution frequency in a region
SELECT 
  r.tz_offset,
  l.language_name,
  COUNT(DISTINCT r.repo) AS repository_count
FROM RegionRepo r
JOIN RepoLang l ON r.repo = l.repo_name
GROUP BY r.tz_offset, l.language_name
ORDER BY r.tz_offset ASC, repository_count DESC



-- Q4
SELECT
  author.tz_offset,
  EXTRACT(HOUR FROM TIMESTAMP_SECONDS(author.time_sec + (author.tz_offset * 60))) AS local_hour,
  COUNT(*) AS commit_count
FROM `bigquery-public-data.github_repos.commits`
WHERE author.tz_offset != 0
  -- Exclude bots
  AND NOT REGEXP_CONTAINS(LOWER(author.email), r'(bot|git|noreply|actions|travis|jenkins|system)')
  -- Exclude personal email
  AND NOT REGEXP_CONTAINS(LOWER(author.email), r'@(gmail|yahoo|hotmail|outlook|live|icloud|me|mac|protonmail|qq|163|126|foxmail|yandex)\.')
  -- Exclude .edu
  AND NOT REGEXP_CONTAINS(LOWER(author.email), r'(\.edu$|\.edu\.|ac\.uk$|\.ac\.)')
GROUP BY author.tz_offset, local_hour
ORDER BY author.tz_offset, local_hour ASC


-- Active rate calculation of github_repos dataset
WITH recent_active_repos AS (
  -- 1. Active repository from github_repos 202603
  -- 20 Star (WatchEvent) or more is considered active
  SELECT
    repo.name AS repo_name,
    COUNT(*) AS star_count
  FROM
    `githubarchive.month.202603`
  WHERE
    type = 'WatchEvent'
  GROUP BY
    1
  HAVING
    star_count >= 20
),
mirrored_repos AS (
-- 2. Extract the list of indexed repositories from the mirrored dataset
--    Using the licenses table as an index, which is far smaller than files or contents
  SELECT DISTINCT
    repo_name
  FROM
    `bigquery-public-data.github_repos.licenses`
)
-- 3. Calculate coverage rate
SELECT
  COUNT(r.repo_name) AS active_repos_sample,
  COUNT(m.repo_name) AS mirrored_repos_match,
  ROUND(SAFE_DIVIDE(COUNT(m.repo_name), COUNT(r.repo_name)) * 100, 2) AS coverage_percentage
FROM
  recent_active_repos r
LEFT JOIN
  mirrored_repos m
ON
  LOWER(r.repo_name) = LOWER(m.repo_name)


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