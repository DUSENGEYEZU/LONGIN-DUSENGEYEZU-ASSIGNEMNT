-- SQL Queries for Amazon Fashion Reviews Data Exploration
-- Generated from DataIngestion.ipynb and AnalysisAndVisualization.ipynb

-- =====================================================
-- 1. BASIC DATA QUALITY ASSESSMENT
-- =====================================================

-- Get basic counts and statistics
SELECT 
    COUNT(*) as total_reviews,
    COUNT(DISTINCT asin) as unique_products,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT toYear(toDate(timestamp / 1000))) as years_covered,
    MIN(toDate(timestamp / 1000)) as earliest_review,
    MAX(toDate(timestamp / 1000)) as latest_review
FROM amazon.reviews;

-- Check for missing values in key columns
SELECT 
    COUNT(*) - COUNT(rating) as missing_ratings,
    COUNT(*) - COUNT(title) as missing_titles,
    COUNT(*) - COUNT(text) as missing_text,
    COUNT(*) - COUNT(asin) as missing_asin,
    COUNT(*) - COUNT(user_id) as missing_user_id,
    COUNT(*) - COUNT(timestamp) as missing_timestamp
FROM amazon.reviews;

-- Data completeness percentage
SELECT 
    ROUND((COUNT(rating) * 100.0 / (SELECT COUNT(*) FROM amazon.reviews)), 2) as rating_completeness,
    ROUND((COUNT(title) * 100.0 / (SELECT COUNT(*) FROM amazon.reviews)), 2) as title_completeness,
    ROUND((COUNT(text) * 100.0 / (SELECT COUNT(*) FROM amazon.reviews)), 2) as text_completeness,
    ROUND((COUNT(asin) * 100.0 / (SELECT COUNT(*) FROM amazon.reviews)), 2) as asin_completeness,
    ROUND((COUNT(user_id) * 100.0 / (SELECT COUNT(*) FROM amazon.reviews)), 2) as user_id_completeness
FROM amazon.reviews;

-- =====================================================
-- 2. RATING ANALYSIS
-- =====================================================

-- Rating distribution
SELECT 
    rating,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM amazon.reviews 
WHERE rating IS NOT NULL
GROUP BY rating 
ORDER BY rating;

-- Rating statistics
SELECT 
    AVG(rating) as avg_rating,
    MIN(rating) as min_rating,
    MAX(rating) as max_rating,
    quantile(0.5)(rating) as median_rating,
    quantile(0.25)(rating) as q1_rating,
    quantile(0.75)(rating) as q3_rating,
    stddevPop(rating) as rating_stddev
FROM amazon.reviews 
WHERE rating IS NOT NULL;

-- =====================================================
-- 3. TEMPORAL ANALYSIS
-- =====================================================

-- Reviews by year
SELECT 
    toYear(toDate(timestamp / 1000)) as review_year,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    COUNT(DISTINCT asin) as unique_products,
    COUNT(DISTINCT user_id) as unique_users
FROM amazon.reviews 
WHERE timestamp IS NOT NULL
GROUP BY toYear(toDate(timestamp / 1000)) 
ORDER BY review_year;

-- Reviews by month (across all years)
SELECT 
    toMonth(toDate(timestamp / 1000)) as review_month,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating
FROM amazon.reviews 
WHERE timestamp IS NOT NULL
GROUP BY toMonth(toDate(timestamp / 1000)) 
ORDER BY review_month;

-- Top months for reviews
SELECT 
    toMonth(toDate(timestamp / 1000)) as review_month,
    COUNT(*) as review_count
FROM amazon.reviews 
WHERE timestamp IS NOT NULL
GROUP BY toMonth(toDate(timestamp / 1000)) 
ORDER BY review_count DESC
LIMIT 5;

-- =====================================================
-- 4. PRODUCT ANALYSIS
-- =====================================================

-- Most reviewed products
SELECT 
    asin,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    COUNT(DISTINCT user_id) as unique_reviewers
FROM amazon.reviews 
WHERE asin IS NOT NULL
GROUP BY asin 
ORDER BY review_count DESC
LIMIT 10;

-- Product rating distribution
SELECT 
    asin,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    stddevPop(rating) as rating_stddev
FROM amazon.reviews 
WHERE asin IS NOT NULL AND rating IS NOT NULL
GROUP BY asin 
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC
LIMIT 10;

-- =====================================================
-- 5. USER BEHAVIOR ANALYSIS
-- =====================================================

-- Most active users
SELECT 
    user_id,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating_given,
    COUNT(DISTINCT asin) as unique_products_reviewed
FROM amazon.reviews 
WHERE user_id IS NOT NULL
GROUP BY user_id 
ORDER BY review_count DESC
LIMIT 10;

-- User rating patterns
SELECT 
    CASE 
        WHEN rating >= 4 THEN 'Positive (4-5 stars)'
        WHEN rating = 3 THEN 'Neutral (3 stars)'
        WHEN rating <= 2 THEN 'Negative (1-2 stars)'
    END as rating_category,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM amazon.reviews 
WHERE rating IS NOT NULL
GROUP BY rating_category
ORDER BY count DESC;

-- =====================================================
-- 6. TEXT ANALYSIS
-- =====================================================

-- Review length analysis
SELECT 
    CASE 
        WHEN length(text) < 50 THEN 'Short (<50 chars)'
        WHEN length(text) < 200 THEN 'Medium (50-200 chars)'
        WHEN length(text) < 500 THEN 'Long (200-500 chars)'
        ELSE 'Very Long (500+ chars)'
    END as review_length_category,
    COUNT(*) as count,
    AVG(rating) as avg_rating
FROM amazon.reviews 
WHERE text IS NOT NULL AND text != ''
GROUP BY review_length_category
ORDER BY count DESC;

-- Title analysis
SELECT 
    CASE 
        WHEN length(title) = 0 THEN 'No Title'
        WHEN length(title) < 20 THEN 'Short Title'
        WHEN length(title) < 50 THEN 'Medium Title'
        ELSE 'Long Title'
    END as title_length_category,
    COUNT(*) as count,
    AVG(rating) as avg_rating
FROM amazon.reviews 
GROUP BY title_length_category
ORDER BY count DESC;

-- =====================================================
-- 7. ADVANCED ANALYTICS
-- =====================================================

-- Rating trends over time
SELECT 
    toStartOfMonth(toDate(timestamp / 1000)) as month,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating,
    COUNT(DISTINCT asin) as unique_products,
    COUNT(DISTINCT user_id) as unique_users
FROM amazon.reviews 
WHERE timestamp IS NOT NULL
GROUP BY month
ORDER BY month;

-- Helpful vote analysis
SELECT 
    helpful_vote,
    COUNT(*) as count,
    AVG(rating) as avg_rating
FROM amazon.reviews 
WHERE helpful_vote IS NOT NULL
GROUP BY helpful_vote
ORDER BY helpful_vote DESC
LIMIT 20;

-- Verified purchase impact
SELECT 
    verified_purchase,
    COUNT(*) as count,
    AVG(rating) as avg_rating,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM amazon.reviews 
WHERE verified_purchase IS NOT NULL
GROUP BY verified_purchase;

-- =====================================================
-- 8. BUSINESS INSIGHTS QUERIES
-- =====================================================

-- Monthly review volume trends
SELECT 
    toYear(toDate(timestamp / 1000)) as year,
    toMonth(toDate(timestamp / 1000)) as month,
    COUNT(*) as review_count,
    AVG(rating) as avg_rating
FROM amazon.reviews 
WHERE timestamp IS NOT NULL
GROUP BY year, month
ORDER BY year, month;

-- Product performance analysis
SELECT 
    asin,
    COUNT(*) as total_reviews,
    AVG(rating) as avg_rating,
    COUNT(CASE WHEN rating >= 4 THEN 1 END) as positive_reviews,
    COUNT(CASE WHEN rating <= 2 THEN 1 END) as negative_reviews,
    ROUND(COUNT(CASE WHEN rating >= 4 THEN 1 END) * 100.0 / COUNT(*), 2) as positive_percentage
FROM amazon.reviews 
WHERE asin IS NOT NULL
GROUP BY asin 
HAVING COUNT(*) >= 5
ORDER BY positive_percentage DESC, total_reviews DESC
LIMIT 20;

-- User engagement analysis
SELECT 
    CASE 
        WHEN COUNT(*) = 1 THEN 'Single Review'
        WHEN COUNT(*) BETWEEN 2 AND 5 THEN 'Occasional Reviewer (2-5)'
        WHEN COUNT(*) BETWEEN 6 AND 20 THEN 'Active Reviewer (6-20)'
        ELSE 'Power Reviewer (20+)'
    END as user_type,
    COUNT(DISTINCT user_id) as user_count,
    AVG(COUNT(*)) as avg_reviews_per_user
FROM amazon.reviews 
WHERE user_id IS NOT NULL
GROUP BY user_id
GROUP BY user_type
ORDER BY user_count DESC;
