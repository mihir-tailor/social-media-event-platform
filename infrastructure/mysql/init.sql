-- Create database if not exists
CREATE DATABASE IF NOT EXISTS social_media_analytics;
USE social_media_analytics;

-- Table for 5-minute aggregated event metrics
CREATE TABLE IF NOT EXISTS event_metrics_5min (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    platform VARCHAR(20) NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    unique_users BIGINT NOT NULL DEFAULT 0,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_window_start (window_start),
    INDEX idx_event_type (event_type),
    INDEX idx_platform (platform),
    INDEX idx_composite (window_start, event_type, platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table for 1-hour aggregated event metrics
CREATE TABLE IF NOT EXISTS event_metrics_1hr (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    platform VARCHAR(20) NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    unique_users BIGINT NOT NULL DEFAULT 0,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_window_start (window_start),
    INDEX idx_event_type (event_type),
    INDEX idx_platform (platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table for 1-day aggregated event metrics
CREATE TABLE IF NOT EXISTS event_metrics_1day (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    platform VARCHAR(20) NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    unique_users BIGINT NOT NULL DEFAULT 0,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_window_start (window_start),
    INDEX idx_event_type (event_type),
    INDEX idx_platform (platform)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table for user activity metrics (1-hour windows)
CREATE TABLE IF NOT EXISTS user_metrics_1hr (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    platform VARCHAR(20) NOT NULL,
    total_events BIGINT NOT NULL DEFAULT 0,
    likes BIGINT NOT NULL DEFAULT 0,
    comments BIGINT NOT NULL DEFAULT 0,
    shares BIGINT NOT NULL DEFAULT 0,
    views BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_window_start (window_start),
    INDEX idx_user_id (user_id),
    INDEX idx_platform (platform),
    INDEX idx_total_events (total_events),
    INDEX idx_composite (window_start, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Optional: Create materialized view for top users (requires manual refresh or triggers)
-- This can be updated periodically for faster queries

-- Insert some sample data for testing
INSERT INTO event_metrics_5min (window_start, window_end, event_type, platform, event_count, unique_users)
VALUES 
    (NOW() - INTERVAL 5 MINUTE, NOW(), 'like', 'web', 150, 80),
    (NOW() - INTERVAL 5 MINUTE, NOW(), 'like', 'ios', 200, 120),
    (NOW() - INTERVAL 5 MINUTE, NOW(), 'comment', 'web', 50, 40),
    (NOW() - INTERVAL 5 MINUTE, NOW(), 'view', 'android', 500, 300);

INSERT INTO user_metrics_1hr (window_start, window_end, user_id, platform, total_events, likes, comments, shares, views, clicks)
VALUES 
    (NOW() - INTERVAL 1 HOUR, NOW(), 'user_1', 'web', 45, 20, 5, 3, 15, 2),
    (NOW() - INTERVAL 1 HOUR, NOW(), 'user_2', 'ios', 60, 30, 8, 4, 15, 3),
    (NOW() - INTERVAL 1 HOUR, NOW(), 'user_3', 'android', 35, 15, 3, 2, 12, 3);