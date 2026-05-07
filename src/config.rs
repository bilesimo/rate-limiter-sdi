use serde::{Deserialize, Serialize};
use std::{fs, path::Path, time::Duration};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse yaml config: {0}")]
    ParseYaml(#[from] serde_yaml::Error),
    #[error("config validation failed: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitBehavior {
    Block,
    Queue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RateLimitAlgorithm {
    FixedWindow {
        requests_per_window: u64,
        #[serde(with = "duration_seconds")]
        window: Duration,
    },
    TokenBucket {
        capacity: u64,
        refill_tokens: u64,
        #[serde(with = "duration_seconds")]
        refill_interval: Duration,
    },
}

impl RateLimitAlgorithm {
    pub fn limit(&self) -> u64 {
        match self {
            Self::FixedWindow {
                requests_per_window,
                ..
            } => *requests_per_window,
            Self::TokenBucket { capacity, .. } => *capacity,
        }
    }

    pub fn validate(&self, rule_name: &str) -> Result<(), ConfigError> {
        match self {
            Self::FixedWindow {
                requests_per_window,
                window,
            } => {
                if *requests_per_window == 0 {
                    return Err(ConfigError::Validation(format!(
                        "rule '{rule_name}' must allow at least one request"
                    )));
                }

                if window.is_zero() {
                    return Err(ConfigError::Validation(format!(
                        "rule '{rule_name}' must have a non-zero window"
                    )));
                }
            }
            Self::TokenBucket {
                capacity,
                refill_tokens,
                refill_interval,
            } => {
                if *capacity == 0 {
                    return Err(ConfigError::Validation(format!(
                        "rule '{rule_name}' must have a token bucket capacity greater than zero"
                    )));
                }

                if *refill_tokens == 0 {
                    return Err(ConfigError::Validation(format!(
                        "rule '{rule_name}' must refill at least one token"
                    )));
                }

                if refill_interval.is_zero() {
                    return Err(ConfigError::Validation(format!(
                        "rule '{rule_name}' must have a non-zero refill interval"
                    )));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimitRule {
    pub name: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub methods: Vec<String>,
    pub algorithm: RateLimitAlgorithm,
    #[serde(default = "default_behavior")]
    pub behavior: RateLimitBehavior,
}

fn default_behavior() -> RateLimitBehavior {
    RateLimitBehavior::Block
}

impl RateLimitRule {
    pub fn matches(&self, method: &str, path: &str) -> bool {
        let method_matches = self.methods.is_empty()
            || self
                .methods
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(method));
        let path_matches = self.path.as_ref().is_none_or(|candidate| candidate == path);

        method_matches && path_matches
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimitConfig {
    pub rules: Vec<RateLimitRule>,
    #[serde(default = "default_queue_key")]
    pub queue_key: String,
}

fn default_queue_key() -> String {
    "ratelimiter:throttled".to_string()
}

impl RateLimitConfig {
    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        let contents = fs::read_to_string(path_ref).map_err(|source| ConfigError::ReadFile {
            path: path_ref.display().to_string(),
            source,
        })?;
        let config: Self = serde_yaml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.rules.is_empty() {
            return Err(ConfigError::Validation(
                "at least one rate-limit rule is required".to_string(),
            ));
        }

        for rule in &self.rules {
            if rule.name.trim().is_empty() {
                return Err(ConfigError::Validation(
                    "rule name must not be empty".to_string(),
                ));
            }

            rule.algorithm.validate(&rule.name)?;
        }

        Ok(())
    }

    pub fn find_rule(&self, method: &str, path: &str) -> Option<&RateLimitRule> {
        self.rules.iter().find(|rule| rule.matches(method, path))
    }
}

mod duration_seconds {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(seconds))
    }
}

#[cfg(test)]
mod tests {
    use super::{RateLimitAlgorithm, RateLimitBehavior, RateLimitConfig, RateLimitRule};
    use std::time::Duration;

    #[test]
    fn rule_matching_respects_method_and_path() {
        let rule = RateLimitRule {
            name: "login".to_string(),
            path: Some("/login".to_string()),
            methods: vec!["POST".to_string()],
            algorithm: RateLimitAlgorithm::TokenBucket {
                capacity: 5,
                refill_tokens: 1,
                refill_interval: Duration::from_secs(1),
            },
            behavior: RateLimitBehavior::Queue,
        };

        assert!(rule.matches("POST", "/login"));
        assert!(!rule.matches("GET", "/login"));
        assert!(!rule.matches("POST", "/signup"));
    }

    #[test]
    fn config_finds_first_matching_rule() {
        let config = RateLimitConfig {
            rules: vec![RateLimitRule {
                name: "global".to_string(),
                path: Some("/status".to_string()),
                methods: vec!["GET".to_string()],
                algorithm: RateLimitAlgorithm::FixedWindow {
                    requests_per_window: 10,
                    window: Duration::from_secs(1),
                },
                behavior: RateLimitBehavior::Block,
            }],
            queue_key: "queue".to_string(),
        };

        let rule = config.find_rule("GET", "/status").unwrap();
        assert_eq!(rule.name, "global");
    }
}
