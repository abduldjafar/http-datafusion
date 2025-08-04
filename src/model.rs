use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub sources: Vec<Source>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Source {
    pub name: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<Pagination>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pagination {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_page: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_param: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size_param: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size_default: Option<u32>,
}

impl Source {
    pub fn new(name: String, url: String) -> Self {
        Self {
            name,
            url,
            method: None,
            pagination: None,
        }
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            start_page: Some(1),
            end_page: Some(10),
            page_size: Some(10),
            page_param: Some("page".to_string()),
            page_size_param: Some("limit".to_string()),
            page_size_default: Some(10),
        }
    }
}