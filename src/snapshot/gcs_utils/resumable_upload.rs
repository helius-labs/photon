use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt};
use log::{debug, info, warn};
use reqwest::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE};
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

// 8 MB chunk size (GCS recommends multiples of 256KB, minimum 256KB for resumable)
const CHUNK_SIZE: usize = 8 * 1024 * 1024;
const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF_MS: u64 = 1000;

/// Performs a resumable upload to Google Cloud Storage.
/// This handles large files by uploading in chunks and supports resuming on failure.
pub async fn resumable_upload(
    bucket: &str,
    object_name: &str,
    byte_stream: impl Stream<Item = Result<Bytes>> + Send + 'static,
    access_token: &str,
) -> Result<()> {
    // Step 1: Initiate the resumable upload session
    let upload_uri = initiate_resumable_upload(bucket, object_name, access_token).await?;
    info!(
        "Initiated resumable upload for {}/{}, upload URI obtained",
        bucket, object_name
    );

    // Step 2: Upload chunks
    upload_chunks(&upload_uri, byte_stream, access_token).await?;

    info!(
        "Successfully completed resumable upload for {}/{}",
        bucket, object_name
    );
    Ok(())
}

/// Initiates a resumable upload session and returns the upload URI
async fn initiate_resumable_upload(
    bucket: &str,
    object_name: &str,
    access_token: &str,
) -> Result<String> {
    let client = Client::new();
    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=resumable&name={}",
        bucket, object_name
    );

    for attempt in 0..MAX_RETRIES {
        let response = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header(CONTENT_TYPE, "application/json")
            .header("X-Upload-Content-Type", "application/octet-stream")
            .body("{}")
            .send()
            .await;

        match response {
            Ok(resp) => {
                if resp.status().is_success() {
                    let upload_uri = resp
                        .headers()
                        .get("Location")
                        .ok_or_else(|| anyhow!("No Location header in resumable upload response"))?
                        .to_str()
                        .context("Invalid Location header")?
                        .to_string();
                    return Ok(upload_uri);
                } else if resp.status().is_server_error() || resp.status().as_u16() == 429 {
                    // Retry on 5xx or 429 (rate limit)
                    let backoff = INITIAL_BACKOFF_MS * 2u64.pow(attempt);
                    warn!(
                        "Resumable upload initiation failed with status {}, retrying in {}ms (attempt {}/{})",
                        resp.status(),
                        backoff,
                        attempt + 1,
                        MAX_RETRIES
                    );
                    sleep(Duration::from_millis(backoff)).await;
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(anyhow!(
                        "Failed to initiate resumable upload: {} - {}",
                        status,
                        body
                    ));
                }
            }
            Err(e) => {
                let backoff = INITIAL_BACKOFF_MS * 2u64.pow(attempt);
                warn!(
                    "Resumable upload initiation request failed: {}, retrying in {}ms (attempt {}/{})",
                    e, backoff, attempt + 1, MAX_RETRIES
                );
                sleep(Duration::from_millis(backoff)).await;
            }
        }
    }

    Err(anyhow!(
        "Failed to initiate resumable upload after {} retries",
        MAX_RETRIES
    ))
}

/// Uploads data in chunks to the resumable upload URI
async fn upload_chunks(
    upload_uri: &str,
    byte_stream: impl Stream<Item = Result<Bytes>> + Send + 'static,
    _access_token: &str,
) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(300)) // 5 minute timeout per chunk
        .build()?;

    pin_mut!(byte_stream);

    // First, we need to collect all data to know total size
    // For very large files, we could use unknown size (*) but that's more complex
    let mut all_data = Vec::new();
    while let Some(chunk_result) = byte_stream.next().await {
        let chunk = chunk_result?;
        all_data.extend_from_slice(&chunk);
    }

    let total_size = all_data.len() as u64;
    info!(
        "Total upload size: {} bytes ({:.2} MB)",
        total_size,
        total_size as f64 / 1024.0 / 1024.0
    );

    if total_size == 0 {
        // Handle empty file case
        let response = client
            .put(upload_uri)
            .header(CONTENT_LENGTH, "0")
            .header(CONTENT_RANGE, "bytes */*")
            .send()
            .await
            .context("Failed to upload empty file")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Failed to upload empty file: {} - {}",
                status,
                body
            ));
        }
        return Ok(());
    }

    // Upload in chunks
    let mut offset: u64 = 0;
    'outer: while offset < total_size {
        let chunk_end = std::cmp::min(offset + CHUNK_SIZE as u64, total_size);
        let chunk_data = &all_data[offset as usize..chunk_end as usize];
        let is_last_chunk = chunk_end == total_size;

        let content_range = format!("bytes {}-{}/{}", offset, chunk_end - 1, total_size);

        debug!(
            "Uploading chunk: {} ({} bytes)",
            content_range,
            chunk_data.len()
        );

        let mut attempt = 0;
        loop {
            let response = client
                .put(upload_uri)
                .header(CONTENT_LENGTH, chunk_data.len().to_string())
                .header(CONTENT_RANGE, &content_range)
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(chunk_data.to_vec())
                .send()
                .await;

            match response {
                Ok(resp) => {
                    let status = resp.status();

                    // 200 or 201 = upload complete
                    // 308 = chunk accepted, continue
                    if status.is_success() {
                        if is_last_chunk {
                            info!("Upload complete!");
                        }
                        break;
                    } else if status.as_u16() == 308 {
                        // Resume Incomplete - chunk accepted
                        debug!("Chunk uploaded successfully (308)");
                        break;
                    } else if status.is_server_error() || status.as_u16() == 429 {
                        attempt += 1;
                        if attempt >= MAX_RETRIES {
                            let body = resp.text().await.unwrap_or_default();
                            return Err(anyhow!(
                                "Failed to upload chunk after {} retries: {} - {}",
                                MAX_RETRIES,
                                status,
                                body
                            ));
                        }
                        let backoff = INITIAL_BACKOFF_MS * 2u64.pow(attempt);
                        warn!(
                            "Chunk upload failed with status {}, retrying in {}ms (attempt {}/{})",
                            status, backoff, attempt, MAX_RETRIES
                        );
                        sleep(Duration::from_millis(backoff)).await;

                        // Query the upload status to resume from correct position
                        if let Some(new_offset) =
                            query_upload_status(&client, upload_uri, total_size).await?
                        {
                            if new_offset != offset {
                                info!("Resuming from byte {} (was at {})", new_offset, offset);
                                offset = new_offset;
                                continue 'outer; // Recalculate chunk from new position
                            }
                        }
                    } else {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(anyhow!("Failed to upload chunk: {} - {}", status, body));
                    }
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        return Err(anyhow!(
                            "Failed to upload chunk after {} retries: {}",
                            MAX_RETRIES,
                            e
                        ));
                    }
                    let backoff = INITIAL_BACKOFF_MS * 2u64.pow(attempt);
                    warn!(
                        "Chunk upload request failed: {}, retrying in {}ms (attempt {}/{})",
                        e, backoff, attempt, MAX_RETRIES
                    );
                    sleep(Duration::from_millis(backoff)).await;

                    // Query the upload status to resume from correct position
                    if let Some(new_offset) =
                        query_upload_status(&client, upload_uri, total_size).await?
                    {
                        if new_offset != offset {
                            info!("Resuming from byte {} (was at {})", new_offset, offset);
                            offset = new_offset;
                            continue 'outer; // Recalculate chunk from new position
                        }
                    }
                }
            }
        }

        offset = chunk_end;

        // Log progress every 100MB
        if offset % (100 * 1024 * 1024) < CHUNK_SIZE as u64 {
            info!(
                "Upload progress: {:.1}% ({:.2} MB / {:.2} MB)",
                (offset as f64 / total_size as f64) * 100.0,
                offset as f64 / 1024.0 / 1024.0,
                total_size as f64 / 1024.0 / 1024.0
            );
        }
    }

    Ok(())
}

/// Query the current upload status to determine how many bytes have been received
async fn query_upload_status(
    client: &Client,
    upload_uri: &str,
    total_size: u64,
) -> Result<Option<u64>> {
    let response = client
        .put(upload_uri)
        .header(CONTENT_LENGTH, "0")
        .header(CONTENT_RANGE, format!("bytes */{}", total_size))
        .send()
        .await;

    match response {
        Ok(resp) => {
            let status = resp.status();
            if status.as_u16() == 308 {
                // Parse Range header to find out how much was uploaded
                if let Some(range) = resp.headers().get("Range") {
                    let range_str = range.to_str().unwrap_or("");
                    // Format: "bytes=0-N" where N is the last byte received
                    if let Some(end) = range_str.strip_prefix("bytes=0-") {
                        if let Ok(last_byte) = end.parse::<u64>() {
                            return Ok(Some(last_byte + 1));
                        }
                    }
                }
                // No Range header means nothing uploaded yet
                return Ok(Some(0));
            } else if status.is_success() {
                // Upload is already complete
                return Ok(None);
            }
            // Other status - can't determine position
            Ok(None)
        }
        Err(_) => Ok(None),
    }
}

/// Gets an access token using the service account credentials from GOOGLE_APPLICATION_CREDENTIALS
pub async fn get_access_token() -> Result<String> {
    // Use gcloud auth to get the token, or parse the service account JSON
    // The cloud-storage crate handles this internally, but we need to do it manually for reqwest

    // First try to get the token from the metadata service (when running on GCP)
    if let Ok(token) = get_token_from_metadata_service().await {
        return Ok(token);
    }

    // Fall back to service account file
    let credentials_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .context("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")?;

    get_token_from_service_account(&credentials_path).await
}

async fn get_token_from_metadata_service() -> Result<String> {
    let client = Client::builder().timeout(Duration::from_secs(5)).build()?;

    let response = client
        .get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token")
        .header("Metadata-Flavor", "Google")
        .send()
        .await?;

    if response.status().is_success() {
        let json: serde_json::Value = response.json().await?;
        let token = json["access_token"]
            .as_str()
            .ok_or_else(|| anyhow!("No access_token in metadata response"))?;
        Ok(token.to_string())
    } else {
        Err(anyhow!("Failed to get token from metadata service"))
    }
}

async fn get_token_from_service_account(credentials_path: &str) -> Result<String> {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::{Deserialize, Serialize};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Deserialize)]
    struct ServiceAccount {
        client_email: String,
        private_key: String,
        token_uri: String,
    }

    #[derive(Debug, Serialize)]
    struct Claims {
        iss: String,
        scope: String,
        aud: String,
        exp: u64,
        iat: u64,
    }

    let credentials_json = std::fs::read_to_string(credentials_path)
        .context("Failed to read service account credentials file")?;
    let sa: ServiceAccount =
        serde_json::from_str(&credentials_json).context("Failed to parse service account JSON")?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = Claims {
        iss: sa.client_email.clone(),
        scope: "https://www.googleapis.com/auth/devstorage.read_write".to_string(),
        aud: sa.token_uri.clone(),
        exp: now + 3600,
        iat: now,
    };

    let header = Header::new(Algorithm::RS256);
    let key = EncodingKey::from_rsa_pem(sa.private_key.as_bytes())
        .context("Failed to parse private key")?;
    let jwt = encode(&header, &claims, &key).context("Failed to encode JWT")?;

    // Exchange JWT for access token
    let client = Client::new();
    let response = client
        .post(&sa.token_uri)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &jwt),
        ])
        .send()
        .await
        .context("Failed to exchange JWT for access token")?;

    if response.status().is_success() {
        let json: serde_json::Value = response.json().await?;
        let token = json["access_token"]
            .as_str()
            .ok_or_else(|| anyhow!("No access_token in token response"))?;
        Ok(token.to_string())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(anyhow!("Failed to get access token: {} - {}", status, body))
    }
}
