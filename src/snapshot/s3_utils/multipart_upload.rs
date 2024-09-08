use s3::request::tokio_backend::HyperRequest as RequestImpl;
use s3::{
    bucket::CHUNK_SIZE as S3_CHUNK_SIZE,
    command::{Command, Multipart},
    error::S3Error,
    request::{Request, ResponseData},
    serde_types::Part,
    utils::{read_chunk_async, PutStreamResponse},
    Bucket,
};
use tokio::io::AsyncRead;

// Slightly modified implementation of the multipart upload to avoid loading all chunks into
// memory and limiting concurrency.
pub async fn put_object_stream_custom<R: AsyncRead + Unpin>(
    bucket: &Bucket,
    reader: &mut R,
    s3_path: impl AsRef<str>,
) -> Result<PutStreamResponse, S3Error> {
    pub_oject_stream_with_content_type_and_retries(
        bucket,
        reader,
        s3_path.as_ref(),
        "application/octet-stream",
    )
    .await
}

async fn pub_oject_stream_with_content_type_and_retries<R: AsyncRead + Unpin>(
    bucket: &Bucket,
    reader: &mut R,
    s3_path: &str,
    content_type: &str,
) -> Result<PutStreamResponse, S3Error> {
    // If the file is smaller CHUNK_SIZE, just do a regular upload.
    // Otherwise perform a multi-part upload.
    let first_chunk = read_chunk_async(reader).await?;
    if first_chunk.len() < S3_CHUNK_SIZE {
        let total_size = first_chunk.len();
        let response_data = bucket
            .put_object_with_content_type(s3_path, first_chunk.as_slice(), content_type)
            .await?;
        if response_data.status_code() >= 300 {
            return Err(error_from_response_data(response_data)?);
        }
        return Ok(PutStreamResponse::new(
            response_data.status_code(),
            total_size,
        ));
    }

    let msg = bucket
        .initiate_multipart_upload(s3_path, content_type)
        .await?;
    let path = msg.key;
    let upload_id = &msg.upload_id;

    let mut part_number: u32 = 1;
    let mut etags = Vec::new();

    // Collect request handles
    let mut total_size = first_chunk.len();

    // Upload the first chunk
    let response = make_multipart_request(
        bucket,
        &path,
        first_chunk,
        part_number,
        upload_id,
        content_type,
    )
    .await?;
    if !(200..300).contains(&response.status_code()) {
        // if chunk upload failed - abort the upload
        bucket.abort_upload(&path, upload_id).await?;
        return Err(error_from_response_data(response)?);
    }
    let etag = response.as_str()?;
    etags.push(etag.to_string());

    loop {
        let mut handles = vec![];
        let mut done = false;

        for _ in 0..10 {
            let chunk = read_chunk_async(reader).await?;
            total_size += chunk.len();

            done = chunk.len() < S3_CHUNK_SIZE;

            // Start chunk upload
            part_number += 1;
            handles.push(make_multipart_request(
                bucket,
                &path,
                chunk,
                part_number,
                upload_id,
                content_type,
            ));

            if done {
                break;
            }
        }

        // Wait for all chunks to finish (or fail)
        let responses = futures::future::join_all(handles).await;

        for response in responses {
            let response_data = response?;
            if !(200..300).contains(&response_data.status_code()) {
                // if chunk upload failed - abort the upload
                match bucket.abort_upload(&path, upload_id).await {
                    Ok(_) => {
                        return Err(error_from_response_data(response_data)?);
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }
            }

            let etag = response_data.as_str()?;
            etags.push(etag.to_string());
        }

        if done {
            break;
        }
    }

    // Finish the upload
    let inner_data = etags
        .into_iter()
        .enumerate()
        .map(|(i, x)| Part {
            etag: x,
            part_number: i as u32 + 1,
        })
        .collect::<Vec<Part>>();
    let response_data = bucket
        .complete_multipart_upload(&path, &msg.upload_id, inner_data)
        .await?;

    Ok(PutStreamResponse::new(
        response_data.status_code(),
        total_size,
    ))
}

async fn make_multipart_request(
    bucket: &Bucket,
    path: &str,
    chunk: Vec<u8>,
    part_number: u32,
    upload_id: &str,
    content_type: &str,
) -> Result<ResponseData, S3Error> {
    let command = Command::PutObject {
        content: &chunk,
        multipart: Some(Multipart::new(part_number, upload_id)), // upload_id: &msg.upload_id,
        content_type,
    };
    let request = RequestImpl::new(bucket, path, command).await?;
    request.response_data(true).await
}

fn error_from_response_data(response_data: ResponseData) -> Result<S3Error, S3Error> {
    let utf8_content = String::from_utf8(response_data.as_slice().to_vec())?;
    Err(S3Error::HttpFailWithBody(
        response_data.status_code(),
        utf8_content,
    ))
}
