use crate::api::api::PhotonApi;
use crate::api::method::get_compressed_accounts_by_owner::PaginatedAccountList;
use crate::api::method::get_compressed_token_account_balance::TokenAccountBalance;
use crate::api::method::get_compressed_token_balances_by_owner::TokenBalance;
use crate::api::method::get_compressed_token_balances_by_owner::TokenBalanceList;
use crate::api::method::get_multiple_compressed_account_proofs::MerkleProofWithContext;
use crate::api::method::get_multiple_compressed_accounts::AccountList;

use crate::api::method::get_transaction_with_compression_info::AccountWithOptionalTokenData;
use crate::api::method::utils::Context;
use crate::api::method::utils::Limit;
use crate::api::method::utils::PaginatedSignatureInfoList;
use crate::api::method::utils::SignatureInfo;
use crate::api::method::utils::SignatureInfoList;
use crate::api::method::utils::TokenAcccount;
use crate::api::method::utils::TokenAccountList;
use crate::common::typedefs::account::Account;
use crate::common::typedefs::account::AccountData;
use crate::common::typedefs::bs58_string::Base58String;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::serializable_signature::SerializableSignature;
use crate::common::typedefs::token_data::AccountState;
use crate::common::typedefs::token_data::TokenData;
use dirs;
use utoipa::openapi::Response;

use crate::common::relative_project_path;

use utoipa::openapi::path::OperationBuilder;

use utoipa::openapi::request_body::RequestBodyBuilder;

use utoipa::openapi::ContentBuilder;

use utoipa::openapi::ObjectBuilder;
use utoipa::openapi::PathItem;
use utoipa::openapi::PathItemType;

use utoipa::openapi::RefOr;
use utoipa::openapi::Required;
use utoipa::openapi::ResponseBuilder;
use utoipa::openapi::ResponsesBuilder;
use utoipa::openapi::Schema;
use utoipa::openapi::SchemaType;

use utoipa::openapi::ServerBuilder;
use utoipa::OpenApi;

const JSON_CONTENT_TYPE: &str = "application/json";

#[derive(OpenApi)]
#[openapi(components(schemas(
    SerializablePubkey,
    Context,
    Hash,
    PaginatedAccountList,
    Account,
    MerkleProofWithContext,
    TokenAccountList,
    TokenAcccount,
    TokenAccountBalance,
    AccountList,
    Limit,
    Base58String,
    Base64String,
    SignatureInfoList,
    PaginatedSignatureInfoList,
    SignatureInfo,
    SerializableSignature,
    TokenBalanceList,
    TokenBalance,
    TokenData,
    AccountData,
    AccountState,
    AccountWithOptionalTokenData
)))]
struct ApiDoc;

fn add_string_property(
    builder: ObjectBuilder,
    name: &str,
    value: &str,
    description: &str,
) -> ObjectBuilder {
    let string_object = ObjectBuilder::new()
        .schema_type(SchemaType::String)
        .description(Some(description.to_string()))
        .enum_values(Some(vec![value.to_string()]))
        .build();

    let string_schema = RefOr::T(Schema::Object(string_object));
    builder.property(name, string_schema)
}

fn build_error_response(description: &str) -> Response {
    ResponseBuilder::new()
        .description(description)
        .content(
            JSON_CONTENT_TYPE,
            ContentBuilder::new()
                .schema(Schema::Object(
                    ObjectBuilder::new()
                        .property(
                            "error",
                            RefOr::T(Schema::Object(
                                ObjectBuilder::new().schema_type(SchemaType::String).build(),
                            )),
                        )
                        .build(),
                ))
                .build(),
        )
        .build()
}

fn request_schema(name: &str, params: Option<RefOr<Schema>>) -> RefOr<Schema> {
    let mut builder = ObjectBuilder::new();

    builder = add_string_property(
        builder,
        "jsonrpc",
        "2.0",
        "The version of the JSON-RPC protocol.",
    );
    builder = add_string_property(
        builder,
        "id",
        "test-account",
        "An ID to identify the request.",
    );
    builder = add_string_property(builder, "method", name, "The name of the method to invoke.");
    builder = builder
        .required("jsonrpc")
        .required("id")
        .required("method");

    if let Some(params) = params {
        builder = builder.property("params", params);
        builder = builder.required("params");
    }

    RefOr::T(Schema::Object(builder.build()))
}

// Examples of allOf references are always {}, which is incorrect.
#[allow(non_snake_case)]
fn fix_examples_for_allOf_references(schema: RefOr<Schema>) -> RefOr<Schema> {
    match schema {
        RefOr::T(mut schema) => match schema {
            Schema::Object(ref mut object) => RefOr::T(match object.schema_type {
                SchemaType::Object => {
                    object.properties = object
                        .properties
                        .iter()
                        .map(|(key, value)| {
                            let new_value = fix_examples_for_allOf_references(value.clone());
                            (key.clone(), new_value)
                        })
                        .collect();
                    schema
                }
                _ => schema,
            }),
            Schema::AllOf(ref all_of) => all_of.items[0].clone(),
            _ => RefOr::T(schema),
        },
        RefOr::Ref(_) => schema,
    }
}

pub fn update_docs(is_test: bool) {
    let method_api_specs = PhotonApi::method_api_specs();

    for spec in method_api_specs {
        let mut doc = ApiDoc::openapi();
        doc.components = doc.components.map(|components| {
            let mut components = components.clone();
            components.schemas = components
                .schemas
                .iter()
                .map(|(k, v)| (k.clone(), fix_examples_for_allOf_references(v.clone())))
                .collect();
            components
        });
        let content = ContentBuilder::new()
            .schema(request_schema(&spec.name, spec.request))
            .build();
        let request_body = RequestBodyBuilder::new()
            .content(JSON_CONTENT_TYPE, content)
            .required(Some(Required::True))
            .build();
        let responses = ResponsesBuilder::new().response(
            "200",
            ResponseBuilder::new().content(
                JSON_CONTENT_TYPE,
                ContentBuilder::new().schema(fix_examples_for_allOf_references(spec.response)).build(),
            ),
        ).response("400", build_error_response("Invalid request."))
        .response("401", build_error_response("Unauthorized request."))
        .response("403", build_error_response("Request was forbidden."))
        .response("404", build_error_response("The specified resource was not found."))
        .response("429", build_error_response("Exceeded rate limit."))
        .response("500", build_error_response("The server encountered an unexpected condition that prevented it from fulfilling the request."));
        let operation = OperationBuilder::new()
            .request_body(Some(request_body))
            .responses(responses)
            .build();
        let mut path_item = PathItem::new(PathItemType::Post, operation);

        path_item.summary = Some(spec.name.clone());
        doc.paths.paths.insert("/".to_string(), path_item);
        doc.servers = Some(vec![ServerBuilder::new()
            .url("http://127.0.0.1".to_string())
            .build()]);
        let yaml = doc.to_yaml().unwrap();

        let path = match is_test {
            true => {
                let tmp_directory = dirs::home_dir().unwrap().join(".tmp");

                // Create tmp directory if it does not exist
                if !tmp_directory.exists() {
                    std::fs::create_dir(&tmp_directory).unwrap();
                }

                relative_project_path(&format!(
                    "{}/{}.test.yaml",
                    tmp_directory.display(),
                    spec.name.clone()
                ))
            }
            false => {
                relative_project_path(&format!("src/openapi/specs/{}.yaml", spec.name.clone()))
            }
        };

        std::fs::write(path.clone(), yaml).unwrap();

        // Call the external swagger-cli validate command and fail if it fails
        let validate_result = std::process::Command::new("swagger-cli")
            .arg("validate")
            .arg(path.to_str().unwrap())
            .output()
            .unwrap();

        if !validate_result.status.success() {
            let stderr = String::from_utf8_lossy(&validate_result.stderr);
            panic!(
                "Failed to validate OpenAPI schema for {}. {}",
                spec.name, stderr
            );
        }
    }
}
