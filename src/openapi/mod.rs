use std::collections::HashSet;

use crate::api::api::PhotonApi;
use crate::api::method::get_compressed_accounts_by_owner::DataSlice;
use crate::api::method::get_compressed_accounts_by_owner::FilterSelector;
use crate::api::method::get_compressed_accounts_by_owner::Memcmp;
use crate::api::method::get_compressed_accounts_by_owner::PaginatedAccountList;
use crate::api::method::get_compressed_mint_token_holders::OwnerBalance;
use crate::api::method::get_compressed_mint_token_holders::OwnerBalanceList;
use crate::api::method::get_compressed_mint_token_holders::OwnerBalancesResponse;
use crate::api::method::get_compressed_token_account_balance::TokenAccountBalance;
use crate::api::method::get_compressed_token_balances_by_owner::TokenBalance;
use crate::api::method::get_compressed_token_balances_by_owner::TokenBalanceList;
use crate::api::method::get_compressed_token_balances_by_owner::TokenBalanceListV2;
use crate::api::method::get_multiple_compressed_accounts::AccountList;

use crate::api::method::get_multiple_new_address_proofs::AddressListWithTrees;
use crate::api::method::get_multiple_new_address_proofs::AddressWithTree;
use crate::api::method::get_multiple_new_address_proofs::MerkleContextWithNewAddressProof;
use crate::api::method::get_transaction_with_compression_info::AccountWithOptionalTokenData;
use crate::api::method::get_validity_proof::CompressedProof;
use crate::api::method::get_validity_proof::CompressedProofWithContext;
use crate::api::method::utils::Context;
use crate::api::method::utils::Limit;
use crate::api::method::utils::PaginatedSignatureInfoList;
use crate::api::method::utils::SignatureInfo;
use crate::api::method::utils::SignatureInfoList;
use crate::api::method::utils::SignatureInfoListWithError;
use crate::api::method::utils::SignatureInfoWithError;
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
use crate::common::typedefs::unix_timestamp::UnixTimestamp;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::ingester::persist::persisted_state_tree::MerkleProofWithContext;
use dirs;
use utoipa::openapi::Components;
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
    AccountWithOptionalTokenData,
    UnixTimestamp,
    UnsignedInteger,
    CompressedProof,
    CompressedProofWithContext,
    MerkleContextWithNewAddressProof,
    SignatureInfoListWithError,
    SignatureInfoWithError,
    DataSlice,
    FilterSelector,
    Memcmp,
    AddressListWithTrees,
    AddressWithTree,
    OwnerBalance,
    OwnerBalanceList,
    OwnerBalancesResponse,
    TokenBalanceListV2,
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

fn build_error_response_old(description: &str) -> Response {
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

fn build_error_response(description: &str) -> Response {
    let error_object = ObjectBuilder::new()
        .property(
            "code",
            RefOr::T(Schema::Object(
                ObjectBuilder::new()
                    .schema_type(SchemaType::Integer)
                    .build(),
            )),
        )
        .property(
            "message",
            RefOr::T(Schema::Object(
                ObjectBuilder::new().schema_type(SchemaType::String).build(),
            )),
        )
        .build();

    let response_schema = ObjectBuilder::new()
        .property(
            "jsonrpc",
            RefOr::T(Schema::Object(
                ObjectBuilder::new().schema_type(SchemaType::String).build(),
            )),
        )
        .property(
            "id",
            RefOr::T(Schema::Object(
                ObjectBuilder::new().schema_type(SchemaType::String).build(),
            )),
        )
        .property("error", RefOr::T(Schema::Object(error_object)))
        .build();

    ResponseBuilder::new()
        .description(description)
        .content(
            JSON_CONTENT_TYPE,
            ContentBuilder::new()
                .schema(Schema::Object(response_schema))
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

fn response_schema(result: RefOr<Schema>) -> RefOr<Schema> {
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
        "An ID to identify the response.",
    );
    builder = builder.property("result", result);

    // Add optional error property
    let error_object = ObjectBuilder::new()
        .property(
            "code",
            RefOr::T(Schema::Object(
                ObjectBuilder::new()
                    .schema_type(SchemaType::Integer)
                    .build(),
            )),
        )
        .property(
            "message",
            RefOr::T(Schema::Object(
                ObjectBuilder::new().schema_type(SchemaType::String).build(),
            )),
        )
        .build();
    builder = builder.property("error", RefOr::T(Schema::Object(error_object)));

    builder = builder.required("jsonrpc").required("id");

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

fn find_all_components(schema: RefOr<Schema>) -> HashSet<String> {
    let mut components = HashSet::new();

    match schema {
        RefOr::T(schema) => match schema {
            Schema::Object(object) => {
                for (_, value) in object.properties {
                    components.extend(find_all_components(value));
                }
            }
            Schema::Array(array) => {
                components.extend(find_all_components(*array.items));
            }
            Schema::AllOf(all_of) => {
                for item in all_of.items {
                    components.extend(find_all_components(item));
                }
            }
            Schema::OneOf(one_of) => {
                for item in one_of.items {
                    components.extend(find_all_components(item));
                }
            }
            Schema::AnyOf(any_of) => {
                for item in any_of.items {
                    components.extend(find_all_components(item));
                }
            }
            _ => {}
        },
        RefOr::Ref(ref_location) => {
            components.insert(
                ref_location
                    .ref_location
                    .split('/')
                    .last()
                    .unwrap()
                    .to_string(),
            );
        }
    }

    components
}

fn filter_unused_components(
    request: RefOr<Schema>,
    response: RefOr<Schema>,
    components: &mut Components,
) {
    let mut used_components = find_all_components(request);
    used_components.extend(find_all_components(response));

    let mut check_stack = used_components.clone();
    while !check_stack.is_empty() {
        // Pop any element from the stack
        let current = check_stack.iter().next().unwrap().clone();
        check_stack.remove(&current);
        let schema = components.schemas.get(&current).unwrap().clone();
        let child_componets = find_all_components(schema.clone());
        for child in child_componets {
            if !used_components.contains(&child) {
                used_components.insert(child.clone());
                check_stack.insert(child);
            }
        }
    }

    components.schemas = components
        .schemas
        .iter()
        .filter(|(k, _)| used_components.contains(*k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
}

pub fn update_docs_new(is_test: bool) {
    let method_api_specs = PhotonApi::method_api_specs();

    for spec in method_api_specs {
        let mut doc = ApiDoc::openapi();

        let mut components = doc.components.unwrap();
        filter_unused_components(
            spec.request.clone().unwrap_or_default(),
            spec.response.clone(),
            &mut components,
        );
        components.schemas = components
            .schemas
            .iter()
            .map(|(k, v)| (k.clone(), fix_examples_for_allOf_references(v.clone())))
            .collect();

        doc.components = Some(components);
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
        )
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
            .url("https://mainnet.helius-rpc.com?api-key=<api_key>".to_string())
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


pub fn update_docs(is_test: bool) {
    let method_api_specs = PhotonApi::method_api_specs();
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

    for spec in method_api_specs {
        let content = ContentBuilder::new()
            .schema(request_schema(&spec.name, spec.request))
            .build();
        let request_body = RequestBodyBuilder::new()
            .content(JSON_CONTENT_TYPE, content)
            .required(Some(Required::True))
            .build();
        let wrapped_response_schema =
            response_schema(fix_examples_for_allOf_references(spec.response));

        let responses = ResponsesBuilder::new().response(
            "200",
            ResponseBuilder::new().content(
                JSON_CONTENT_TYPE,
                ContentBuilder::new().schema(wrapped_response_schema).build(),
            ),
        )
        .response("429", build_error_response("Exceeded rate limit."))
        .response("500", build_error_response("The server encountered an unexpected condition that prevented it from fulfilling the request."));
        let operation = OperationBuilder::new()
            .request_body(Some(request_body))
            .responses(responses)
            .build();
        let mut path_item = PathItem::new(PathItemType::Post, operation);

        path_item.summary = Some(spec.name.clone());
        doc.paths
            .paths
            .insert(format!("/{method}", method = spec.name), path_item);
    }

    // doc.paths.paths.insert("/".to_string(), path_item);
        doc.servers = Some(vec![ServerBuilder::new()
            .url("https://devnet.helius-rpc.com?api-key=<api_key>".to_string())
            .build()]);
        let yaml = doc.to_yaml().unwrap();

        let path = match is_test {
            true => {
                let tmp_directory = dirs::home_dir().unwrap().join(".tmp");

                // Create tmp directory if it does not exist
                if !tmp_directory.exists() {
                    std::fs::create_dir(&tmp_directory).unwrap();
                }

            relative_project_path(&format!("{}/test.yaml", tmp_directory.display()))
            }
            false => {
            // relative_project_path(&format!("src/openapi/specs/{}.yaml", spec.name.clone()))
            relative_project_path("src/openapi/specs/api.yaml")
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
        panic!("Failed to validate OpenAPI schema. {}", stderr);
    }
}