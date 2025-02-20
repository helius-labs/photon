use crate::api::method::get_compressed_accounts_by_owner::{
    get_compressed_accounts_by_owner, get_compressed_accounts_by_owner_v2,
    GetCompressedAccountsByOwnerRequest, GetCompressedAccountsByOwnerResponse,
    GetCompressedAccountsByOwnerV2Response,
};
use crate::api::method::get_multiple_compressed_account_proofs::{
    get_multiple_compressed_account_proofs, GetMultipleCompressedAccountProofsResponse, HashList,
};
use crate::api::method::get_queue_elements::{
    get_queue_elements, GetQueueElementsRequest, GetQueueElementsResponse,
};
use crate::api::method::get_validity_proof::{
    get_validity_proof, get_validity_proof_v2, GetValidityProofRequest,
    GetValidityProofRequestDocumentation, GetValidityProofResponse,
};
use crate::api::method::utils::{
    AccountBalanceResponse, GetLatestSignaturesRequest, GetNonPaginatedSignaturesResponse,
    GetNonPaginatedSignaturesResponseWithError, GetPaginatedSignaturesResponse, HashRequest,
    TokenAccountListResponse, TokenAccountListResponseV2,
};
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement};
use solana_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use utoipa::openapi::{ObjectBuilder, RefOr, Schema, SchemaType};
use utoipa::ToSchema;

use super::method::get_compressed_account::{
    get_compressed_account, get_compressed_account_v2, AccountResponse, AccountResponseV2,
};
use super::method::get_compressed_balance_by_owner::{
    get_compressed_balance_by_owner, GetCompressedBalanceByOwnerRequest,
};
use super::method::get_compressed_mint_token_holders::{
    get_compressed_mint_token_holders, GetCompressedMintTokenHoldersRequest, OwnerBalancesResponse,
};
use super::method::get_compressed_token_accounts_by_delegate::{
    get_compressed_account_token_accounts_by_delegate,
    get_compressed_account_token_accounts_by_delegate_v2,
};
use super::method::get_compressed_token_accounts_by_owner::{
    get_compressed_token_accounts_by_owner, get_compressed_token_accounts_by_owner_v2,
};
use super::method::get_compressed_token_balances_by_owner::{
    get_compressed_token_balances_by_owner, get_compressed_token_balances_by_owner_v2,
    GetCompressedTokenBalancesByOwnerRequest, TokenBalancesResponse, TokenBalancesResponseV2,
};
use super::method::get_compression_signatures_for_account::get_compression_signatures_for_account;
use super::method::get_compression_signatures_for_address::{
    get_compression_signatures_for_address, GetCompressionSignaturesForAddressRequest,
};
use super::method::get_compression_signatures_for_owner::{
    get_compression_signatures_for_owner, GetCompressionSignaturesForOwnerRequest,
};
use super::method::get_compression_signatures_for_token_owner::{
    get_compression_signatures_for_token_owner, GetCompressionSignaturesForTokenOwnerRequest,
};
use super::method::get_latest_compression_signatures::get_latest_compression_signatures;
use super::method::get_latest_non_voting_signatures::get_latest_non_voting_signatures;
use super::method::get_multiple_compressed_accounts::{
    get_multiple_compressed_accounts, get_multiple_compressed_accounts_v2,
    GetMultipleCompressedAccountsRequest, GetMultipleCompressedAccountsResponse,
    GetMultipleCompressedAccountsResponseV2,
};
use super::method::get_multiple_new_address_proofs::{
    get_multiple_new_address_proofs, get_multiple_new_address_proofs_v2, AddressList,
    AddressListWithTrees, GetMultipleNewAddressProofsResponse,
};
use super::method::get_transaction_with_compression_info::{
    get_transaction_with_compression_info, get_transaction_with_compression_info_v2,
    GetTransactionRequest, GetTransactionResponse, GetTransactionResponseV2,
};
use super::method::utils::{
    CompressedAccountRequest, GetCompressedTokenAccountsByDelegate,
    GetCompressedTokenAccountsByOwner,
};
use super::{
    error::PhotonApiError,
    method::{
        get_compressed_account_balance::get_compressed_account_balance,
        get_compressed_account_proof::{
            get_compressed_account_proof, GetCompressedAccountProofResponse,
        },
        get_compressed_token_account_balance::{
            get_compressed_token_account_balance, GetCompressedTokenAccountBalanceResponse,
        },
        get_indexer_health::get_indexer_health,
        get_indexer_slot::get_indexer_slot,
    },
};

pub struct PhotonApi {
    db_conn: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    prover_url: String,
}

impl PhotonApi {
    pub fn new(
        db_conn: Arc<DatabaseConnection>,
        rpc_client: Arc<RpcClient>,
        prover_url: String,
    ) -> Self {
        Self {
            db_conn,
            rpc_client,
            prover_url,
        }
    }
}

pub struct OpenApiSpec {
    pub name: String,
    pub request: Option<RefOr<Schema>>,
    pub response: RefOr<Schema>,
}

impl PhotonApi {
    pub async fn liveness(&self) -> Result<(), PhotonApiError> {
        Ok(())
    }

    pub async fn readiness(&self) -> Result<(), PhotonApiError> {
        self.db_conn
            .execute(Statement::from_string(
                self.db_conn.as_ref().get_database_backend(),
                "SELECT 1".to_string(),
            ))
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    pub async fn get_compressed_account(
        &self,
        request: CompressedAccountRequest,
    ) -> Result<AccountResponse, PhotonApiError> {
        get_compressed_account(&self.db_conn, request).await
    }

    pub async fn get_compressed_account_v2(
        &self,
        request: CompressedAccountRequest,
    ) -> Result<AccountResponseV2, PhotonApiError> {
        get_compressed_account_v2(&self.db_conn, request).await
    }

    pub async fn get_compressed_account_proof(
        &self,
        request: HashRequest,
    ) -> Result<GetCompressedAccountProofResponse, PhotonApiError> {
        get_compressed_account_proof(&self.db_conn, request).await
    }

    pub async fn get_multiple_compressed_account_proofs(
        &self,
        request: HashList,
    ) -> Result<GetMultipleCompressedAccountProofsResponse, PhotonApiError> {
        get_multiple_compressed_account_proofs(self.db_conn.as_ref(), request).await
    }

    pub async fn get_multiple_new_address_proofs(
        &self,
        request: AddressList,
    ) -> Result<GetMultipleNewAddressProofsResponse, PhotonApiError> {
        get_multiple_new_address_proofs(self.db_conn.as_ref(), request).await
    }

    pub async fn get_multiple_new_address_proofs_v2(
        &self,
        request: AddressListWithTrees,
    ) -> Result<GetMultipleNewAddressProofsResponse, PhotonApiError> {
        get_multiple_new_address_proofs_v2(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compressed_token_accounts_by_owner(
        &self,
        request: GetCompressedTokenAccountsByOwner,
    ) -> Result<TokenAccountListResponse, PhotonApiError> {
        get_compressed_token_accounts_by_owner(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_accounts_by_owner_v2(
        &self,
        request: GetCompressedTokenAccountsByOwner,
    ) -> Result<TokenAccountListResponseV2, PhotonApiError> {
        get_compressed_token_accounts_by_owner_v2(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_accounts_by_delegate(
        &self,
        request: GetCompressedTokenAccountsByDelegate,
    ) -> Result<TokenAccountListResponse, PhotonApiError> {
        get_compressed_account_token_accounts_by_delegate(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_accounts_by_delegate_v2(
        &self,
        request: GetCompressedTokenAccountsByDelegate,
    ) -> Result<TokenAccountListResponseV2, PhotonApiError> {
        get_compressed_account_token_accounts_by_delegate_v2(&self.db_conn, request).await
    }

    pub async fn get_compressed_balance_by_owner(
        &self,
        request: GetCompressedBalanceByOwnerRequest,
    ) -> Result<AccountBalanceResponse, PhotonApiError> {
        get_compressed_balance_by_owner(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_balances_by_owner(
        &self,
        request: GetCompressedTokenBalancesByOwnerRequest,
    ) -> Result<TokenBalancesResponse, PhotonApiError> {
        get_compressed_token_balances_by_owner(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_balances_by_owner_v2(
        &self,
        request: GetCompressedTokenBalancesByOwnerRequest,
    ) -> Result<TokenBalancesResponseV2, PhotonApiError> {
        get_compressed_token_balances_by_owner_v2(&self.db_conn, request).await
    }

    pub async fn get_compressed_token_account_balance(
        &self,
        request: CompressedAccountRequest,
    ) -> Result<GetCompressedTokenAccountBalanceResponse, PhotonApiError> {
        get_compressed_token_account_balance(&self.db_conn, request).await
    }

    pub async fn get_compressed_account_balance(
        &self,
        request: CompressedAccountRequest,
    ) -> Result<AccountBalanceResponse, PhotonApiError> {
        get_compressed_account_balance(&self.db_conn, request).await
    }

    pub async fn get_indexer_health(&self) -> Result<String, PhotonApiError> {
        get_indexer_health(self.db_conn.as_ref(), &self.rpc_client).await
    }

    pub async fn get_indexer_slot(&self) -> Result<UnsignedInteger, PhotonApiError> {
        get_indexer_slot(self.db_conn.as_ref()).await
    }

    pub async fn get_queue_elements(
        &self,
        request: GetQueueElementsRequest,
    ) -> Result<GetQueueElementsResponse, PhotonApiError> {
        get_queue_elements(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compressed_accounts_by_owner(
        &self,
        request: GetCompressedAccountsByOwnerRequest,
    ) -> Result<GetCompressedAccountsByOwnerResponse, PhotonApiError> {
        get_compressed_accounts_by_owner(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compressed_accounts_by_owner_v2(
        &self,
        request: GetCompressedAccountsByOwnerRequest,
    ) -> Result<GetCompressedAccountsByOwnerV2Response, PhotonApiError> {
        get_compressed_accounts_by_owner_v2(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compressed_mint_token_holders(
        &self,
        request: GetCompressedMintTokenHoldersRequest,
    ) -> Result<OwnerBalancesResponse, PhotonApiError> {
        get_compressed_mint_token_holders(self.db_conn.as_ref(), request).await
    }

    pub async fn get_multiple_compressed_accounts(
        &self,
        request: GetMultipleCompressedAccountsRequest,
    ) -> Result<GetMultipleCompressedAccountsResponse, PhotonApiError> {
        get_multiple_compressed_accounts(self.db_conn.as_ref(), request).await
    }

    pub async fn get_multiple_compressed_accounts_v2(
        &self,
        request: GetMultipleCompressedAccountsRequest,
    ) -> Result<GetMultipleCompressedAccountsResponseV2, PhotonApiError> {
        get_multiple_compressed_accounts_v2(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compression_signatures_for_account(
        &self,
        request: HashRequest,
    ) -> Result<GetNonPaginatedSignaturesResponse, PhotonApiError> {
        get_compression_signatures_for_account(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compression_signatures_for_address(
        &self,
        request: GetCompressionSignaturesForAddressRequest,
    ) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
        get_compression_signatures_for_address(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compression_signatures_for_owner(
        &self,
        request: GetCompressionSignaturesForOwnerRequest,
    ) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
        get_compression_signatures_for_owner(self.db_conn.as_ref(), request).await
    }

    pub async fn get_compression_signatures_for_token_owner(
        &self,
        request: GetCompressionSignaturesForTokenOwnerRequest,
    ) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
        get_compression_signatures_for_token_owner(self.db_conn.as_ref(), request).await
    }

    pub async fn get_transaction_with_compression_info(
        &self,
        request: GetTransactionRequest,
    ) -> Result<GetTransactionResponse, PhotonApiError> {
        get_transaction_with_compression_info(self.db_conn.as_ref(), &self.rpc_client, request)
            .await
    }

    pub async fn get_transaction_with_compression_info_v2(
        &self,
        request: GetTransactionRequest,
    ) -> Result<GetTransactionResponseV2, PhotonApiError> {
        get_transaction_with_compression_info_v2(self.db_conn.as_ref(), &self.rpc_client, request)
            .await
    }

    pub async fn get_validity_proof(
        &self,
        request: GetValidityProofRequest,
    ) -> Result<GetValidityProofResponse, PhotonApiError> {
        get_validity_proof(self.db_conn.as_ref(), &self.prover_url, request).await
    }

    pub async fn get_validity_proof_v2(
        &self,
        request: GetValidityProofRequest,
    ) -> Result<GetValidityProofResponse, PhotonApiError> {
        get_validity_proof_v2(self.db_conn.as_ref(), &self.prover_url, request).await
    }

    pub async fn get_latest_compression_signatures(
        &self,
        request: GetLatestSignaturesRequest,
    ) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
        get_latest_compression_signatures(self.db_conn.as_ref(), request).await
    }

    pub async fn get_latest_non_voting_signatures(
        &self,
        request: GetLatestSignaturesRequest,
    ) -> Result<GetNonPaginatedSignaturesResponseWithError, PhotonApiError> {
        get_latest_non_voting_signatures(self.db_conn.as_ref(), request).await
    }
    pub fn method_api_specs() -> Vec<OpenApiSpec> {
        vec![
            OpenApiSpec {
                name: "getQueueElements".to_string(),
                request: Some(GetQueueElementsRequest::schema().1),
                response: GetQueueElementsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccount".to_string(),
                request: Some(CompressedAccountRequest::adjusted_schema()),
                response: AccountResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccountV2".to_string(),
                request: Some(CompressedAccountRequest::adjusted_schema()),
                response: AccountResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccountBalance".to_string(),
                request: Some(CompressedAccountRequest::adjusted_schema()),
                response: AccountBalanceResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountBalance".to_string(),
                request: Some(CompressedAccountRequest::adjusted_schema()),
                response: GetCompressedTokenAccountBalanceResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedBalanceByOwner".to_string(),
                request: Some(GetCompressedBalanceByOwnerRequest::schema().1),
                response: AccountBalanceResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenBalancesByOwner".to_string(),
                request: Some(GetCompressedTokenBalancesByOwnerRequest::schema().1),
                response: TokenBalancesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenBalancesByOwnerV2".to_string(),
                request: Some(GetCompressedTokenBalancesByOwnerRequest::schema().1),
                response: TokenBalancesResponseV2::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccountsByOwner".to_string(),
                request: Some(GetCompressedAccountsByOwnerRequest::schema().1),
                response: GetCompressedAccountsByOwnerResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccountsByOwnerV2".to_string(),
                request: Some(GetCompressedAccountsByOwnerRequest::schema().1),
                response: GetCompressedAccountsByOwnerV2Response::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedMintTokenHolders".to_string(),
                request: Some(GetCompressedMintTokenHoldersRequest::schema().1),
                response: OwnerBalancesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getMultipleCompressedAccounts".to_string(),
                request: Some(GetMultipleCompressedAccountsRequest::adjusted_schema()),
                response: GetMultipleCompressedAccountsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getMultipleCompressedAccountsV2".to_string(),
                request: Some(GetMultipleCompressedAccountsRequest::adjusted_schema()),
                response: GetMultipleCompressedAccountsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountsByOwner".to_string(),
                request: Some(GetCompressedTokenAccountsByOwner::schema().1),
                response: TokenAccountListResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountsByOwnerV2".to_string(),
                request: Some(GetCompressedTokenAccountsByOwner::schema().1),
                response: TokenAccountListResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountsByDelegate".to_string(),
                request: Some(GetCompressedTokenAccountsByDelegate::schema().1),
                response: TokenAccountListResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedTokenAccountsByDelegateV2".to_string(),
                request: Some(GetCompressedTokenAccountsByDelegate::schema().1),
                response: TokenAccountListResponse::schema().1,
            },
            OpenApiSpec {
                name: "getTransactionWithCompressionInfo".to_string(),
                request: Some(GetTransactionRequest::schema().1),
                response: GetTransactionResponse::schema().1,
            },
            OpenApiSpec {
                name: "getTransactionWithCompressionInfoV2".to_string(),
                request: Some(GetTransactionRequest::schema().1),
                response: GetTransactionResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressedAccountProof".to_string(),
                request: Some(HashRequest::schema().1),
                response: GetCompressedAccountProofResponse::schema().1,
            },
            OpenApiSpec {
                name: "getMultipleCompressedAccountProofs".to_string(),
                request: Some(HashList::schema().1),
                response: GetMultipleCompressedAccountProofsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getMultipleNewAddressProofs".to_string(),
                request: Some(AddressList::schema().1),
                response: GetMultipleNewAddressProofsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getMultipleNewAddressProofsV2".to_string(),
                request: Some(AddressListWithTrees::schema().1),
                response: GetMultipleNewAddressProofsResponse::schema().1,
            },
            OpenApiSpec {
                name: "getValidityProof".to_string(),
                request: Some(GetValidityProofRequestDocumentation::schema().1),
                response: GetValidityProofResponse::schema().1,
            },
            OpenApiSpec {
                name: "getValidityProofV2".to_string(),
                request: Some(GetValidityProofRequestDocumentation::schema().1),
                response: GetValidityProofResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForAccount".to_string(),
                request: Some(HashRequest::schema().1),
                response: GetNonPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForAddress".to_string(),
                request: Some(GetCompressionSignaturesForAddressRequest::schema().1),
                response: GetPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForOwner".to_string(),
                request: Some(GetCompressionSignaturesForOwnerRequest::schema().1),
                response: GetPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getCompressionSignaturesForTokenOwner".to_string(),
                request: Some(GetCompressionSignaturesForTokenOwnerRequest::schema().1),
                response: GetPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getLatestCompressionSignatures".to_string(),
                request: Some(GetLatestSignaturesRequest::schema().1),
                response: GetPaginatedSignaturesResponse::schema().1,
            },
            OpenApiSpec {
                name: "getLatestNonVotingSignatures".to_string(),
                request: Some(GetLatestSignaturesRequest::schema().1),
                response: GetNonPaginatedSignaturesResponseWithError::schema().1,
            },
            OpenApiSpec {
                name: "getIndexerHealth".to_string(),
                request: None,
                response: RefOr::T(Schema::Object(
                    ObjectBuilder::new()
                        .schema_type(SchemaType::String)
                        .description(Some("ok if healthy"))
                        .default(Some(serde_json::Value::String("ok".to_string())))
                        .enum_values(Some(vec!["ok".to_string()]))
                        .build(),
                )),
            },
            OpenApiSpec {
                name: "getIndexerSlot".to_string(),
                request: None,
                response: UnsignedInteger::schema().1,
            },
        ]
    }
}
