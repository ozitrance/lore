// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_proto::lore::repository::v1::RepositoryCreateRequest;
use lore_proto::lore::repository::v1::RepositoryCreateResponse;
use lore_revision::branch;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryMetadata;
use lore_telemetry::InstrumentProvider;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::Span;
use tracing::info;
use tracing::warn;

use super::record::build_repository;
use super::repository_get::repository_load_id;
use super::repository_get::repository_load_name;
use crate::grpc::ServerResultExt;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_user_id;
use crate::grpc::get_write_token;
use crate::grpc::handlers::repository_create::ensure_default_branch_name_matches;
use crate::grpc::handlers::repository_create::repository_create_auth_resource;
use crate::grpc::hook_error_to_status;
use crate::grpc::warn_error_to_status;
use crate::hooks::HookContext;
use crate::hooks::HookDispatcher;
use crate::hooks::HookPoint;
use crate::util::setup_execution;

/// `lore.repository.v1.RepositoryService.RepositoryCreate` handler.
///
/// The caller pre-generates `id` and `default_branch_id` for retry
/// idempotency. The server assigns `created`. `creator` is hybrid:
/// caller-set if permitted, otherwise the authenticated JWT identity.
#[tracing::instrument(
    name = "RepositoryCreate::v1::handle",
    skip_all,
    fields(requested_repo_id)
)]
pub async fn handler(
    request: Request<RepositoryCreateRequest>,
    auth_url: Option<String>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    hook_dispatcher: &HookDispatcher,
    instrument_provider: &impl InstrumentProvider,
) -> Result<Response<RepositoryCreateResponse>, Status> {
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();
    let authorization = request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string());
    let req = request.into_inner();

    let id: RepositoryId = Context::from(req.id).into();
    let name = req.name;
    let description = req.description;
    let default_branch_id: Context = req.default_branch_id.into();
    let default_branch_name = req.default_branch_name;
    let creator = req
        .creator
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| user_id.clone());

    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();

    let execution = setup_execution(module_path!(), correlation_id.clone(), user_id.clone());
    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        id,
    ));
    Span::current().record("requested_repo_id", id.to_string());

    LORE_CONTEXT
        .scope(execution, async move {
            let hook_ctx = HookContext::builder()
                .correlation_id(correlation_id)
                .hook_point(HookPoint::RepositoryCreate)
                .repository(id)
                .user(user_id)
                .build();

            hook_dispatcher
                .dispatch_pre(HookPoint::RepositoryCreate, &hook_ctx)
                .map_err(hook_error_to_status)?;

            let (created_metadata, metadata_hash) = repository_create_inner(
                repository.clone(),
                &name,
                &description,
                default_branch_id,
                &default_branch_name,
                &creator,
                created,
                auth_url,
                authorization,
            )
            .await
            .inspect_err(|err| warn!(error = ?err, "Repository create failed"))?;

            hook_dispatcher.spawn_post(HookPoint::RepositoryCreate, hook_ctx);

            instrument_provider
                .counter("num_repositories_created")
                .add(1, &[]);

            Ok(Response::new(RepositoryCreateResponse {
                repository: Some(build_repository(id, &created_metadata, metadata_hash)),
            }))
        })
        .await
}

/// Reject oversized string fields early to prevent resource exhaustion.
fn validate_create_input(
    name: &str,
    description: &str,
    default_branch_name: &str,
    creator: &str,
) -> Result<(), Status> {
    if name.len() > repository::MAX_NAME_LEN {
        return Err(Status::invalid_argument(format!(
            "Repository name exceeds maximum length of {} bytes",
            repository::MAX_NAME_LEN,
        )));
    }
    if description.len() > repository::MAX_DESCRIPTION_LEN {
        return Err(Status::invalid_argument(format!(
            "Repository description exceeds maximum length of {} bytes",
            repository::MAX_DESCRIPTION_LEN,
        )));
    }
    if default_branch_name.len() > repository::MAX_NAME_LEN {
        return Err(Status::invalid_argument(format!(
            "Branch name exceeds maximum length of {} bytes",
            repository::MAX_NAME_LEN,
        )));
    }
    if creator.len() > repository::MAX_NAME_LEN {
        return Err(Status::invalid_argument(format!(
            "Creator exceeds maximum length of {} bytes",
            repository::MAX_NAME_LEN,
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn repository_create_inner(
    repository: Arc<RepositoryContext>,
    name: &str,
    description: &str,
    default_branch_id: Context,
    default_branch_name: &str,
    creator: &str,
    created: u64,
    auth_url: Option<String>,
    authorization: Option<String>,
) -> Result<(RepositoryMetadata, lore_storage::Hash), Status> {
    validate_create_input(name, description, default_branch_name, creator)?;

    if !repository::is_valid_name(name) {
        return Err(Status::invalid_argument("Invalid repository name"));
    }

    if let Ok(name_id) = Context::from_str(name)
        && !name_id.is_zero()
        && RepositoryId::from(name_id) != repository.id
    {
        return Err(Status::invalid_argument("Invalid repository name"));
    }

    if let Ok((metadata, metadata_hash)) =
        repository_load_id(repository.clone(), repository.id, None, None).await
    {
        return if metadata.name == name {
            ensure_default_branch_name_matches(
                repository.id,
                metadata.default_branch_name.as_str(),
                default_branch_name,
            )?;

            info!(
                "Repository {} already exist with name {}, early out create successful",
                repository.id, metadata.name
            );

            if repository_load_name(repository.clone(), name, None, None)
                .await
                .is_err()
            {
                info!(
                    "Recreating repository name {} -> ID {} mapping",
                    name, repository.id
                );
                let _ = repository::store_name_to_id(repository.clone(), name, repository.id)
                    .await
                    .inspect_err(|err| info!("Recreate name -> ID mapping failed: {err}"));
            }

            Ok((metadata, metadata_hash))
        } else {
            Err(Status::already_exists(format!(
                "Repository {} already exist with name {} which does not match {}",
                repository.id, metadata.name, name
            )))
        };
    }
    if let Ok((id, metadata, metadata_hash)) =
        repository_load_name(repository.clone(), name, None, None).await
    {
        return if id == repository.id {
            ensure_default_branch_name_matches(
                id,
                metadata.default_branch_name.as_str(),
                default_branch_name,
            )?;

            info!(
                "Repository {} already exist with id {}, early out create successful",
                name, id
            );
            Ok((metadata, metadata_hash))
        } else {
            Err(Status::already_exists(format!(
                "Repository {} already exist with id {} which does not match {}",
                name, id, repository.id
            )))
        };
    }

    if let Some(auth_url) = auth_url {
        let client = Box::new(crate::authnz::rebac::grpc_get_rebac_client(auth_url).await?);
        repository_create_auth_resource(client, authorization, repository.id, name).await?;
    }

    let metadata = RepositoryMetadata {
        name: name.to_string(),
        description: description.to_string(),
        default_branch: default_branch_id,
        default_branch_name: default_branch_name.to_string(),
        creator: creator.to_string(),
        created,
    };

    let metadata_hash = repository::metadata_store(repository.clone(), metadata.clone())
        .await
        .warn_map_err(|err| {
            Status::internal(format!("Failed to serialize repository metadata: {err}"))
        })?;

    let stack = vec![];
    let write_token = get_write_token();
    match branch::create(
        repository.clone(),
        &write_token,
        default_branch_id,
        default_branch_name,
        branch::default_category(),
        creator,
        created,
        stack,
        false,
        false,
    )
    .await
    {
        Ok(_) => {}
        Err(err) if err.is_branch_already_exists() => {}
        Err(err) => {
            let response = warn_error_to_status(&err, |err| {
                Status::internal(format!("Failed to create default branch: {err}"))
            });
            return Err(response);
        }
    }

    repository::metadata_store_hash(repository.clone(), metadata_hash)
        .await
        .warn_map_err(|err| {
            Status::internal(format!(
                "Failed to store metadata hash for {name}/{}: {err}",
                repository.id
            ))
        })?;

    repository::store_name_to_id(repository.clone(), name, repository.id)
        .await
        .warn_map_err(|err| {
            Status::internal(format!(
                "Failed to store name to ID lookup for {name} -> {}: {err}",
                repository.id
            ))
        })?;

    info!("Created repository {} with ID {}", name, repository.id);

    Ok((metadata, metadata_hash))
}

#[cfg(test)]
mod tests {
    mod input_length_validation {
        use lore_revision::repository;

        use super::super::*;

        #[test]
        fn accepts_valid_input() {
            validate_create_input("my-repo", "a description", "main", "alice")
                .expect("valid input should pass");
        }

        #[test]
        fn accepts_name_at_max_length() {
            let name = "a".repeat(repository::MAX_NAME_LEN);
            validate_create_input(&name, "desc", "main", "alice")
                .expect("name at exactly MAX_NAME_LEN should pass");
        }

        #[test]
        fn rejects_oversized_repository_name() {
            let long_name = "a".repeat(repository::MAX_NAME_LEN + 1);
            let err = validate_create_input(&long_name, "desc", "main", "alice")
                .expect_err("should reject oversized name");
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert!(
                err.message()
                    .contains("Repository name exceeds maximum length")
            );
        }

        #[test]
        fn rejects_oversized_description() {
            let long_desc = "a".repeat(repository::MAX_DESCRIPTION_LEN + 1);
            let err = validate_create_input("my-repo", &long_desc, "main", "alice")
                .expect_err("should reject oversized description");
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert!(err.message().contains("description exceeds maximum length"));
        }

        #[test]
        fn rejects_oversized_branch_name() {
            let long_branch = "a".repeat(repository::MAX_NAME_LEN + 1);
            let err = validate_create_input("my-repo", "desc", &long_branch, "alice")
                .expect_err("should reject oversized branch name");
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert!(err.message().contains("Branch name exceeds maximum length"));
        }

        #[test]
        fn rejects_oversized_creator() {
            let long_creator = "a".repeat(repository::MAX_NAME_LEN + 1);
            let err = validate_create_input("my-repo", "desc", "main", &long_creator)
                .expect_err("should reject oversized creator");
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert!(err.message().contains("Creator exceeds maximum length"));
        }
    }
}
