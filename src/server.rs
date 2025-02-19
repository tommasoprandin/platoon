use std::sync::Arc;

use openraft::{
    error::{ClientWriteError, RaftError},
    OptionalSend, OptionalSync, Raft,
};
use tokio::{sync::RwLock, task::JoinHandle};
use tonic::transport::Error;
use tonic_reflection::server::Builder as ReflectionBuilder;
use tracing::instrument;

use crate::{
    grpc::{
        app::{
            platoon_service_client::PlatoonServiceClient,
            platoon_service_server::{PlatoonService, PlatoonServiceServer},
            DeleteVehicleRequest, DeleteVehicleResponse, GetPlatoonResponse, UpdateVehicleRequest,
            UpdateVehicleResponse,
        },
        types::APP_FILE_DESCRIPTOR,
    },
    raft::{
        storage::{engine::StorageEngine, state_machine::StateMachine},
        types::{Request, TypeConfig},
    },
    types::{Vehicle, VehiclePosition, VehicleSpeed},
};

#[derive(Clone)]
pub struct PlatoonServer<SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync + Clone + 'static,
{
    raft: Raft<TypeConfig>,
    state_machine: StateMachine<SE>,
}

impl<SE> PlatoonServer<SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync + Clone + 'static,
{
    pub fn new(raft: Raft<TypeConfig>, state_machine: StateMachine<SE>) -> Self {
        Self {
            raft,
            state_machine,
        }
    }

    pub fn start(&self) -> JoinHandle<Result<(), Error>> {
        let raft_instance = self.raft.clone();
        let sm_instance = self.state_machine.clone();
        tokio::task::spawn(async move {
            static ADDRESS: &'static str = "0.0.0.0:8001";
            let app_service = PlatoonServer::new(raft_instance, sm_instance);
            let reflection_service = ReflectionBuilder::configure()
                .register_encoded_file_descriptor_set(APP_FILE_DESCRIPTOR)
                .build_v1()
                .expect("Failed to build reflection service");

            tonic::transport::Server::builder()
                .add_service(PlatoonServiceServer::new(app_service))
                .add_service(reflection_service)
                .serve(ADDRESS.parse().expect("Invalid address"))
                .await
        })
    }
}

#[tonic::async_trait]
impl<SE> PlatoonService for PlatoonServer<SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync + Clone + 'static,
{
    #[instrument(level = "debug", skip(self))]
    async fn update_vehicle(
        &self,
        request: tonic::Request<UpdateVehicleRequest>,
    ) -> Result<tonic::Response<UpdateVehicleResponse>, tonic::Status> {
        // Try to write on this node
        let vehicle = request.into_inner().vehicle;

        let res = self
            .raft
            .client_write(Request::Set(Vehicle {
                speed: VehicleSpeed {
                    speed: vehicle.speed,
                    heading: vehicle.heading,
                },
                position: VehiclePosition {
                    x: vehicle.x,
                    y: vehicle.y,
                },
                id: vehicle.id.clone(),
            }))
            .await;

        match res {
            // If everything went well we return immediately
            Ok(_) => {
                tracing::info!(message = "Vehicle updated correctly", vehicle = ?vehicle);
                Ok(tonic::Response::new(UpdateVehicleResponse {
                    result: crate::grpc::app::Result {
                        ok: true,
                        error: None,
                    },
                }))
            }
            // Otherwise we check if we need to forward the request to the leader
            Err(err) => {
                match err {
                    RaftError::APIError(err) => match err {
                        ClientWriteError::ForwardToLeader(forward_to_leader) => {
                            // Here we try to forward to the leader
                            let leader =
                                forward_to_leader
                                    .leader_node
                                    .ok_or(tonic::Status::unavailable(
                                        "Leader is currently unavailable",
                                    ))?;

                            let leader_addr = format!(
                                "http://{}:8001",
                                &leader.addr.split_once(":").expect("Wrong address format").0
                            );
                            tracing::info!(address = leader_addr, "Forwarding request to leader");
                            let mut client =
                                PlatoonServiceClient::connect(leader_addr).await.unwrap();

                            client
                                .update_vehicle(tonic::Request::new(UpdateVehicleRequest {
                                    vehicle: vehicle.into(),
                                }))
                                .await
                        }
                        ClientWriteError::ChangeMembershipError(change_membership_error) => {
                            tracing::error!(message="Change membership error", error =?change_membership_error);
                            Err(tonic::Status::internal(format!(
                                "Change membership error: {}",
                                change_membership_error.to_string()
                            )))
                        }
                    },
                    RaftError::Fatal(err) => {
                        let error_message = err.to_string();
                        tracing::error!(
                            message = "Fatal error in update_vehicle",
                            error = err.to_string()
                        );
                        Err(tonic::Status::internal(format!(
                            "Fatal internal error: {}",
                            error_message
                        )))
                    }
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_vehicle(
        &self,
        request: tonic::Request<DeleteVehicleRequest>,
    ) -> Result<tonic::Response<DeleteVehicleResponse>, tonic::Status> {
        // Try to write on this node
        let vehicle_id = request.into_inner().vehicle_id;

        let res = self
            .raft
            .client_write(Request::Delete(vehicle_id.clone()))
            .await;

        match res {
            // If everything went well we return immediately
            Ok(_) => {
                tracing::info!(message = "Vehicle deleted correctly", vehicle_id);
                Ok(tonic::Response::new(DeleteVehicleResponse {
                    result: crate::grpc::app::Result {
                        ok: true,
                        error: None,
                    },
                }))
            }
            // Otherwise we check if we need to forward the request to the leader
            Err(err) => {
                match err {
                    RaftError::APIError(err) => match err {
                        ClientWriteError::ForwardToLeader(forward_to_leader) => {
                            // Here we try to forward to the leader
                            let leader =
                                forward_to_leader
                                    .leader_node
                                    .ok_or(tonic::Status::unavailable(
                                        "Leader is currently unavailable",
                                    ))?;

                            let leader_addr = format!("http://{}", &leader.addr);
                            tracing::info!(address = leader_addr, "Forwarding request to leader");
                            let mut client =
                                PlatoonServiceClient::connect(leader_addr).await.unwrap();

                            client
                                .delete_vehicle(tonic::Request::new(DeleteVehicleRequest {
                                    vehicle_id,
                                }))
                                .await
                        }
                        ClientWriteError::ChangeMembershipError(change_membership_error) => {
                            tracing::error!(error = ?change_membership_error);
                            Err(tonic::Status::internal(format!(
                                "Change membership error: {}",
                                change_membership_error.to_string()
                            )))
                        }
                    },
                    RaftError::Fatal(err) => {
                        let error_message = err.to_string();
                        tracing::error!(
                            message = "Fatal error in delete_vehicle",
                            error = err.to_string()
                        );
                        Err(tonic::Status::internal(format!(
                            "Fatal internal error: {}",
                            error_message
                        )))
                    }
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_platoon(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<tonic::Response<GetPlatoonResponse>, tonic::Status> {
        let vehicles = self.state_machine.get_state().await;
        tracing::info!(message = "Fetch vehicles", ?vehicles);
        Ok(tonic::Response::new(GetPlatoonResponse {
            vehicles: vehicles.iter().map(|v| v.clone().into()).collect(),
        }))
    }
}
