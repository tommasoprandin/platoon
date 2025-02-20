use openraft::{
    error::{CheckIsLeaderError, ClientWriteError, RaftError},
    OptionalSend, OptionalSync, Raft,
};
use tokio::task::JoinHandle;
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
    port: u16,
}

impl<SE> PlatoonServer<SE>
where
    SE: StorageEngine + OptionalSend + OptionalSync + Clone + 'static,
{
    pub fn new(raft: Raft<TypeConfig>, state_machine: StateMachine<SE>, port: u16) -> Self {
        Self {
            raft,
            state_machine,
            port,
        }
    }

    pub fn start(self) -> JoinHandle<Result<(), Error>> {
        tokio::task::spawn(async move {
            let address = format!("0.0.0.0:{}", self.port);
            let reflection_service = ReflectionBuilder::configure()
                .register_encoded_file_descriptor_set(APP_FILE_DESCRIPTOR)
                .build_v1()
                .expect("Failed to build reflection service");

            tonic::transport::Server::builder()
                .add_service(PlatoonServiceServer::new(self))
                .add_service(reflection_service)
                .serve(address.parse().expect("Invalid address"))
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
                                "http://{}:{}",
                                &leader.addr.split_once(":").expect("Wrong address format").0,
                                self.port
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

                            let leader_addr = format!(
                                "http://{}:{}",
                                &leader.addr.split_once(":").expect("Wrong address format").0,
                                self.port
                            );
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
                        tracing::error!(
                            message = "Fatal error in delete_vehicle",
                            error = err.to_string()
                        );
                        Err(tonic::Status::internal(format!(
                            "Fatal internal error: {}",
                            err.to_string().as_str()
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
        let res = self.raft.ensure_linearizable().await;

        match res {
            Ok(_) => {
                let vehicles = self
                    .state_machine
                    .get_state()
                    .await
                    .into_iter()
                    .map(|v| v.into())
                    .collect();

                tracing::info!(message = "Vehicles read successfully", ?vehicles);
                Ok(tonic::Response::new(GetPlatoonResponse { vehicles }))
            }
            Err(err) => match err {
                RaftError::APIError(err) => match err {
                    CheckIsLeaderError::ForwardToLeader(forward_to_leader) => {
                        // Here we try to forward to the leader
                        let leader =
                            forward_to_leader
                                .leader_node
                                .ok_or(tonic::Status::unavailable(
                                    "Leader is currently unavailable",
                                ))?;

                        let leader_addr = format!(
                            "http://{}:{}",
                            &leader.addr.split_once(":").expect("Wrong address format").0,
                            self.port
                        );
                        tracing::info!(address = leader_addr, "Forwarding request to leader");
                        let mut client = PlatoonServiceClient::connect(leader_addr).await.unwrap();

                        client.get_platoon(tonic::Request::new(())).await
                    }
                    CheckIsLeaderError::QuorumNotEnough(quorum_not_enough) => {
                        tracing::warn!(
                            message = "Quorum not enough, cluster may be temporarily down",
                            error = quorum_not_enough.to_string()
                        );
                        Err(tonic::Status::unavailable(format!(
                            "Not enough nodes responding for quorum, cluster may be down: {}",
                            quorum_not_enough.to_string().as_str()
                        )))
                    }
                },
                RaftError::Fatal(fatal) => {
                    tracing::error!(
                        message = "Fatal error in get_platoon",
                        error = fatal.to_string()
                    );
                    Err(tonic::Status::internal(format!(
                        "Fatal internal error: {}",
                        fatal.to_string().as_str()
                    )))
                }
            },
        }
    }
}
