use core::panic;
use std::time::Duration;

use lazy_static::lazy_static;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use rand_distr::Normal;
use tokio::task::JoinHandle;

use crate::{
    grpc::app::{platoon_service_client::PlatoonServiceClient, UpdateVehicleRequest},
    types::Vehicle,
};

#[derive(Debug, Clone, Copy)]
enum Action {
    Straight = 0,
    TurnLeft = 1,
    TurnRight = 2,
}

impl From<usize> for Action {
    fn from(value: usize) -> Self {
        match value {
            0 => Action::Straight,
            1 => Action::TurnLeft,
            2 => Action::TurnRight,
            _ => Action::Straight,
        }
    }
}

impl Action {
    fn weight(&self) -> f32 {
        match self {
            Action::Straight => 0.8,
            Action::TurnLeft => 0.1,
            Action::TurnRight => 0.1,
        }
    }

    fn heading(&self) -> f32 {
        match self {
            Action::Straight => 0.0,
            Action::TurnLeft => -90.0,
            Action::TurnRight => 90.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PlatoonClient {
    id: String,
    port: u16,
    rng: SmallRng,
    read_period: Duration,
    write_period: Duration,
}

impl PlatoonClient {
    pub fn new(
        id: String,
        port: u16,
        seed: u64,
        read_period: Duration,
        write_period: Duration,
    ) -> Self {
        Self {
            id,
            port,
            rng: SmallRng::seed_from_u64(seed),
            read_period,
            write_period,
        }
    }

    fn update_vehicle(&mut self, vehicle: &mut Vehicle) {
        lazy_static! {
            static ref ACTIONS_SAMPLE: Vec<Action> = {
                let mut actions = vec![];
                let samples = 100.0;

                for _ in 0..((samples * Action::Straight.weight()) as usize) {
                    actions.push(Action::Straight);
                }
                for _ in 0..((samples * Action::TurnRight.weight()) as usize) {
                    actions.push(Action::TurnRight);
                }
                for _ in 0..((samples * Action::TurnLeft.weight()) as usize) {
                    actions.push(Action::TurnLeft);
                }

                actions
            };
        }
        let action: Action = ACTIONS_SAMPLE
            .get(self.rng.random_range(0..100))
            .map(|elt| elt.clone())
            .unwrap_or(Action::Straight);

        match action {
            Action::Straight => {
                vehicle.position.y += vehicle.speed.speed * self.write_period.as_secs_f32();
                vehicle.speed.heading = action.heading();
            }
            Action::TurnLeft => {
                vehicle.position.x -= 10.0;
                vehicle.speed.heading = action.heading();
            }
            Action::TurnRight => {
                vehicle.position.x += 10.0;
                vehicle.speed.heading = action.heading();
            }
        }

        let distr = Normal::new(30.0, 5.0);

        if let Ok(distr) = distr {
            vehicle.speed.speed = self.rng.sample(distr);
        }
    }

    pub fn start(self) -> (JoinHandle<()>, JoinHandle<()>) {
        let mut self_clone = self.clone();
        let Self {
            id,
            port,
            read_period,
            write_period,
            ..
        } = self;
        // Update task
        let write_jh = tokio::task::spawn(async move {
            let address = format!("http://localhost:{}", port);
            let client = PlatoonServiceClient::connect(address.clone()).await;

            if let Ok(mut client) = client {
                tracing::info!("Platoon write client connected!");
                let mut vehicle = Vehicle {
                    id: id.clone(),
                    ..Default::default()
                };
                loop {
                    tokio::time::sleep(write_period).await;
                    self_clone.update_vehicle(&mut vehicle);
                    let mut req = tonic::Request::new(UpdateVehicleRequest {
                        vehicle: vehicle.clone().into(),
                    });
                    req.set_timeout(Duration::from_millis(500));
                    tracing::info!(message = "Updating vehicle", ?vehicle);
                    let res = client.update_vehicle(req).await;
                    match res {
                        Ok(_) => {
                            tracing::info!(message = "Vehicle updated successfully!", ?vehicle);
                        }
                        Err(err) => {
                            tracing::error!(
                                message = "Error in platoon client write",
                                error = err.to_string()
                            );
                        }
                    }
                }
            } else {
                tracing::error!("Platoon client connection failed");
                panic!();
            }
        });

        // Read task
        let read_jh = tokio::task::spawn(async move {
            let address = format!("http://localhost:{}", port);
            let client = PlatoonServiceClient::connect(address.clone()).await;

            if let Ok(mut client) = client {
                tracing::info!("Platoon read client connected!");
                loop {
                    tokio::time::sleep(read_period).await;
                    let mut req = tonic::Request::new(());
                    req.set_timeout(Duration::from_millis(500));
                    tracing::info!(message = "Reading vehicles");
                    let res = client.get_platoon(req).await;
                    match res {
                        Ok(res) => {
                            let vehicles = res.into_inner().vehicles;
                            tracing::info!(message = "Vehicles read successfully!", ?vehicles);
                        }
                        Err(err) => {
                            tracing::error!(
                                message = "Error in platoon client write",
                                error = err.to_string()
                            );
                        }
                    }
                }
            } else {
                tracing::error!("Platoon client connection failed");
                panic!();
            }
        });

        (read_jh, write_jh)
    }
}
