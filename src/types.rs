use serde::{Deserialize, Serialize};

pub type VehicleId = String;
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct VehiclePosition {
    pub x: f32,
    pub y: f32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct VehicleSpeed {
    pub speed: f32,
    pub heading: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Vehicle {
    pub id: VehicleId,
    pub position: VehiclePosition,
    pub speed: VehicleSpeed,
}
