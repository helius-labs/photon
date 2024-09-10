FROM rust:1.81-slim-bullseye
RUN apt update && apt install -y build-essential pkg-config libssl-dev

# Copy the project files
COPY . .

# Build the project
RUN cargo build --release

RUN mv target/release/photon . 
RUN mv target/release/photon-snapshotter . 
RUN mv target/release/photon-migration . 

RUN rm -rf target

