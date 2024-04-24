FROM rust:1.77

# Copy the project files
COPY . .

# Build the project
RUN cargo build
