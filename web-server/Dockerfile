# Start from the official Golang base image
FROM golang:1.23-alpine AS builder

# Install curl for downloading dependencies
RUN apk add --no-cache curl build-base git

# Set the working directory inside the container
WORKDIR /app

# Install golang-migrate
RUN curl -L https://github.com/golang-migrate/migrate/releases/download/v4.16.2/migrate.linux-amd64.tar.gz | tar xvz && \
    mv migrate /usr/local/bin/migrate && \
    chmod +x /usr/local/bin/migrate

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire project
COPY . .

# Build the Go application
RUN go build -o web-server ./cmd

# Use a minimal base image for the final image
FROM alpine:latest

# Set the working directory inside the container
WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/web-server .

# Copy the migrate binary from the builder stage
COPY --from=builder /usr/local/bin/migrate /usr/local/bin/migrate

# Expose the port on which the app runs
EXPOSE 3000

# Copy config files if needed
COPY config.yaml .

# Copy the database migration files
COPY db/migrations ./db/migrations

# Command to run the executable
CMD ./web-server migrate up && ./web-server serve