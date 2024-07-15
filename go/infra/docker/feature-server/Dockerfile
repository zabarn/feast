FROM golang:1.22.5

# Update the package list and install the ca-certificates package
RUN apt-get update && apt-get install -y ca-certificates

# Set the current working directory inside the container
WORKDIR /app

# Copy the source code into the container
COPY go/ ./go/
COPY go.mod go.sum ./

# Build the Go application
RUN go build -o feast ./go/main.go

# Expose ports
EXPOSE 6556

# Command to run the executable
# Pass arguments to the executable (Ex: ./feast --type=grpc)
CMD ["./feast"]
