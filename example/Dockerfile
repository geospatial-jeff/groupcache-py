FROM --platform=linux/amd64 python:3.11-slim

WORKDIR /app

# Install uv for faster package management
RUN pip install uv

# Copy uv dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

# Copy the groupcache package
COPY groupcache ./groupcache

# Copy the example application
COPY example/app.py .

# Expose the port
EXPOSE 8080

# Run the application
CMD ["uv", "run", "python", "app.py"]