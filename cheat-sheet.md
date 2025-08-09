# Make sure .env has IMAGE_TAG=develop (or latest / sha-<commit>)
`docker compose pull`
`docker compose up -d`

# Runs GHCR images plus bind mounts so local code changes take effect immediately (optionally local builds if you uncomment them in docker-compose.dev.yml).
`docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d`
# If you want to rebuild locally for one or more services (after uncommenting build: for them):
`docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build`

1) Pin to a specific commit SHA
Edit .env:
__IMAGE_TAG=sha-f5f46bbce759c1de814b774449420e5bfb30ffc1__

`docker compose pull`
`docker compose up -d`

# This guarantees you’re running the exact build from that commit.