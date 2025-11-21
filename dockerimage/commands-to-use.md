# To build:
- Download from github action, extract the zip, and load the tar with `docker load -i scouting.tar`
- Use `docker build -f dockerimage/dockerfile -t <imagename>:latest .` in the project root directory
# To volume:
- Make a volume via `docker volume create <volumename>`
# To run:
- `docker run --name <imagename> -p <localhost-port>:5000 --rm -v <volumename>:/app <imagename>:latest`
