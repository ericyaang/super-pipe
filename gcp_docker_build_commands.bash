docker build -t corner-etl/teste:latest .

# to check the files in the container
docker run --rm -it corner-etl/teste:latest

docker tag corner-etl/teste:latest southamerica-east1-docker.pkg.dev/cornershop-390320/cornershop/corner-etl/teste:latest

docker push southamerica-east1-docker.pkg.dev/cornershop-390320/cornershop/corner-etl/teste:latest