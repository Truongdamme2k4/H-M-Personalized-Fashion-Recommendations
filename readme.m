docker swarm init --advertise-addr <IP_MAY_1>

docker swarm init --advertise-addr <IP_MAY_1>

docker node ls

#Overlay network
docker network create --driver overlay --attachable bigdata-net

docker stack deploy -c hdfs.yml hdfs


deploy:
  placement:
    constraints:
      - node.role == worker


docker exec -it zerotier-one zerotier-cli join <NETWORK_ID_CỦA_BẠN>
docker swarm init --advertise-addr 10.229.91.65
docker network create --driver overlay --attachable bigdata_network
docker swarm join --token SWMTKN-1-365xz1fpdo2tbjd24o4pew6tzgdik2bj5stlxcs7j1flo27948-4oqb15s70wd6x8dq6voykxq8x 10.229.91.65:2377