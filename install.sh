echo "Pull Docker ELK"
docker pull sebp/elk
docker run -p 5601:5601 -p 9200:9200 -p 5044:5044 -it --name elk sebp/elk