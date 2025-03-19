# sudo apt-get install -y pciutils
# curl https://ollama.ai/install.sh | sh
# sudo apt-get install liblzma-dev
sudo /etc/init.d/docker start
docker-compose build
docker-compose up -d
