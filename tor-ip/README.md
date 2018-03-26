# tor ip list
https://check.torproject.org/exit-addresses


# proxy 1
curl -s "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list.txt" | sed '1,2d; $d; s/\s.*//; /^$/d' > proxy-list.txt

#proxy 2
source env/bin/activate
pip install -U git+https://github.com/constverum/ProxyBroker.git
