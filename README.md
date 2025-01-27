# multi
Fires most common operations in parallel utilising task multiplexing on I/O wait for better performance

# Example Usage 


   
    $ time python2.7 mssh.py  -p 50    exec -s server1-027.jun,server2-028.jun -c 'uptime' --combine
