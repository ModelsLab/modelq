from typing import Optional

import redis
import json
import functools



class CeleryML :
    """"""

    def __init__(
            self,
            host : str = "localhost",
            username : str = None,
            port : str = 6379,
            db : int = 0  ,
            password : str = None,
            ssl : bool = False,
            ssl_cert_reqs : any = None,
            **kwargs
                 ):
        self.redis_client = self._connect_to_redis(
            host=host,
            port = port,
            db = db,
            password= password,
            username = username,
            ssh = ssl,
            ssl_cert_reqs= ssl_cert_reqs,
            **kwargs
        )
    
    def _connect_to_redis(
            self,host : str,port : str , db : int ,  password : str,ssl : bool , ssl_cert_reqs : any,username : str
    ) -> redis.Redis:
        if host == "localhost":
            connection = redis.Redis(host = "localhost",db = 3) 

        else :
            connection = redis.Redis(
                host=host,
                port=port,
                password=password,
                username=username,
                ssl=ssl,
                ssl_cert_reqs=ssl
            )

        return connection

            