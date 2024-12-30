import logging
import urllib.parse
import hashlib
import json
import subprocess
import sys
import requests
import json
import urllib
import aiohttp
import json
from fyers_apiv3.fyers_logger import FyersLogger


class Config:

    #URL's
    API = 'https://api-t1.fyers.in/api/v3'
    DATA_API = "https://api-t1.fyers.in/data"

    # Endpoint
    get_profile = "/profile"
    tradebook = "/tradebook"
    positions = "/positions"
    holdings = "/holdings"
    convert_position = "/positions"
    funds = "/funds"
    orders_endpoint = "/orders/sync"
    orderbook = "/orders"
    market_status = "/marketStatus"
    auth = "/generate-authcode"
    generate_access_token = "/validate-authcode"
    generate_data_token = "/data-token"
    data_vendor_td = "/truedata-ws"
    multi_orders = "/multi-order/sync"
    history = "/history"
    quotes = "/quotes"
    market_depth = "/depth"
    option_chain = "/options-chain-v3"
    multileg_orders = "/multileg/orders/sync"



class FyersServiceSync:
    def __init__(self, logger,request_logger):
        """
        Initializes an instance of FyersServiceSync.

        Args:
            logger: The logger object used for logging errors.
        """
        self.api_logger = logger
        self.request_logger = request_logger
        self.content = "application/json"
        self.error_resp = {"s":"error", "code": 0 , "message":"Bad request"}
        self.error_message = "invalid input please check your input"


    def post_call(self, api: str, header: str, data=None) -> dict:
        """
        Makes a POST request to the specified API.

        Args:
            api: The API endpoint to make the request to.
            header: The authorization header for the request.
            data: The data to send in the request payload.

        Returns:
            The response JSON as a dictionary, or the response object if an error occurs.
        """
        try:
            URL = Config.API + api 
            response = requests.post(
                URL,
                data=json.dumps(data),
                headers={"Authorization": header, "Content-Type": self.content ,"version": "3"},
            )
            self.request_logger.debug({"Status Code":response.status_code, "API":api  })
            self.api_logger.debug({"URL": URL, "post data": json.dumps(data), \
                                   "Response Status Code": response.status_code, \
                                    "Response": response.json()})
            response.raise_for_status()
            return response.json()
        
        except requests.HTTPError as e:
            self.api_logger.error({"API":api, "Error": response.json()})
            return response.json()
            
        except TypeError as e:
            if "response" in locals():
                self.error_resp["code"] = response.status_code
            else:
                self.error_resp["code"] = -99
                self.error_resp["message"]=self.error_message
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"data": json.dumps(data),"message": self.error_resp})
            return self.error_resp 
                 
        except Exception as e:
            if "response" in locals():
                self.error_resp["code"] = response.status_code
            else:
                self.error_resp["code"] = -99
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"data": json.dumps(data),"message": e})
            return self.error_resp

    def get_call(self, api: str, header: str, data=None, data_flag=False) -> dict:
        """
        Makes a GET request to the specified API.

        Args:
            api: The API endpoint to make the request to.
            header: The authorization header for the request.
            data: The data to send in the request query parameters.
            data_flag: A flag indicating whether to use custom data URLs.

        Returns:
            The response JSON as a dictionary, or the response object if an error occurs.
        """
        try:
            if data_flag:
                URL = Config.DATA_API + api
            else:
                URL = Config.API + api  
            if data is not None:
                url_params = urllib.parse.urlencode(data)
                URL = URL + "?" + url_params   
            response = requests.get(
                url=URL,
                headers={
                    "Authorization": header,
                    "Content-Type": self.content,
                    "version": "3"
                },
            )
            self.request_logger.debug({"Status Code":response.status_code, "API":api  })
            self.api_logger.debug({"URL": URL, "Status Code": response.status_code, "Response": response.json()})
            response.raise_for_status()
            return response.json()

        except requests.HTTPError as e:
            self.api_logger.error({"API":api, "Error": response.json()})
            return response.json()
   
        except TypeError as e:
            if "response" in locals():
                self.error_resp["code"] = response.status_code
            else:
                self.error_resp["code"] = -99
                self.error_resp["message"]=self.error_message
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"message": self.error_resp})
            return self.error_resp 
                 
        except Exception as e:
            if "response" in locals():
                self.error_resp["code"] = response.status_code
            else:
                self.error_resp["code"] = -99
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"message": e})
            return self.error_resp
        
    def delete_call(self, api: str, header: str, data) -> dict:
        """
        Makes a DELETE request to the specified API.

        Args:
            api: The API endpoint to make the request to.
            header: The authorization header for the request.
            data: The data to send in the request payload.

        Returns:
            The response JSON as a dictionary, or the response object if an error occurs.
        """
        try:
            URL = Config.API + api
            response = requests.delete(
                URL,
                data=json.dumps(data),
                headers={"Authorization": header, "Content-Type": self.content ,"version": "3"},
            )
            self.request_logger.debug({"Status Code":response.status_code, "API":api  })
            self.api_logger.debug({"URL": URL, "data": json.dumps(data), \
                                   "Response Status Code": response.status_code,\
                                   "Response": response.json()})
            response.raise_for_status()
            return response.json()

        except requests.HTTPError as e:
            self.api_logger.error({"API":api, "Error": response.json()})
            return response.json()
        
        except TypeError as e:
            if "response" in locals():
                self.error_resp["code"] = response.status_code
            else:
                self.error_resp["code"] = -99
                self.error_resp["message"]=self.error_message
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"data": json.dumps(data),"message": self.error_resp})
            return self.error_resp 
                 
        except Exception as e:
            if "response" in locals():
                self.error_resp["code"] = response.status_code
            else:
                self.error_resp["code"] = -99
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"data": json.dumps(data),"message": e})
            return self.error_resp
        

    def patch_call(self, api: str, header: str, data) -> dict:
        """
        Makes a PATCH request to the specified API.

        Args:
            api: The API endpoint to make the request to.
            header: The authorization header for the request.
            data: The data to send in the request payload.

        Returns:
            The response JSON as a dictionary, or the response object if an error occurs.
        """
        try:
            URL = Config.API + api
            response = requests.patch(
                URL,
                data=json.dumps(data),
                headers={"Authorization": header, "Content-Type": self.content ,"version": "3"},
            )
            self.request_logger.debug({"Status Code":response.status_code, "API":api  })
            self.api_logger.debug({"URL": URL, "data": json.dumps(data), \
                                   "Response Status Code": response.status_code, \
                                   "Response": response.json()})
            response.raise_for_status()            
            return response.json()

        except requests.HTTPError as e:
            self.api_logger.error({"API":api, "Error": response.json()})
            return response.json()
        
        except TypeError as e:
            if "response" in locals():
                self.error_resp["code"] = response.status_code
            else:
                self.error_resp["code"] = -99
                self.error_resp["message"]=self.error_message
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"data": json.dumps(data),"message": self.error_resp})
            return self.error_resp 
                 
        except Exception as e:
            if "response" in locals():
                self.error_resp["code"] = response.status_code
            else:
                self.error_resp["code"] = -99
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"data": json.dumps(data),"message": e})
            return self.error_resp

class FyersServiceAsync:
    def __init__(self, logger, request_logger):
        """
        Initializes an instance of FyersServiceAsync.

        Args:
            logger: The logger object used for logging errors.
        """
        self.api_logger = logger
        self.request_logger = request_logger
        self.content = "application/json"
        self.error_resp = {"s":"error", "code": 0 , "message":"Bad request"}
        self.error_message = "invalid input please check your input"


    async def post_async_call(self, api: str, header: str, data=None) -> dict:
        """
        Makes an asynchronous POST request to the specified API.

        Args:
            api: The API endpoint to make the request to.
            header: The authorization header for the request.
            data: The data to send in the request payload.

        Returns:
            The response JSON as a dictionary, or the response object if an error occurs.
        """
        try:
            url = Config.API + api
            async with aiohttp.ClientSession(
                headers={"Authorization": header, "Content-Type": self.content ,"version": "3"}
            ) as session:
                async with session.post(url, data=json.dumps(data)) as response:
                    self.request_logger.debug({"Status Code":response.status, "API":api  })
                    content = await response.read()
                    self.api_logger.debug({"URL": url,"Post Data": json.dumps(data), \
                                           "Response Status Code": response.status, \
                                            "Response": json.loads(content)})
                    response.raise_for_status()
                    response = await response.json()
                    return response

        except aiohttp.ClientResponseError as e:
            self.api_logger.error({"Api": api, "Response": json.loads(content)})
            return await response.json()
                
        except TypeError as e:
            if "response" in locals():
                self.error_resp["code"] = response.status
            else:
                self.error_resp["code"] = -99
                self.error_resp["message"]=self.error_message
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": url,"Post Data": json.dumps(data),"message": self.error_resp})
            return self.error_resp 
                 
        except Exception as e:
            if "response" in locals():
                self.error_resp["code"] = response.status
            else:
                self.error_resp["code"] = -99
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": url,"Post Data": json.dumps(data),"message": e})
            return self.error_resp

    async def get_async_call(
        self, api: str, header: str, params=None, data_flag=False
    ) -> dict:
        """
        Makes an asynchronous GET request to the specified API.

        Args:
            api: The API endpoint to make the request to.
            header: The authorization header for the request.
            params: The query parameters to send with the request.
            data_flag: A flag indicating whether to use custom data URLs.

        Returns:
            The response JSON as a dictionary, or the response object if an error occurs.
        """
        try:
            if data_flag:
                URL = Config.DATA_API + api
            else:
                URL = Config.API + api
            async with aiohttp.ClientSession(
                headers={
                    "Authorization": header,
                    "Content-Type": self.content,
                    "version": "3",
                }
            ) as session:
                async with session.get(URL, params=params) as response:
                    self.request_logger.debug({"Status Code": response.status, "API":api })
                    content = await response.read()
                    self.api_logger.debug({"URL": URL, "Status Code": response.status, "Response": json.loads(content)})
                    response.raise_for_status()
                    response = await response.json()
                    return response               

        except aiohttp.ClientResponseError as e:
            self.api_logger.error({"Api": api, "Response": json.loads(content)})
            return await response.json() 

        except TypeError as e:
            if "response" in locals():
                self.error_resp["code"] = response.status
            else:
                self.error_resp["code"] = -99
                self.error_resp["message"]= self.error_message
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"message": self.error_resp})
            return self.error_resp

        except Exception as e:
            if "response" in locals():
                self.error_resp["code"] = response.status
            else:
                self.error_resp["code"] = -99
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": URL,"message": e})
            return self.error_resp

    async def delete_async_call(self, api: str, header: str, data) -> dict:
        """
        Makes an asynchronous DELETE request to the specified API.

        Args:
            api: The API endpoint to make the request to.
            header: The authorization header for the request.
            data: The data to send in the request payload.

        Returns:
            The response JSON as a dictionary, or the response object if an error occurs.
        """
        try:
            url = Config.API + api
            async with aiohttp.ClientSession(
                headers={"Authorization": header, "Content-Type": self.content ,"version": "3"}
            ) as session:
                async with session.delete(url, data=json.dumps(data)) as response:
                    self.request_logger.debug({"Status Code": response.status, "API":api })
                    content = await response.read()
                    self.api_logger.debug({"URL": url, "data": json.dumps(data), \
                                            "Response Status Code": response.status, \
                                            "Response": json.loads(content)})
                    response.raise_for_status()
                    response = await response.json()          
                    return response

        except aiohttp.ClientResponseError as e:
            self.api_logger.error({"Api": api, "Response": json.loads(content)})
            return await response.json()

        except TypeError as e:
            if "response" in locals():
                self.error_resp["code"] = response.status
            else:
                self.error_resp["code"] = -99
                self.error_resp["message"]=self.error_message
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": url,"data": json.dumps(data),"message": self.error_resp})
            return self.error_resp 
                 
        except Exception as e:
            if "response" in locals():
                self.error_resp["code"] = response.status
            else:
                self.error_resp["code"] = -99
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": url,"data": json.dumps(data),"message": e})
            return self.error_resp

    async def patch_async_call(self, api: str, header: str, data) -> dict:
        """
        Makes an asynchronous PATCH request to the specified API.

        Args:
            api: The API endpoint to make the request to.
            header: The authorization header for the request.
            data: The data to send in the request payload.

        Returns:
            The response JSON as a dictionary, or the response object if an error occurs.
        """
        try:
            url = Config.API + api
            async with aiohttp.ClientSession(
                headers={"Authorization": header, "Content-Type": self.content ,"version": "3"}
            ) as session:
                json_data = json.dumps(data).encode("utf-8")

                async with session.patch(url, data=json_data) as response:
                    self.request_logger.debug({"Status Code": response.status, "API":api })
                    content = await response.read()
                    self.api_logger.debug({"URL": url, "data": json.dumps(data), \
                                          "Status Code": response.status, \
                                          "Response": json.loads(content)})
                    response.raise_for_status()
                    response = await response.json()           
                    return response 
                
        except aiohttp.ClientResponseError as e:
            self.api_logger.error({"Api": api, "Response": json.loads(content)})
            return await response.json()
        
        except TypeError as e:
            if "response" in locals():
                self.error_resp["code"] = response.status
            else:
                self.error_resp["code"] = -99
                self.error_resp["message"]=self.error_message
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": url,"data": json.dumps(data),"message": self.error_resp})
            return self.error_resp 
                 
        except Exception as e:
            if "response" in locals():
                self.error_resp["code"] = response.status
            else:
                self.error_resp["code"] = -99
            self.api_logger.error({"API": api, "error": e})
            self.api_logger.debug({"URL": url,"data": json.dumps(data),"message": e})
            return self.error_resp



class SessionModel:
    def __init__(
        self,
        client_id=None,
        redirect_uri=None,
        response_type=None,
        scope=None,
        state=None,
        nonce=None,
        secret_key=None,
        grant_type=None,
    ):
        self.client_id = client_id
        self.redirect_uri = redirect_uri
        self.response_type = response_type
        self.scope = scope
        self.state = state
        self.nonce = nonce
        self.secret_key = secret_key
        self.grant_type = grant_type

    def generate_authcode(self):
        data = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": self.response_type,
            "state": self.state,
        }
        if self.scope is not None:
            data["scope"] = self.scope
        if self.nonce is not None:
            data["nonce"] = self.nonce

        url_params = urllib.parse.urlencode(data)
        return f"{Config.API}{Config.auth}?{url_params}"

    def get_hash(self):
        hash_val = hashlib.sha256(f"{self.client_id}:{self.secret_key}".encode())
        return hash_val

    def set_token(self, token):
        self.auth_token = token

    def generate_token(self):
        data = {
            "grant_type": self.grant_type,
            "appIdHash": self.get_hash().hexdigest(),
            "code": self.auth_token,
        }
        response = requests.post(
            Config.API + Config.generate_access_token, headers="", json=data
        )
        return response.json()


class FyersModel:
    def __init__(
        self,
        is_async: bool = False,
        log_path=None,
        client_id: str = "",
        token: str = "",
        log_level: str = "ERROR"
    ):
        """
        Initializes an instance of FyersModelv3.

        Args:
            is_async: A boolean indicating whether API calls should be made asynchronously.
            client_id: The client ID for API authentication.
            token: The token for API authentication.
        """
        self.client_id = client_id
        self.token = token
        self.is_async = is_async
        self.log_path = log_path
        self.header = "{}:{}".format(self.client_id, self.token)
        self.log_level = log_level
        if log_path:
            self.log_path = log_path + "/"
        else:
            self.log_path = ""

        self.api_logger = FyersLogger(
            "FyersAPI",
            log_level,
            stack_level=2,
            logger_handler=logging.FileHandler(self.log_path + "fyersApi.log"),
        )

        self.request_logger = FyersLogger(
            "FyersAPIRequest",
            "DEBUG",
            stack_level=2,
            logger_handler=logging.FileHandler(self.log_path + "fyersRequests.log"),
        )
        if is_async:
            self.service = FyersServiceAsync(self.api_logger, self.request_logger)
        else:
            self.service = FyersServiceSync(self.api_logger,self.request_logger)

    def get_profile(self) -> dict:
        """
        Retrieves the user profile information.

        """
        if self.is_async:
            response = self.service.get_async_call(Config.get_profile, self.header)
            
        else:
            response = self.service.get_call(Config.get_profile, self.header)
        return response

    def tradebook(self) -> dict:
        """
        Retrieves daily trade details of the day.

        """
        if self.is_async:
            response = self.service.get_async_call(Config.tradebook, self.header)
            
        else:
            response = self.service.get_call(Config.tradebook, self.header)
        return response

    def funds(self) -> dict:
        """
        Retrieves funds details.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.get_async_call(Config.funds, self.header)
            
        else:
            response = self.service.get_call(Config.funds, self.header)
        return response

    def positions(self) -> dict:
        """
        Retrieves information about current open positions.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.get_async_call(Config.positions, self.header)
            
        else:
            response = self.service.get_call(Config.positions, self.header)
        return response

    def holdings(self) -> dict:
        """
        Retrieves information about current holdings.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.get_async_call(Config.holdings, self.header)
            
        else:
            response = self.service.get_call(Config.holdings, self.header)
        return response

    def get_orders(self, data) -> dict:
        """
        Retrieves order details by ID.

        Args:
            data: The data containing the order ID.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.get_async_call(Config.orderbook, self.header)
            
        else:
            response = self.service.get_call(Config.orderbook, self.header)
        id_list = data['id'].split(",")
        response["orderBook"]= [order for order in response["orderBook"] if order["id"] in id_list]

        return response

    def orderbook(self, data = None) -> dict:
        """
        Retrieves the order information.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.get_async_call(Config.orderbook, self.header, data)
            
        else:
            response = self.service.get_call(Config.orderbook, self.header, data)
        return response

    def market_status(self) -> dict:
        """
        Retrieves market status.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.get_async_call(
                    Config.market_status, self.header, data_flag=True
                )
            
        else:
            response = self.service.get_call(
                Config.market_status, self.header, data_flag=True
            )
        return response

    def convert_position(self, data) -> dict:
        """
        Converts positions from one product type to another based on the provided details.

        Args:
            symbol (str): Symbol of the positions. Eg: "MCX:SILVERMIC20NOVFUT".
            positionSide (int): Side of the positions. 1 for open long positions, -1 for open short positions.
            convertQty (int): Quantity to be converted. Should be in multiples of lot size for derivatives.
            convertFrom (str): Existing product type of the positions. (CNC positions cannot be converted)
            convertTo (str): The new product type to convert the positions to.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.post_async_call(
                    Config.convert_position, self.header, data
                )
            
        else:
            response = self.service.post_call(
                Config.convert_position, self.header, data
            )
        return response

    def cancel_order(self, data) -> dict:
        """
        Cancel order.

        Args:
            id (str, optional): ID of the position to close. If not provided, all open positions will be closed.


        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.delete_async_call(Config.orders_endpoint, self.header, data)
            
        else:
            response = self.service.delete_call(Config.orders_endpoint, self.header, data)
        return response

    def place_order(self, data) -> dict:
        """
        Places an order based on the provided data.

        Args:
        data (dict): A dictionary containing the order details.
            - 'productType' (str): Type of the product. Possible values: 'CNC', 'INTRADAY', 'MARGIN', 'CO', 'BO'.
            - 'side' (int): Side of the order. 1 for Buy, -1 for Sell.
            - 'symbol' (str): Symbol of the product. Eg: 'NSE:SBIN-EQ'.
            - 'qty' (int): Quantity of the product. Should be in multiples of lot size for derivatives.
            - 'disclosedQty' (int): Disclosed quantity. Allowed only for equity. Default: 0.
            - 'type' (int): Type of the order. 1 for Limit Order, 2 for Market Order,
                            3 for Stop Order (SL-M), 4 for Stoplimit Order (SL-L).
            - 'validity' (str): Validity of the order. Possible values: 'IOC' (Immediate or Cancel), 'DAY' (Valid till the end of the day).
            - 'filledQty' (int): Filled quantity. Default: 0.
            - 'limitPrice' (float): Valid price for Limit and Stoplimit orders. Default: 0.
            - 'stopPrice' (float): Valid price for Stop and Stoplimit orders. Default: 0.
            - 'offlineOrder' (bool): Specifies if the order is placed when the market is open (False) or as an AMO order (True).

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.post_async_call(Config.orders_endpoint, self.header, data)
        else:
            response = self.service.post_call(Config.orders_endpoint, self.header, data)
        return response

    def modify_order(self, data) -> dict:
        """
        Modifies the parameters of a pending order based on the provided details.

        Parameters:
            id (str): ID of the pending order to be modified.
            limitPrice (float, optional): New limit price for the order. Mandatory for Limit/Stoplimit orders.
            stopPrice (float, optional): New stop price for the order. Mandatory for Stop/Stoplimit orders.
            qty (int, optional): New quantity for the order.
            type (int, optional): New order type for the order.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.patch_async_call(Config.orders_endpoint, self.header, data)
            
        else:
            response = self.service.patch_call(Config.orders_endpoint, self.header, data)
        return response

    def exit_positions(self, data={}) -> dict:
        """
        Closes open positions based on the provided ID or closes all open positions if ID is not passed.

        Args:
            id (str, optional): ID of the position to close. If not provided, all open positions will be closed.


        Returns:
            The response JSON as a dictionary.
        """
        if len(data) == 0 :
            data = {"exit_all": 1}

        if self.is_async:
            response = self.service.delete_async_call(Config.positions, self.header, data)
            
        else:
            response = self.service.delete_call(Config.positions, self.header, data)
        return response
    
    
    def place_multileg_order(self, data) -> dict:
        """
        Places an multileg order based on the provided data.

        Args:
        data (dict): A dictionary containing the order details.
            - 'productType' (str): Type of the product. Possible values: 'INTRADAY', 'MARGIN'.
            - 'offlineOrder' (bool): Specifies if the order is placed when the market is open (False) or as an AMO order (True).
            - 'orderType' (str): Type of multileg. Possible values: '3L' for 3 legs and '2L' for 2 legs .
            - 'validity' (str): Validity of the order. Possible values: 'IOC' (Immediate or Cancel).
            legs (dict): A dictionary containing multiple legs order details.
                - 'symbol' (str): Symbol of the product. Eg: 'NSE:SBIN-EQ'.
                - 'qty' (int): Quantity of the product. Should be in multiples of lot size for derivatives.
                - 'side' (int): Side of the order. 1 for Buy, -1 for Sell.
                - 'type' (int): Type of the order. Possible values: 1 for Limit Order.
                - 'limitPrice' (float): Valid price for Limit and Stoplimit orders.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.post_async_call(Config.multileg_orders, self.header, data)
        else:
            response = self.service.post_call(Config.multileg_orders, self.header, data)
        return response

    def generate_data_token(self, data):
        allPackages = subprocess.check_output([sys.executable, "-m", "pip", "freeze"])
        installed_packages = [r.decode().split("==")[0] for r in allPackages.split()]
        if Config.data_vendor_td not in installed_packages:
            print("Please install truedata package | pip install truedata-ws")
        response = self.service.post_call(Config.generate_data_token, self.header, data)
        return response

    def cancel_basket_orders(self, data):
        """
        Cancels the orders with the provided IDs.

        Parameters:
            order_ids (list): A list of order IDs to be cancelled.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.delete_async_call(
                    Config.multi_orders, self.header, data
                )
            
        else:
            response = self.service.delete_call(
                Config.multi_orders, self.header, data
            )
        return response

    def place_basket_orders(self, data):
        """
        Places multiple orders based on the provided details.

        Parameters:
        orders (list): A list of dictionaries containing the order details.
            Each dictionary should have the following keys:
            - 'symbol' (str): Symbol of the product. Eg: 'MCX:SILVERM20NOVFUT'.
            - 'qty' (int): Quantity of the product.
            - 'type' (int): Type of the order. 1 for Limit Order, 2 for Market Order, and so on.
            - 'side' (int): Side of the order. 1 for Buy, -1 for Sell.
            - 'productType' (str): Type of the product. Eg: 'INTRADAY', 'CNC', etc.
            - 'limitPrice' (float): Valid price for Limit and Stoplimit orders.
            - 'stopPrice' (float): Valid price for Stop and Stoplimit orders.
            - 'disclosedQty' (int): Disclosed quantity. Allowed only for equity.
            - 'validity' (str): Validity of the order. Eg: 'DAY', 'IOC', etc.
            - 'offlineOrder' (bool): Specifies if the order is placed when the market is open (False) or as an AMO order (True).
            - 'stopLoss' (float): Valid price for CO and BO orders.
            - 'takeProfit' (float): Valid price for BO orders.


        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.post_async_call(Config.multi_orders, self.header, data)
            
        else:
            response = self.service.post_call(
                Config.multi_orders, self.header, data
            )
        return response

    def modify_basket_orders(self, data):
        """
        Modifies multiple pending orders based on the provided details.

        Parameters:
        orders (list): A list of dictionaries containing the order details to be modified.
            Each dictionary should have the following keys:
            - 'id' (str): ID of the pending order to be modified.
            - 'limitPrice' (float): New limit price for the order. Mandatory for Limit/Stoplimit orders.
            - 'stopPrice' (float): New stop price for the order. Mandatory for Stop/Stoplimit orders.
            - 'qty' (int): New quantity for the order.
            - 'type' (int): New order type for the order.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.patch_async_call(
                    Config.multi_orders, self.header, data
                )
            
        else:
            response = self.service.patch_call(
                Config.multi_orders, self.header, data
            )
        return response

    def history(self, data=None):
        """
        Fetches candle data based on the provided parameters.

        Parameters:
        symbol (str): Symbol of the product. Eg: 'NSE:SBIN-EQ'.
        resolution (str): The candle resolution. Possible values are:
            'Day' or '1D', '1', '2', '3', '5', '10', '15', '20', '30', '60', '120', '240'.
        date_format (int): Date format flag. 0 to enter the epoch value, 1 to enter the date format as 'yyyy-mm-dd'.
        range_from (str): Start date of the records. Accepts epoch value if date_format flag is set to 0,
            or 'yyyy-mm-dd' format if date_format flag is set to 1.
        range_to (str): End date of the records. Accepts epoch value if date_format flag is set to 0,
            or 'yyyy-mm-dd' format if date_format flag is set to 1.
        cont_flag (int): Flag indicating continuous data and future options. Set to 1 for continuous data.


        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            response = self.service.get_async_call(
                    Config.history, self.header, data, data_flag=True
                )
            
        else:
            response = self.service.get_call(
                Config.history, self.header, data, data_flag=True
            )
        return response

    def quotes(self, data=None):
        """
        Fetches quotes data for multiple symbols.

        Parameters:
            symbols (str): Comma-separated symbols of the products. Maximum symbol limit is 50. Eg: 'NSE:SBIN-EQ,NSE:HDFC-EQ'.


        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
            
            response =   self.service.get_async_call(
                    Config.quotes, self.header, data, data_flag=True
                )
            
        else:
            response = self.service.get_call(
                Config.quotes, self.header, data, data_flag=True
            )
        return response

    def depth(self, data=None):
        """
        Fetches market depth data for a symbol.

        Parameters:
            symbol (str): Symbol of the product. Eg: 'NSE:SBIN-EQ'.
            ohlcv_flag (int): Flag to indicate whether to retrieve open, high, low, closing, and volume quantity. Set to 1 for yes.

        Returns:
            The response JSON as a dictionary.
        """
        if self.is_async:
                response = self.service.get_async_call(
                    Config.market_depth, self.header, data, data_flag=True
                )
            
        else:
            response = self.service.get_call(
                Config.market_depth, self.header, data, data_flag=True
            )
        return response


    def optionchain(self, data=None):
        """
        Fetches option chain data for a given symbol.

        Parameters:
            symbol (str): The symbol of the product. For example, 'NSE:NIFTY50-INDEX'.
            timestamp (int): Expiry timestamp of the stock. Use empty for current expiry. Example: 1813831200.
            strikecount (int): Number of strike price data points desired. 
                For instance, setting it to 7 provides: 1 INDEX + 7 ITM + 1 ATM + 7 OTM = 1 INDEX and 15 STRIKE (15 CE + 15 PE).

        Returns:
            dict: The response JSON containing the option chain data.
        """
        if self.is_async:
                response = self.service.get_async_call(
                    Config.option_chain, self.header, data, data_flag=True
                )
            
        else:
            response = self.service.get_call(
                Config.option_chain, self.header, data, data_flag=True
            )
        return response