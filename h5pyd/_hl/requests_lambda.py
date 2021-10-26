import json

# rom .config import Config

"""
get aiobotocore lambda client
"""

LAMBDA_REQ_PREFIX = "http+lambda://"

class Session:
    def __init__(self, timeout=10):
        self.timeout = timeout

    def mount(self, protocol, adapter):
        # TBD
        print(f"requests_lambda mount({protocol})")

    def _invoke(self, req, method="GET", params=None, headers=None): 
        if not req:
            msg = "no req"
            raise ValueError(msg)
        if not req.startswith(LAMBDA_REQ_PREFIX):
            msg = f"Expected req to start with {LAMBDA_REQ_PREFIX}"
            raise ValueError(msg)
        if method not in ("GET", "PUT", "POST", "DELETE"):
            msg = f"Unexpected method: {method}"
            raise ValueError(msg)

        s = req[len(LAMBDA_REQ_PREFIX):] # strip off protocol
        index = s.find('/')
        if index <= 0:
            msg = "Unexpected request"
            raise ValueError(msg)
        function_name = s[:(index-1)]
        req_path = s[:index]
        if not req_path:
            msg = "no request path found"
            raise ValueError(msg)

        req_json = {"method": method,
                   "request": req_path,
                   "params": params,
                   "headers": headers
                   }

        payload = json.dumps(req_json).encode('utf-8')

        import boto3  # import here so it's not a global dependency

        with boto3.client('lambda') as lambda_client:
            lambda_rsp = lambda_client.invoke(
                     FunctionName=function_name,
                     InvocationType='RequestResponse',
                     Payload=payload)
        print("got lambda rsp:", lambda_rsp)
        return lambda_rsp

    def get(self, req, params=None, headers=None):
        """
        Lambda GET request

        req should be in form: "http+lambda://function/path"
        """
        rsp = self._invoke(req, params=params, headers=headers)
        return rsp

    def put(self, req, params=None, headers=None):
        """
        Lambda PUT request

        req should be in form: "http+lambda://function/path"
        """
        rsp = self._invoke(req, method="PUT", params=params, headers=headers)
        return rsp

    def post(self, req, params=None, headers=None):
        """
        Lambda POST request

        req should be in form: "http+lambda://function/path"
        """
        rsp = self._invoke(req, method="POST", params=params, headers=headers)
        return rsp

    def delete(self, req, params=None, headers=None):
        """
        Lambda DELETE request

        req should be in form: "http+lambda://function/path"
        """
        rsp = self._invoke(req, method="DELETE", params=params, headers=headers)
        return rsp

    def close(self):
        # TBD - release any held resources
        pass

        



