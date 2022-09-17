import json

# rom .config import Config

"""
get aiobotocore lambda client
"""

LAMBDA_REQ_PREFIX = "http+lambda://"

STATUS_REASONS = {
    200: "OK",
    201: "Created",
    202: "Accepted",
    204: "No Content",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    408: "Request Timeout",
    409: "Confict",
    410: "Gone",
    413: "Payload Too Large",
    500: "Internal Server Error",
    501: "Not Implemented",
    503: "Service Unavailable",
    504: "Gateway Timeout",
    507: "Insufficient Storage",
}


class HttpChunkIterator(object):
    """
    Class to iterate through list of chunks of a http response
    """

    def __init__(self, data, chunk_size=1):
        self._data = data
        self._chunk_size = chunk_size
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._data is None:
            raise StopIteration()
        if self._index >= len(self._data):
            raise StopIteration()
        num_bytes = len(self._data)
        n = self._index
        m = n + self._chunk_size
        if m > num_bytes:
            m = num_bytes
        self._index = m
        return self._data[n:m]


class LambdaResponse:
    def __init__(self, lambda_rsp):
        self._status_code = 500
        self._reason = ""
        self._headers = {}
        self._text = None
        self._json = None
        self._content_length = 0
        self._iter_index = 0
        if lambda_rsp and isinstance(lambda_rsp, dict):
            if "StatusCode" in lambda_rsp:
                lambda_status_code = lambda_rsp["StatusCode"]

                if lambda_status_code in (200, 201) and "Payload" in lambda_rsp:
                    payload = lambda_rsp["Payload"]
                    rsp_text = payload.read().decode("utf-8")
                    rsp_payload = json.loads(rsp_text)
                    if rsp_payload.get("isBase64Encoded"):
                        is_base64_encoded = True
                    else:
                        is_base64_encoded = False

                    if "statusCode" in rsp_payload:
                        self._status_code = rsp_payload["statusCode"]
                    if "headers" in rsp_payload:
                        headers_text = rsp_payload["headers"]

                        headers = json.loads(headers_text)
                        for k in headers:
                            v = headers[k]
                            self._headers[k] = v
                    if self._status_code in (200, 201) and "body" in rsp_payload:
                        body_text = rsp_payload["body"]
                        # set the json prop for a dict,
                        # otherwise just set the text prop
                        if isinstance(body_text, dict):
                            self._json = body_text
                        elif is_base64_encoded and body_text:
                            # convert hex encoded to bytes
                            self._text = bytes.fromhex(body_text)
                        else:
                            self._text = body_text

                else:
                    raise ValueError("lambda: unable to get payload")
        else:
            raise TypeError("lambda: expected dict response")
        if self._status_code in STATUS_REASONS:
            self._reason = STATUS_REASONS[self._status_code]
        else:
            self._reason = "Unexpected status code"

    @property
    def status_code(self):
        return self._status_code

    @property
    def reason(self):
        return self._reason

    @property
    def text(self):
        if self._text:
            return self._text
        elif self._json:
            self._text = json.dumps(self._json)
            return self._text
        else:
            return None

    def json(self):
        if self._json:
            return self._json
        elif self._text:
            self._json = json.loads(self._text)
            return self._json
        else:
            return None

    def iter_content(self, chunk_size=1):
        return HttpChunkIterator(self._text, chunk_size=chunk_size)

    @property
    def headers(self):
        return self._headers

    @property
    def content_length(self):
        if self._text:
            return len(self._text)
        elif self._json:
            self._text = json.dumps(self._text)
            return len(self._text)
        else:
            return 0


class Session:
    def __init__(self, timeout=10):
        self.timeout = timeout

    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def mount(self, protocol, adapter):
        # TBD
        # print(f"requests_lambda mount({protocol})")
        pass

    def _invoke(self, req, method="GET", params=None, headers=None, data=None):
        if not req:
            msg = "no req"
            raise ValueError(msg)
        if not req.startswith(LAMBDA_REQ_PREFIX):
            msg = f"Expected req to start with {LAMBDA_REQ_PREFIX}"
            raise ValueError(msg)
        if method not in ("GET", "PUT", "POST", "DELETE"):
            msg = f"Unexpected method: {method}"
            raise ValueError(msg)
        if method in ("GET", "DELETE") and data:
            msg = f"data not expected for method: {method}"
            raise ValueError(msg)

        # Convert uri of the form: http+lambda://FUNC_NAME/REQ
        # as:
        # function_name = FUNC_NAME
        # req_path = REQ
        # params = {PARAMS}
        s = req[len(LAMBDA_REQ_PREFIX) :]  # strip off protocol
        index = s.find("/")
        if index <= 0:
            msg = "Unexpected request"
            raise ValueError(msg)
        function_name = s[:index]
        if function_name.find("/") >= 0:
            msg = f"unexpected lambda function name: {function_name}"
            raise ValueError(msg)
        index = s.find(function_name)
        req_path = s[index + len(function_name) :]
        if not req_path:
            msg = "no request path found"
            raise ValueError(msg)

        # convert header values to string from bytes if needed
        json_headers = {}
        for k in headers:
            v = headers[k]
            if isinstance(v, bytes):
                json_headers[k] = v.decode("utf-8")
            else:
                json_headers[k] = v

        req_json = {
            "method": method,
            "path": req_path,
            "params": params,
            "headers": json_headers,
            "body": data,
        }

        payload = json.dumps(req_json).encode("utf-8")

        import boto3  # import here so it's not a global dependency
        from botocore.exceptions import ClientError

        # with boto3.client('lambda')
        lambda_client = boto3.client("lambda")
        try:
            lambda_rsp = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=payload,
            )
        except ClientError as ce:
            if "Error" in ce.response and "Code" in ce.response["Error"]:
                error_code = ce.response["Error"]["Code"]
            else:
                error_code = "Unknown Lambda error"
            if error_code == "UnrecognizedClientException":
                # this happens when the AWS access key not provided
                error_code += " (are the AWS credentials valid?)"
            raise ValueError(error_code)
        rsp = LambdaResponse(lambda_rsp)
        return rsp

    def get(
        self, req, params=None, headers=None, stream=False, timeout=None, verify=None
    ):
        """
        Lambda GET request

        req should be in form: "http+lambda://function/path"
        """
        if stream:
            raise ValueError("stream not supported for Lambda")
        rsp = self._invoke(req, params=params, headers=headers)
        return rsp

    def put(self, req, params=None, headers=None, data=None, verify=None):
        """
        Lambda PUT request

        req should be in form: "http+lambda://function/path"
        """
        rsp = self._invoke(req, method="PUT", params=params, headers=headers, data=data)
        return rsp

    def post(self, req, params=None, headers=None, data=None, verify=None):
        """
        Lambda POST request

        req should be in form: "http+lambda://function/path"
        """
        rsp = self._invoke(
            req, method="POST", params=params, headers=headers, data=data
        )
        return rsp

    def delete(self, req, params=None, headers=None, verify=None):
        """
        Lambda DELETE request

        req should be in form: "http+lambda://function/path"
        """
        rsp = self._invoke(req, method="DELETE", params=params, headers=headers)
        return rsp

    def close(self):
        # TBD - release any held resources
        pass
