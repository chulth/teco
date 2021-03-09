from airflow.hooks.http_hook import HttpHook


class CoreHook(HttpHook):
    """Specific http hook to interact with ONSA-Core API.
        Uses specific http_con_id and content-type and
        Authorization headers"""

    def __init__(self, method, api_token, **kwargs):

        super().__init__(method=method,
                         http_conn_id='core_api',
                         auth_type=None)
        self.api_token = str(api_token) if api_token else ''

    def get_conn(self, headers):

        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.api_token
        }
        # self.log.info(f"Extrar Headers at [ get_conn() ]: {headers} ")

        session = super().get_conn(headers)
        session.auth = None
        session.verify = False
        return session