from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
from inventory_plugin.inventory_hook import InventoryHook
from airflow.exceptions import AirflowException, AirflowFailException
import json
import os


class CoreHttpOperator(SimpleHttpOperator):
    """ Operator that handles http request to ONSA-Core API"""
    @apply_defaults
    def __init__(self, *args, **kwargs) -> None:
        super(CoreHttpOperator, self).__init__(*args, **kwargs)
        self.core_token = None
        if 'health_check' in kwargs['params']:
            self.get_health_check_task_id = "get_health_check"

    def _set_token_from_authentication_task(self, task_instance):
        """Obtains token from 'get_inventory_token' task
           to be used on further requests"""
        self.core_token = task_instance.xcom_pull(
            task_ids="get_core_token")

    def _get_auth_data(self):
        """Returns auth credentials as dict."""
        return {
            'email': os.getenv('CORE_USER'),
            'password': os.getenv('CORE_PASSWORD')
        }
    def execute(self, context):
        from airflow.utils.operator_helpers import make_kwargs_callable

        if self.endpoint and 'authenticate' in self.endpoint:
            self.data = self._get_auth_data()
        else:
            self._set_token_from_authentication_task(
                context['task_instance'])

        http_hook = CoreHook(self.method, self.api_token)

        # serialize data as JSON is a mandatory rq from Inventory API.
        if self.data:
            self.data = json.dumps(self.data)
        self.log.info("Calling HTTP method")

        response = http_hook.run(
            self.endpoint, self.data, self.headers, self.extra_options)
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            kwargs_callable = make_kwargs_callable(self.response_check)
            if not kwargs_callable(response, **context):
                raise AirflowException("Response check returned False.")
        if self.response_filter:
            kwargs_callable = make_kwargs_callable(self.response_filter)
            return kwargs_callable(response, **context)
        return response.text