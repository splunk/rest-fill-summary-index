import ssl
import unittest
from unittest.mock import MagicMock, patch

import fill_summary_index_rest as module


class SplunkRestClientTlsTests(unittest.TestCase):
    def test_default_context_verifies_certificates_and_hostnames(self):
        client = module.SplunkRestClient("https://splunk.example:8089")

        self.assertEqual(client.ssl_context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(client.ssl_context.check_hostname)

    @patch("fill_summary_index_rest.ssl.create_default_context")
    def test_custom_ca_file_is_loaded_by_default_context(self, create_default_context):
        context = create_default_context.return_value

        client = module.SplunkRestClient(
            "https://splunk.example:8089", ca_file_path="/secure/company-ca.pem"
        )

        create_default_context.assert_called_once_with(cafile="/secure/company-ca.pem")
        self.assertIs(client.ssl_context, context)

    @patch("fill_summary_index_rest.urlopen")
    def test_requests_use_verified_ssl_context(self, urlopen):
        response = MagicMock()
        response.read.return_value = b"{}"
        urlopen.return_value.__enter__.return_value = response
        client = module.SplunkRestClient("https://splunk.example:8089")

        self.assertEqual(client._request("GET", "/services/server/info"), {})

        self.assertIs(urlopen.call_args.kwargs["context"], client.ssl_context)


if __name__ == "__main__":
    unittest.main()
