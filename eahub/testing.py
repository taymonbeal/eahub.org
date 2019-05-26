from django.conf import settings
from django.test import selenium
import environ


env = environ.Env()

selenium.SeleniumTestCaseBase.external_host = settings.ALLOWED_HOSTS[0]


class SeleniumTestCase(selenium.SeleniumTestCase):
    #host = '0.0.0.0'
    port = env.int("TEST_SERVER_PORT")
    browsers = env.list("WEBDRIVER_BROWSERS")
    selenium_hub = env.str("WEBDRIVER_URL")

    @classmethod
    def setUpClass(cls):
        #import pdb; pdb.set_trace()
        super().setUpClass()
