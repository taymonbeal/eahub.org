from django.conf import settings
from django.test import selenium
import environ


env = environ.Env()

selenium.SeleniumTestCaseBase.external_host = settings.ALLOWED_HOSTS[0]


class SeleniumTestCase(selenium.SeleniumTestCase):
    browsers = env.list("WEBDRIVER_BROWSERS")
    selenium_hub = env.str("WEBDRIVER_URL")
