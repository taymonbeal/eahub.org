from django.test import selenium
from eahub import testing


class SmokeTest(testing.SeleniumTestCase):
    def test_homepage_title(self):
        self.selenium.get(self.live_server_url)
        self.assertEqual(self.selenium.title, "EA Hub Home")
