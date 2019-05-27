from django.test import TestCase
from eahub.base.tests.homepage import Homepage
from eahub.base.tests.pageobjects.login import Login
from eahub.base.tests.pageobjects.base_page import BasePage

class LoginTestCase(TestCase, BasePage):
    def test_login(self):
        Homepage.go_to_login(self)
        Login.do_login("login", "password")
