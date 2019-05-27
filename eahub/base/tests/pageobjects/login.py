from eahub.base.tests.pageobjects.base_page import BasePage
from eahub.base.tests.locators import login


class Login(BasePage):

    def do_login(self, login, password):
        self.find_element(login.LOGIN_EMAIL).send_keys(login)
        self.find_element(login.LOGIN_PASSWORD).send_keys(password)
        self.find_element(login.LOGIN_BUTTON).click()