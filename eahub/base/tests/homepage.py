from eahub.base.tests.pageobjects.base_page import BasePage
from eahub.base.tests.locators import home, topnavbar, login


class Homepage(BasePage):

    def go_to_login(self):
        self.find_element(login.LOGIN_BUTTON).click()

    def go_to_profiles(self):
        self.find_element(home.BUTTON_PROFILES).click()

    def go_to_groups(self):
        self.find_element(home.BUTTON_GROUPS).click()

    def go_to_resources(self):
        self.find_element(home.BUTTON_RESOURCES).click()

    def switch_map_selector(self):
        self.find_element(home.MAP_SELECTOR_INDIVIDUALS).click()
        self.wait_for_element_to_be_visible(home.MAP)
