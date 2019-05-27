from selenium.webdriver.common.by import By


class Login(object):
    LOGIN_EMAIL = (By.CSS_SELECTOR, '#id_login')
    LOGIN_PASSWORD = (By.CSS_SELECTOR, '#id_password')
    LOGIN_BUTTON = (By.CSS_SELECTOR, '#body > div.panel.panel-default > div > form > button')