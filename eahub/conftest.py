import environ
import pytest


env = environ.Env()


@pytest.fixture(scope="session", params=env.list("BROWSERS"))
def session_capabilities(request, session_capabilities):
    import time, requests
    for _ in range(60):
        json = requests.get("http://webdriver:4444/grid/api/hub?configuration=nodes").json()
        browsers = {browser["browser"] for node in json["nodes"] for browser in node["browsers"]}
        if request.param in browsers:
            break
        time.sleep(0.5)
    else:
        raise IOError("could not connect to browser")
    session_capabilities["browserName"] = request.param
    return session_capabilities
