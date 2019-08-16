FROM python:3.7
RUN ["mkdir", "/backend", "/static"]
WORKDIR /backend
COPY requirements.txt ./
RUN ["pip", "install", "--requirement=requirements.txt"]
# eahub.org/assets isn't a real URL; there's no container registry on eahub.org.
# Rather, that container must be built locally before this one is, and given that name.
# This is ensured by docker-compose.yml (in local development) and azure-pipelines.yml.
COPY --from=eahub.org/assets /dist/ /dist/
COPY --from=eahub.org/assets /webpack-stats/ /webpack-stats/
COPY ./ ./
ENV PYTHONPATH /backend
RUN ["env", "DJANGO_SETTINGS_MODULE=eahub.config.build_settings", "django-admin", "collectstatic"]
ENV DJANGO_SETTINGS_MODULE eahub.config.settings
EXPOSE 8000
CMD ["gunicorn", "--bind=0.0.0.0:8000", "eahub.config.wsgi"]
