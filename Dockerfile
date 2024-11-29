FROM python:3.13-slim

# Install poetry
RUN pip install poetry

# Copy the poetry.lock and pyproject.toml files
COPY poetry.lock pyproject.toml ./

# Install dependencies
RUN poetry config virtualenvs.create false && poetry install --no-dev

# Copy the rest of the files
COPY *.py ./

# Run the main.py file
CMD ["python", "main.py"]
